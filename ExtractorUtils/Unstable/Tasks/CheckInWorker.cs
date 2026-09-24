using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extractor.Common;
using CogniteSdk;
using CogniteSdk.Alpha;
using Microsoft.Extensions.Logging;

namespace Cognite.Extractor.Utils.Unstable.Tasks
{
    /// <summary>
    /// Worker for submitting periodic check-ins to the integrations API.
    /// </summary>
    public class CheckInWorker : IIntegrationSink
    {
        private readonly object _lock = new object();
        private readonly Dictionary<string, ErrorWithTask> _errors = new Dictionary<string, ErrorWithTask>();
        private List<TaskUpdate> _taskUpdates = new List<TaskUpdate>();
        private List<ActionUpdate> _actionUpdates = new List<ActionUpdate>();
        private Func<IReadOnlyList<IntegrationAction>, Task>? _actionDispatcher;
        private readonly Client _client;

        private readonly string _integrationId;
        private readonly ILogger _logger;

        private const int MAX_ERRORS_PER_CHECKIN = 1000;
        private const int MAX_TASK_UPDATES_PER_CHECKIN = 1000;
        private const int MAX_ACTION_UPDATES_PER_CHECKIN = 100;

        private bool _isRunning;

        private int? _activeRevision;
        private readonly Action<int> _onRevisionChanged;

        private SemaphoreSlim _flushLock = new SemaphoreSlim(1);
        private bool _retryStartup;
        private bool _hasReportedStartup;
        private Random _random = new Random();

        const int STARTUP_BACKOFF_SECONDS = 30;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="integrationId">ID of the integration the worker should write to.</param>
        /// <param name="logger">Internal logger.</param>
        /// <param name="client">Cognite client</param>
        /// <param name="onRevisionChanged">Callback to call when the remote configuration revision is updated.</param>
        /// <param name="activeRevision">Currently active config revision. Used to know whether the extractor has received a new
        /// config revision since the last check-in. Null indiciates that the extractor is running local config,
        /// and should not restart based on changes to remote config.</param>
        /// <param name="retryStartup">Whether to retry the startup request if it fails,
        /// beyond normal retries. If this is `true`, the check-in worker will retry startup requests indefinitely,
        /// instead of raising an exception.</param>
        public CheckInWorker(
            string integrationId,
            ILogger logger,
            Client client,
            Action<int> onRevisionChanged,
            int? activeRevision,
            bool retryStartup = false
        )
        {
            _client = client;
            _logger = logger;
            _integrationId = integrationId;
            _onRevisionChanged = onRevisionChanged;
            _activeRevision = activeRevision;
            _retryStartup = retryStartup;
        }

        /// <summary>
        /// Start running the check-in worker.
        /// 
        /// This may only be called once.
        /// </summary>
        /// <param name="token">Cancellation token</param>
        /// <param name="startupPayload">Payload to send to the startup endpoint before beginning to
        /// report periodic check-ins..</param>
        /// <param name="interval">Interval, defaults to 30 seconds.</param>
        public async Task RunPeriodicCheckIn(CancellationToken token, StartupRequest startupPayload, TimeSpan? interval = null)
        {
            if (startupPayload is null) throw new ArgumentNullException(nameof(startupPayload));

            lock (_lock)
            {
                if (_isRunning) throw new InvalidOperationException("Attempted to start a check-in worker that was already running");
                _isRunning = true;
            }

            // Make sure the external ID in the startup payload matches the external ID of the target integration.
            startupPayload.ExternalId = _integrationId;

            // Hold the flush lock while reporting startup, to ensure that we don't start reporting check-ins
            // before the startup request has been sent.
            // With this, calls to flush will wait until the startup request has been sent,
            // or startup fails.
            // This keeps us from reporting events before the startup, in case the extractor is started offline.
            // In this case, we would like to potentially report a startup and anything that has happened
            // while the connection to CDF was down.
            try
            {
                await _flushLock.WaitAsync(token).ConfigureAwait(false);
            }
            catch
            {
                // Should only happen if we are cancelled while waiting.
                if (token.IsCancellationRequested) return;
                throw;
            }

            try
            {
                while (!token.IsCancellationRequested)
                {
                    try
                    {
                        await ReportStartup(startupPayload, token).ConfigureAwait(false);
                        _hasReportedStartup = true;
                        break;
                    }
                    catch (Exception ex)
                    {
                        if (!_retryStartup) throw;
                        // Retry about every 30 seconds, but with some jitter so we don't
                        // end up with too bursty retries.
                        var toDelay = _random.Next(STARTUP_BACKOFF_SECONDS / 2, STARTUP_BACKOFF_SECONDS * 3 / 2);
                        _logger.LogError("Failed to report startup, retrying after {Time} seconds: {Err}", toDelay, ex.Message);
                        await Task.Delay(TimeSpan.FromSeconds(toDelay), token).ConfigureAwait(false);
                        continue;
                    }
                }
            }
            finally
            {
                _flushLock.Release();
            }

            var rinterval = interval ?? TimeSpan.FromSeconds(30);
            while (!token.IsCancellationRequested)
            {
                var waitTask = Task.Delay(rinterval, token);
                try
                {
                    await Flush(token).ConfigureAwait(false);
                    await waitTask.ConfigureAwait(false);
                }
                catch (TaskCanceledException)
                {
                    break;
                }
            }

            _isRunning = false;
        }

        /// <summary>
        /// Report a check-in immediately, flushing the cache.
        /// 
        /// This should be called after terminating everything else, to report a final check-in.
        /// </summary>
        /// <param name="token"></param>
        /// <returns></returns>
        public async Task Flush(CancellationToken token)
        {
            // Ensure that only one flush is running at a time,
            // importantly this also means that if we call flush, we will wait for
            // any running flushes to complete, meaning that once this method returns,
            // we are guaranteed to have flushed all updates that were not yet sent
            // when the method was called.
            try
            {
                await _flushLock.WaitAsync(token).ConfigureAwait(false);
            }
            catch
            {
                // Only really happens if we're cancelled while waiting.
                return;
            }

            // Reporting check-in is safely behind locks, so we can just call report.
            try
            {
                await ReportCheckIn(token).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error during check-in: {Message}", ex.Message);
            }
            finally
            {
                _flushLock.Release();
            }
        }

        private void RequeueCheckIn(IEnumerable<ErrorWithTask> errors, IEnumerable<TaskUpdate> tasks, IEnumerable<ActionUpdate> actionUpdates)
        {
            lock (_lock)
            {
                foreach (var err in errors)
                {
                    if (!_errors.ContainsKey(err.ExternalId)) _errors.Add(err.ExternalId, err);
                }
                _taskUpdates.AddRange(tasks);
                // Unlike errors/task updates (which are re-sorted by timestamp on every send
                // regardless of list order, making append-order irrelevant), ActionUpdate has no
                // timestamp and is always sent in raw list order. Requeued action updates must
                // therefore go back to the *front* of the queue, not the end -- otherwise a
                // stale update that failed to send (e.g. a `running` progress update) could end
                // up ordered after a newer update for the same action queued in the meantime
                // (e.g. its own terminal `succeeded`), reversing their effective order on the
                // next send. Matches python-extractor-utils' checkin_worker.py, which does the
                // same prepend for the same reason.
                _actionUpdates.InsertRange(0, actionUpdates);
            }
        }

        private async Task TryWriteCheckIn(IEnumerable<ErrorWithTask> errors, IEnumerable<TaskUpdate> tasks, IEnumerable<ActionUpdate> actionUpdates, CancellationToken token)
        {
            try
            {
                var response = await _client.Alpha.Integrations.CheckInAsync(new CheckInRequest
                {
                    ExternalId = _integrationId,
                    TaskEvents = tasks,
                    Errors = errors,
                    ActionUpdates = actionUpdates,
                }, token).ConfigureAwait(false);
                await HandleCheckInResponse(response).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                if (ex is ResponseException rex && (rex.Code == 400 || rex.Code == 404))
                {
                    // A 404 here typically means one of the action externalIds we reported an
                    // update for is unknown to the server (e.g. a very stale queue entry from
                    // before a restart) -- odin does not support partial failure within a
                    // check-in batch, so the whole batch is dropped rather than requeued, to
                    // avoid retrying an unresolvable bad ID forever and blocking every other
                    // queued item behind it.
                    _logger.LogError(rex, "CheckIn failed with a 400 status code, this is a bug! Dropping current check-in batch and continuing.");
                    return;
                }
                // If pushing the update failed, keep the updates to try again later.
                RequeueCheckIn(errors, tasks, actionUpdates);
                throw;
            }
        }

        private string? Truncate(string? value, string fieldName, string externalId)
        {
            if (value != null && value.Length > 5000)
            {
                _logger.LogWarning("Truncating {FieldName} for error {ExternalId} to 5,000 characters. Original length was {OriginalLength}", fieldName, externalId, value.Length);
                return value.Substring(0, 5000);
            }
            return value;
        }

        private async Task ReportCheckIn(CancellationToken token)
        {
            List<ErrorWithTask> newErrors;
            List<TaskUpdate> taskUpdates;
            List<ActionUpdate> actionUpdates;

            lock (_lock)
            {
                if (!_hasReportedStartup)
                {
                    newErrors = _errors.Values.Where(e => e.Task == null).ToList();
                    // No point checking in pre-startup if there are no errors.
                    if (newErrors.Count == 0)
                    {
                        _logger.LogInformation("Check-in worker has not reported startup yet, skipping check-in.");
                        return;
                    }
                    _logger.LogWarning("Check-in worker has not reported startup yet, only reporting errors not associated with a task.");
                    foreach (var err in newErrors)
                    {
                        _errors.Remove(err.ExternalId);
                    }
                    taskUpdates = new List<TaskUpdate>();
                    // Actions can't have been triggered before startup has succeeded (the
                    // extractor hasn't advertised any AvailableActions yet), but leave
                    // _actionUpdates untouched regardless, same as task updates above, so nothing
                    // queued so far is lost.
                    actionUpdates = new List<ActionUpdate>();
                }
                else
                {
                    newErrors = new List<ErrorWithTask>(_errors.Values);
                    foreach (var err in _errors.Values)
                    {
                        // Modifying err since it's an object reference in the dictionary, so changes will be reflected when we write the check-in,
                        // but we want to log the full error before truncating.
                        _logger.LogInformation("Error: {ExternalId}, Level: {Level}, Description: {Description}, Details: {Details}, Task: {Task}, StartTime: {StartTime}, EndTime: {EndTime}",
                            err.ExternalId, err.Level, err.Description, err.Details, err.Task, err.StartTime, err.EndTime);
                        err.Description = Truncate(err.Description, "Description", err.ExternalId);
                        err.Details = Truncate(err.Details, "Details", err.ExternalId);
                    }
                    _errors.Clear();
                    taskUpdates = _taskUpdates;
                    _taskUpdates = new List<TaskUpdate>();
                    actionUpdates = _actionUpdates;
                    _actionUpdates = new List<ActionUpdate>();
                }
            }

            newErrors.Sort((a, b) =>
            {
                long? aTime = a.EndTime ?? a.StartTime;
                long? bTime = b.EndTime ?? b.StartTime;
                // Handle null timestamps by treating them as 0 (earliest possible time)
                return (aTime ?? 0).CompareTo(bTime ?? 0);
            });
            taskUpdates.Sort((a, b) => a.Timestamp.CompareTo(b.Timestamp));
            // ActionUpdate has no timestamp field to sort by (unlike errors/task updates), so
            // action updates are always sent in the order they were queued (FIFO), independent of
            // the error/task-update time-based merge below.

            while (!token.IsCancellationRequested)
            {
                if (newErrors.Count <= MAX_ERRORS_PER_CHECKIN
                    && taskUpdates.Count <= MAX_TASK_UPDATES_PER_CHECKIN
                    && actionUpdates.Count <= MAX_ACTION_UPDATES_PER_CHECKIN)
                {
                    var errorsToWrite = newErrors;
                    var tasksToWrite = taskUpdates;
                    var actionsToWrite = actionUpdates;
                    newErrors = new List<ErrorWithTask>();
                    taskUpdates = new List<TaskUpdate>();
                    actionUpdates = new List<ActionUpdate>();
                    await TryWriteCheckIn(errorsToWrite, tasksToWrite, actionsToWrite, token).ConfigureAwait(false);
                    break;
                }

                var errIdx = 0;
                var taskIdx = 0;

                // In the (unlikely) case that we have more than 1000 updates, we need to send them in order of the time where they occured,
                // roughly.
                while ((errIdx < newErrors.Count || taskIdx < taskUpdates.Count) && errIdx < MAX_ERRORS_PER_CHECKIN && taskIdx < MAX_TASK_UPDATES_PER_CHECKIN)
                {
                    var taskTime = taskUpdates.ElementAtOrDefault(taskIdx)?.Timestamp ?? long.MaxValue;
                    var err = newErrors.ElementAtOrDefault(errIdx);
                    var errTime = err?.EndTime ?? err?.StartTime ?? long.MaxValue;

                    if (taskTime <= errTime)
                    {
                        taskIdx++;
                    }
                    if (errTime <= taskTime)
                    {
                        errIdx++;
                    }
                }

                var errorsBatch = newErrors.Take(errIdx).ToList();
                var taskBatch = taskUpdates.Take(taskIdx).ToList();
                // Action updates are batched independently of the time-based merge above, in
                // simple FIFO chunks of at most MAX_ACTION_UPDATES_PER_CHECKIN -- this runs on
                // every iteration of the outer loop, so a large action-update backlog with few or
                // no errors/tasks queued alongside it still drains correctly (the outer loop only
                // exits once errors, task updates, *and* action updates are all empty).
                var actionIdx = Math.Min(actionUpdates.Count, MAX_ACTION_UPDATES_PER_CHECKIN);
                var actionBatch = actionUpdates.Take(actionIdx).ToList();

                if (errIdx > 0) newErrors = newErrors.Skip(errIdx).ToList();
                if (taskIdx > 0) taskUpdates = taskUpdates.Skip(taskIdx).ToList();
                if (actionIdx > 0) actionUpdates = actionUpdates.Skip(actionIdx).ToList();

                await TryWriteCheckIn(errorsBatch, taskBatch, actionBatch, token).ConfigureAwait(false);
                if (newErrors.Count == 0 && taskUpdates.Count == 0 && actionUpdates.Count == 0) break;
            }

            // If the task was cancelled, re-queue any unsubmitted errors and updates.
            // This way, we don't lose any updates, and can push them when doing the final flush.
            if (token.IsCancellationRequested)
            {
                RequeueCheckIn(newErrors, taskUpdates, actionUpdates);
            }
        }

        private async Task ReportStartup(StartupRequest request, CancellationToken token)
        {
            var response = await _client.Alpha.Integrations.StartupAsync(request, token).ConfigureAwait(false);
            await HandleCheckInResponse(response).ConfigureAwait(false);
        }

        /// <summary>
        /// Queue an update to the status of a triggered action, to be sent on a future check-in.
        /// </summary>
        /// <param name="update">Update to queue. Must have <see cref="ActionUpdate.ExternalId"/> set.</param>
        public void QueueActionUpdate(ActionUpdate update)
        {
            if (update == null) throw new ArgumentNullException(nameof(update));
            if (string.IsNullOrEmpty(update.ExternalId)) throw new ArgumentException("ActionUpdate must have ExternalId set", nameof(update));
            lock (_lock)
            {
                _actionUpdates.Add(update);
            }
        }

        /// <summary>
        /// Register the callback to invoke whenever a check-in or startup response contains one
        /// or more actions pending execution by the extractor.
        /// </summary>
        /// <param name="dispatcher">Callback invoked with the current list of pending actions.</param>
        public void SetActionDispatcher(Func<IReadOnlyList<IntegrationAction>, Task> dispatcher)
        {
            lock (_lock)
            {
                _actionDispatcher = dispatcher;
            }
        }

        /// <inheritdoc />
        public void ReportError(ExtractorError error)
        {
            if (error == null) throw new ArgumentNullException(nameof(error));

            lock (_lock)
            {
                _errors[error.ExternalId] = error.ToSdk();
            }
        }

        /// <inheritdoc />
        public void ReportTaskEnd(string taskName, TaskUpdatePayload? update = null, DateTime? timestamp = null)
        {
            if (string.IsNullOrEmpty(taskName)) throw new ArgumentNullException(nameof(taskName));
            lock (_lock)
            {
                _taskUpdates.Add(new TaskUpdate
                {
                    Type = TaskUpdateType.ended,
                    Name = taskName,
                    Timestamp = (timestamp ?? DateTime.UtcNow).ToUnixTimeMilliseconds(),
                    Message = update?.Message,
                });
            }
        }

        /// <inheritdoc />
        public void ReportTaskStart(string taskName, TaskUpdatePayload? update = null, DateTime? timestamp = null)
        {
            if (string.IsNullOrEmpty(taskName)) throw new ArgumentNullException(nameof(taskName));
            lock (_lock)
            {
                _taskUpdates.Add(new TaskUpdate
                {
                    Type = TaskUpdateType.started,
                    Name = taskName,
                    Timestamp = (timestamp ?? DateTime.UtcNow).ToUnixTimeMilliseconds(),
                    Message = update?.Message,
                });
            }
        }

        private async Task HandleCheckInResponse(CheckInResponse response)
        {
            if (response.LastConfigRevision != _activeRevision && response.LastConfigRevision != null)
            {
                if (_activeRevision != null && _onRevisionChanged != null)
                {
                    _logger.LogInformation("Remote config revision changed {From} -> {To}", _activeRevision, response.LastConfigRevision);
                    _onRevisionChanged(response.LastConfigRevision.Value);
                }
                else if (_activeRevision != null)
                {
                    _logger.LogInformation(
                        "Remote config revision changed {From} -> {To}. The extractor is currently using local configuration and will need to be manually restarted and configured to use remote config for the new config to take effect.",
                        _activeRevision, response.LastConfigRevision);
                }
                _activeRevision = response.LastConfigRevision.Value;
            }

            await DispatchPendingActions(response.PendingActions).ConfigureAwait(false);
        }

        private async Task DispatchPendingActions(IEnumerable<IntegrationAction>? pendingActions)
        {
            var actions = pendingActions?.ToList();
            if (actions == null || actions.Count == 0) return;

            Func<IReadOnlyList<IntegrationAction>, Task>? dispatcher;
            lock (_lock)
            {
                dispatcher = _actionDispatcher;
            }

            if (dispatcher == null)
            {
                _logger.LogWarning("Received {Count} pending action(s), but no action dispatcher is registered. Ignoring.", actions.Count);
                return;
            }

            try
            {
                await dispatcher(actions).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                // The dispatcher is expected to hand off actual execution rather than run
                // actions to completion itself (see SetActionDispatcher), so an exception here
                // means the dispatcher's own routing/hand-off logic failed, not that a single
                // action failed -- that must never be silently lost, but it also must never take
                // down the check-in loop, which is why this catches broadly and only logs.
                _logger.LogError(ex, "Action dispatcher threw an unhandled exception: {Message}", ex.Message);
            }
        }
    }
}