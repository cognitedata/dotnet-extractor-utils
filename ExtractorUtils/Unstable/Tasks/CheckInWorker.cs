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
    /// Worker for submitting periodic checkins to the integrations API.
    /// </summary>
    public class CheckInWorker : IIntegrationSink
    {
        private readonly object _lock = new object();
        private readonly Dictionary<string, ErrorWithTask> _errors = new Dictionary<string, ErrorWithTask>();
        private List<TaskUpdate> _taskUpdates = new List<TaskUpdate>();
        private readonly Client _client;

        private readonly string _integrationId;
        private readonly ILogger _logger;

        private const int MAX_ERRORS_PER_CHECKIN = 1000;
        private const int MAX_TASK_UPDATES_PER_CHECKIN = 1000;

        private bool _isRunning;

        private int? _activeRevision;
        private readonly Action<int> _onRevisionChanged;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="integrationId">ID of the integration the worker should write to.</param>
        /// <param name="logger">Internal logger.</param>
        /// <param name="client">Cognite client</param>
        /// <param name="onRevisionChanged">Callback to call when the remote configuration revision is updated.</param>
        /// <param name="activeRevision">Currently active config revision. Used to know whether the extractor has received a new
        /// config revision since the last checkin. Null indiciates that the extractor is running local config,
        /// and should not restart based on changes to remote config.</param>
        public CheckInWorker(
            string integrationId,
            ILogger logger,
            Client client,
            Action<int> onRevisionChanged,
            int? activeRevision
        )
        {
            _client = client;
            _logger = logger;
            _integrationId = integrationId;
            _onRevisionChanged = onRevisionChanged;
            _activeRevision = activeRevision;
        }

        /// <summary>
        /// Start running the checkin worker.
        /// 
        /// This may only be called once.
        /// </summary>
        /// <param name="token">Cancellation token</param>
        /// <param name="interval">Interval, defaults to 30 seconds.</param>
        public async Task RunPeriodicCheckin(CancellationToken token, TimeSpan? interval = null)
        {
            lock (_lock)
            {
                if (_isRunning) throw new InvalidOperationException("Attempted to start a checkin worker that was already running");
                _isRunning = true;
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
        /// Report a checkin immediately, flushing the cache.
        /// 
        /// This should be called after terminating everything else, to report a final checkin.
        /// </summary>
        /// <param name="token"></param>
        /// <returns></returns>
        public async Task Flush(CancellationToken token)
        {
            // Reporting checkin is safely behind locks, so we can just call report.
            try
            {
                await ReportCheckIn(token).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error during checkin: {Message}", ex.Message);
            }
        }

        private void RequeueCheckIn(IEnumerable<ErrorWithTask> errors, IEnumerable<TaskUpdate> tasks)
        {
            lock (_lock)
            {
                foreach (var err in errors)
                {
                    if (!_errors.ContainsKey(err.ExternalId)) _errors.Add(err.ExternalId, err);
                }
                _taskUpdates.AddRange(tasks);
            }
        }

        private async Task TryWriteCheckIn(IEnumerable<ErrorWithTask> errors, IEnumerable<TaskUpdate> tasks, CancellationToken token)
        {
            try
            {
                var response = await _client.Alpha.Integrations.CheckInAsync(new CheckInRequest
                {
                    ExternalId = _integrationId,
                    TaskEvents = tasks,
                    Errors = errors,
                }, token).ConfigureAwait(false);
                HandleCheckInResponse(response);
            }
            catch (Exception ex)
            {
                if (ex is ResponseException rex && (rex.Code == 400 || rex.Code == 404))
                {
                    _logger.LogError(rex, "CheckIn failed with a 400 status code, this is a bug! Dropping current checkin batch and continuing.");
                    return;
                }
                // If pushing the update failed, keep the updates to try again later.
                RequeueCheckIn(errors, tasks);
                throw;
            }
        }

        private async Task ReportCheckIn(CancellationToken token)
        {
            List<ErrorWithTask> newErrors;
            List<TaskUpdate> taskUpdates;

            lock (_lock)
            {
                newErrors = _errors.Values.ToList();
                _errors.Clear();
                taskUpdates = _taskUpdates;
                _taskUpdates = new List<TaskUpdate>();
            }

            newErrors.Sort((a, b) => (a.EndTime ?? a.StartTime).CompareTo(b.EndTime ?? b.StartTime));
            taskUpdates.Sort((a, b) => a.Timestamp.CompareTo(b.Timestamp));

            while (!token.IsCancellationRequested)
            {
                if (newErrors.Count <= MAX_ERRORS_PER_CHECKIN && taskUpdates.Count <= MAX_TASK_UPDATES_PER_CHECKIN)
                {
                    var errorsToWrite = newErrors;
                    var tasksToWrite = taskUpdates;
                    newErrors = new List<ErrorWithTask>();
                    taskUpdates = new List<TaskUpdate>();
                    await TryWriteCheckIn(errorsToWrite, tasksToWrite, token).ConfigureAwait(false);
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

                if (errIdx > 0) newErrors = newErrors.Skip(errIdx).ToList();
                if (taskIdx > 0) taskUpdates = taskUpdates.Skip(taskIdx).ToList();

                await TryWriteCheckIn(errorsBatch, taskBatch, token).ConfigureAwait(false);
                if (newErrors.Count == 0 && taskUpdates.Count == 0) break;
            }

            // If the task was cancelled, re-queue any unsubmitted errors and updates.
            // This way, we don't lose any updates, and can push them when doing the final flush.
            if (token.IsCancellationRequested)
            {
                RequeueCheckIn(newErrors, taskUpdates);
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

        private void HandleCheckInResponse(CheckInResponse response)
        {
            if (_activeRevision != null
                && response.LastConfigRevision != _activeRevision
                && response.LastConfigRevision != null)
            {
                if (_onRevisionChanged != null)
                {
                    _logger.LogInformation("Remote config revision changed {From} -> {To}", _activeRevision, response.LastConfigRevision);
                    _onRevisionChanged(response.LastConfigRevision.Value);
                }
                else
                {
                    _logger.LogInformation(
                        "Remote config revision changed {From} -> {To}. The extractor is currently using local configuration and will not restart.",
                        _activeRevision, response.LastConfigRevision);

                }
                _activeRevision = response.LastConfigRevision.Value;
            }
        }
    }
}