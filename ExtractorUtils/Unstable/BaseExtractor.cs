using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extensions;
using Cognite.Extractor.Common;
using Cognite.Extractor.Utils.Unstable.Configuration;
using Cognite.Extractor.Utils.Unstable.Tasks;
using CogniteSdk;
using CogniteSdk.Alpha;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Cognite.Extractor.Utils.Unstable
{
    /// <summary>
    /// Base class for extractors.
    /// </summary>
    /// <typeparam name="TConfig">Config type.</typeparam>
    public abstract class BaseExtractor<TConfig> : BaseErrorReporter, IAsyncDisposable
    {
        /// <summary>
        /// Configuration object
        /// </summary>
        protected TConfig Config { get; }
        /// <summary>
        /// CDF destination
        /// </summary>
        protected CogniteDestination? Destination { get; }
        private readonly IIntegrationSink _sink;

        /// <summary>
        /// Task scheduler containing all public extractor tasks.
        /// </summary>
        protected ExtractorTaskScheduler TaskScheduler { get; }

        /// <summary>
        /// Scheduler for internal extractor tasks.
        ///
        /// Use this for tasks that are not exposed to integrations,
        /// like state store, metrics, or other background processes.
        ///
        /// Note that this is null until initialized in `Init`.
        /// </summary>
        protected PeriodicScheduler Scheduler { get; private set; } = null!;

        /// <summary>
        /// Access to the service provider this extractor was built from
        /// </summary>
        protected IServiceProvider Provider { get; private set; }

        /// <summary>
        /// Cancellation token source.
        ///
        /// Note that this is null until initialized in `Init`.
        /// </summary>
        protected CancellationTokenSource Source { get; private set; } = null!;

        private readonly ILogger<BaseExtractor<TConfig>> _logger;

        private readonly Dictionary<string, CustomAction<TConfig>> _customActions = new Dictionary<string, CustomAction<TConfig>>(StringComparer.OrdinalIgnoreCase);

        // The same startup snapshot is used for advertisement, collision checks and dispatch.
        // Routing must not invoke task metadata getters or take the scheduler lock at check-in time.
        private IReadOnlyList<IntegrationTask> _registeredTasks = Array.Empty<IntegrationTask>();
        private readonly Dictionary<string, (string TaskName, ActionType Type)> _taskActions
            = new Dictionary<string, (string, ActionType)>(StringComparer.Ordinal);
        private readonly HashSet<string> _taskNames = new HashSet<string>(StringComparer.Ordinal);
        private readonly object _actionLock = new object();
        private bool _stopping;
        private Task? _shutdownTask;

        private sealed class InFlightAction : IDisposable
        {
            private readonly object _lock = new object();
            private readonly CancellationTokenSource _source = new CancellationTokenSource();
            private Task? _cancellation;
            private bool _completed;
            private bool _cancellationRequested;

            public CancellationToken Token => _source.Token;
            public bool IsCancellationRequested => Volatile.Read(ref _cancellationRequested);

            public Task CancelAsync(Action<CancellationTokenSource> cancel)
            {
                lock (_lock)
                {
                    if (_completed) return _cancellation ?? Task.CompletedTask;
                    // Claim cancellation once, but never execute user callbacks under our lock
                    // or on the check-in thread. Reentrant cancellation reuses the same request.
                    // Record intent now so a target returning before the worker runs still
                    // reports canceled rather than succeeded.
                    Volatile.Write(ref _cancellationRequested, true);
                    return _cancellation ??= Task.Run(() => cancel(_source));
                }
            }

            public async Task CompleteAsync()
            {
                Task cancellation;
                lock (_lock)
                {
                    _completed = true;
                    cancellation = _cancellation ?? Task.CompletedTask;
                }
                try
                {
                    await cancellation.ConfigureAwait(false);
                }
                finally
                {
                    Dispose();
                }
            }

            public void Dispose()
            {
                _source.Dispose();
            }
        }

        /// <summary>
        /// Custom actions currently being dispatched, keyed by action externalId, each mapped to
        /// its token owner, which serializes cancellation and disposal without locking callbacks.
        ///
        /// - Used for dedup: skip re-dispatching an action redelivered before its first dispatch
        ///   completes (see DispatchAction).
        /// - Used for cancel-in-flight: cancel the specific in-flight run on a `cancel_pending`
        ///   redelivery (see RunCustomAction).
        /// - Start/Stop currently use task-name state in <see cref="TaskScheduler"/>.
        ///   Execution-specific Start cancellation is tracked in EDG-965.
        /// - Entries are removed on completion, not acknowledgement. Completed-action
        ///   deduplication is tracked separately in EDG-966/EDG-967.
        /// </summary>
        private readonly ConcurrentDictionary<string, InFlightAction> _inFlightCustomActions
            = new ConcurrentDictionary<string, InFlightAction>();

        private readonly string _integrationExternalId;

        private object _lock = new object();

        private ManualResetEvent _triggerEvent = new ManualResetEvent(false);

        /// <summary>
        /// Extractor start time. Set after `Init` has completed.
        /// </summary>
        protected DateTime? StartTime { get; private set; }

        /// <summary>
        /// Currently active config revision.
        /// </summary>
        protected int? ConfigRevision { get; }


        /// <summary>
        /// Constructor, usable with dependency injection.
        /// </summary>
        /// <param name="config">Configuration object</param>
        /// <param name="provider">Service provider used to build this</param>
        /// <param name="taskScheduler">Task scheduler.</param>
        /// <param name="sink">Sink for extractor task updates and errors.</param>
        /// <param name="destination">Cognite destination.</param>
        public BaseExtractor(
            ConfigWrapper<TConfig> config,
            IServiceProvider provider,
            ExtractorTaskScheduler taskScheduler,
            IIntegrationSink sink,
            CogniteDestination? destination = null
        )
        {
            if (config == null) throw new ArgumentNullException(nameof(config));
            Config = config.Config;
            ConfigRevision = config.Revision;
            Destination = destination;
            Provider = provider;
            _sink = sink;
            TaskScheduler = taskScheduler;
            _logger = provider.GetService<ILogger<BaseExtractor<TConfig>>>() ?? new NullLogger<BaseExtractor<TConfig>>();
            // Best-effort: ConnectionConfig is only registered when the extractor is built via
            // the full Runtime/Builder DI setup with a real integration configured (e.g. not in
            // unit tests that construct a BaseExtractor directly, and not for an extractor
            // running fully offline). Falls back to empty rather than throwing, since a missing
            // integration external id here is never actually observable in practice: without a
            // real integration, nothing can ever invoke DispatchActions in the first place (it's
            // only reachable via a checkin/startup response, which requires one).
            _integrationExternalId = provider.GetService<ConnectionConfig>()?.Integration?.ExternalId ?? string.Empty;
        }

        /// <summary>
        /// Initialize the extractor, adding tasks to the
        /// task runner as needed.
        ///
        /// This runs _before_ the extractor reports startup, if you
        /// have complex or heavy startup tasks, they should run
        /// in one or more tasks in the task scheduler, set to run
        /// immediately on startup.
        ///
        /// The task runner is not started yet when this method is called.
        /// </summary>
        /// <returns></returns>
        protected abstract Task InitTasks();

        /// <summary>
        /// Register any custom actions this extractor supports, by calling
        /// <see cref="RegisterAction"/>.
        ///
        /// This runs after <see cref="InitTasks"/> (see <see cref="Init"/>), so that the
        /// auto-generated Start/Stop actions for tasks registered there are already known when
        /// custom actions are registered, and can be checked for name collisions.
        ///
        /// Does nothing by default.
        /// </summary>
        /// <returns></returns>
        protected virtual Task InitActions()
        {
            return Task.CompletedTask;
        }

        /// <summary>
        /// Register a custom action, to be advertised to integrations at startup and dispatched
        /// to when triggered.
        ///
        /// Must be called from within <see cref="InitActions"/> (or before it, but after
        /// <see cref="InitTasks"/>) -- the framework does not verify this, but registering a
        /// duplicate name later triggers a validation error on the next attempt.
        /// </summary>
        /// <param name="action">Action to register.</param>
        /// <exception cref="InvalidOperationException">If an action or task with a colliding
        /// name is already registered.</exception>
        protected void RegisterAction(CustomAction<TConfig> action)
        {
            if (action == null) throw new ArgumentNullException(nameof(action));
            if (_customActions.ContainsKey(action.Name))
            {
                throw new InvalidOperationException($"An action named '{action.Name}' is already registered");
            }
            // Guard against colliding with an auto-generated Start/Stop action name for an
            // already-registered actionable task. InitTasks() always runs before InitActions()
            // (see Init()), so every actionable task the extractor will ever advertise at this
            // startup is already known here -- this does not protect against a task added
            // dynamically at runtime after startup, which is out of scope for action name
            // collision checking (the two aren't recomputed together after startup anyway).
            foreach (var task in _registeredTasks)
            {
                if (!task.Action) continue;
                if (string.Equals(action.Name, ActionNaming.StartActionName(task.Name), StringComparison.OrdinalIgnoreCase)
                    || string.Equals(action.Name, ActionNaming.StopActionName(task.Name), StringComparison.OrdinalIgnoreCase))
                {
                    throw new InvalidOperationException(
                        $"Action name '{action.Name}' collides with the auto-generated Start/Stop action for task '{task.Name}'");
                }
            }
            _customActions.Add(action.Name, action);
        }

        /// <summary>
        /// Return the version of the active extractor.
        /// </summary>
        /// <returns></returns>
        protected abstract ExtractorId GetExtractorVersion();

        private void InitBase(CancellationToken token)
        {
            if (Source != null) throw new InvalidOperationException("Extractor already started");
            Source = CancellationTokenSource.CreateLinkedTokenSource(token);
            Scheduler = new PeriodicScheduler(Source.Token);
        }

        /// <summary>
        /// Add a task that should be watched by the extractor.
        ///
        /// Use this for tasks that will not be reported to integrations,
        /// but that you still want to monitor, so that the extractor can crash
        /// if they fail or exit unexpectedly.
        /// </summary>
        /// <param name="task">Task to monitor.</param>
        /// <param name="name">Task name, just used for logging.</param>
        protected void AddMonitoredTask(Func<CancellationToken, Task<SchedulerTaskResult>> task, string name)
        {
            if (task == null) throw new ArgumentNullException(nameof(task));
            if (name == null) throw new ArgumentNullException(nameof(name));
            if (Scheduler == null) throw new InvalidOperationException("Attempt to add monitored task without starting the extractor first.");
            Scheduler.ScheduleTask(name, task);
        }

        /// <summary>
        /// Cancel a monitored task, then wait for it to complete.
        ///
        /// This is typically used for ordered shutdown.
        /// </summary>
        /// <param name="name">Name of task to cancel.
        /// </param>
        /// <returns></returns>
        protected async Task CancelMonitoredTaskAndWait(string name)
        {
            await Scheduler.CancelAndWaitForTermination(name).ConfigureAwait(false);
        }

        /// <summary>
        /// Add a monitored task that should be watched by the extractor.
        ///
        /// Use this for tasks that will not be reported to integrations,
        /// but that you still want to monitor, so that the extractor can crash
        /// if they fail or exit unexpectedly.
        ///
        /// This variant takes a static SchedulerTaskResult, to indicate whether the
        /// task is expected to terminate on its own or not.
        /// </summary>
        /// <param name="task">Task to monitor.</param>
        /// <param name="staticResult">Whether the task exiting on its own without cancellation
        /// should be considered an error.</param>
        /// <param name="name">Task name, just used for logging.</param>
        /// <exception cref="ArgumentNullException"></exception>
        protected void AddMonitoredTask(Func<CancellationToken, Task> task, SchedulerTaskResult staticResult, string name)
        {
            if (task == null) throw new ArgumentNullException(nameof(task));
            if (name == null) throw new ArgumentNullException(nameof(name));
            if (Source == null) throw new InvalidOperationException("Attempt to add monitored task without starting the extractor first.");
            Scheduler.ScheduleTask(name, task, staticResult);
        }


        private bool _initialized;
        /// <summary>
        /// Initialize the extractor, if it has not already been initialized.
        ///
        /// This is called automatically if you call Start, so only use this if you need to separate
        /// the init stage from the run stage, for example for testing.
        /// </summary>
        /// <param name="token">Cancellation token to use for the run</param>
        /// <returns></returns>
        public async Task Init(CancellationToken token)
        {
            lock (_lock)
            {
                if (_initialized) return;
                _initialized = true;
            }
            InitBase(token);
            await TestConfig().ConfigureAwait(false);
            await InitTasks().ConfigureAwait(false);
            _registeredTasks = TaskScheduler.GetRegisteredTasks().ToList();
            foreach (var task in _registeredTasks)
            {
                _taskNames.Add(task.Name);
                if (!task.Action) continue;
                _taskActions.Add(ActionNaming.StartActionName(task.Name), (task.Name, ActionType.start_task));
                _taskActions.Add(ActionNaming.StopActionName(task.Name), (task.Name, ActionType.stop_task));
            }
            await InitActions().ConfigureAwait(false);
        }

        /// <summary>
        /// Build the AvailableActions list to advertise at startup: an auto-generated Start/Stop
        /// pair for every actionable task (in task-registration order), followed by custom
        /// actions in registration order.
        /// </summary>
        private IEnumerable<AvailableActionWrite> GetAvailableActions()
        {
            foreach (var task in _registeredTasks)
            {
                if (!task.Action) continue;
                yield return new AvailableActionWrite
                {
                    Name = ActionNaming.StartActionName(task.Name),
                    Type = ActionType.start_task,
                    Description = $"Start the '{task.Name}' task",
                    Task = task.Name,
                };
                yield return new AvailableActionWrite
                {
                    Name = ActionNaming.StopActionName(task.Name),
                    Type = ActionType.stop_task,
                    Description = $"Stop the '{task.Name}' task",
                    Task = task.Name,
                };
            }
            foreach (var action in _customActions.Values)
            {
                yield return new AvailableActionWrite
                {
                    Name = action.Name,
                    Type = ActionType.custom,
                    Description = action.Description,
                };
            }
        }

        private StartupRequest GetStartupRequest()
        {
            var version = GetExtractorVersion();
            version.Version = version.Version?.Truncate(32);
            return new StartupRequest()
            {
                ActiveConfigRevision = ConfigRevision.HasValue
                    ? StringOrInt.Create(ConfigRevision.Value)
                    : StringOrInt.Create("local"),
                Tasks = _registeredTasks,
                Extractor = version,
                // StartTime is not null here, as this is called after Init.
                Timestamp = CogniteTime.ToUnixTimeMilliseconds(StartTime!.Value),
                AvailableActions = GetAvailableActions().ToList(),
            };
        }

        /// <summary>
        /// Entry point registered with <see cref="IIntegrationSink.SetActionDispatcher"/>: routes
        /// pending actions to registered Start/Stop or custom handlers. Execution is handed off
        /// to independent tasks; cancellation callbacks also run off the check-in thread. A routing or
        /// cancellation failure for one action must not prevent dispatching the rest of the batch.
        /// </summary>
        /// <param name="actions">Actions pending execution, from a check-in or startup response.</param>
        private Task DispatchActions(IReadOnlyList<IntegrationAction> actions)
        {
            if (actions == null) throw new ArgumentNullException(nameof(actions));
            foreach (var action in actions)
            {
                try
                {
                    lock (_actionLock)
                    {
                        // In particular, the final flush may deliver more pending actions.
                        // Leave them pending in Odin for the next process instead of starting
                        // work after the cancellation snapshot and final status flush.
                        if (_stopping) return Task.CompletedTask;
                        DispatchAction(action);
                    }
                }
                catch (Exception ex)
                {
                    // Keep dispatching the remaining actions if routing/handoff fails.
                    _logger.LogError(ex, "Failed to dispatch action {ExternalId}", action.ExternalId);
                    try
                    {
                        QueueFailedAction(action.ExternalId, $"Internal error: {ex.Message}");
                    }
                    catch (Exception reportException)
                    {
                        _logger.LogError(reportException, "Failed to report failure for action {ExternalId}", action.ExternalId);
                    }
                }
            }
            return Task.CompletedTask;
        }

        private void DispatchAction(IntegrationAction action)
        {
            var actionName = action.ActionName;
            // Prefixes are not reserved: a custom action may be named "Start ..." or "Stop ...".
            // Only exact generated names for actionable tasks belong to the task handlers.
            if (actionName != null && _taskActions.TryGetValue(actionName, out var task))
            {
                if (task.Type == ActionType.start_task)
                {
                    if (action.Status == ActionStatus.cancel_pending)
                    {
                        // Still task-name based until EDG-965. Scheduler cancellation invokes
                        // user callbacks, so it must not block dispatch of the remaining batch.
                        Task.Run(() => CancelStartTaskAction(action.ExternalId, task.TaskName));
                    }
                    else
                    {
                        Task.Run(() => RunStartTaskAction(action.ExternalId, task.TaskName));
                    }
                }
                else if (action.Status != ActionStatus.cancel_pending)
                {
                    // Stop's own dispatch is instant; cancellation must not re-dispatch it.
                    Task.Run(() => RunStopTaskAction(action.ExternalId, task.TaskName));
                }
            }
            else if (action.Status == ActionStatus.cancel_pending)
            {
                HandleActionCancellation(action.ExternalId, actionName);
            }
            else if (actionName != null && _customActions.TryGetValue(actionName, out var customAction))
            {
                // Only protects against redelivery while this custom action is in flight.
                // Retaining completed outcomes until acknowledgement is a separate follow-up.
                var run = new InFlightAction();
                if (!_inFlightCustomActions.TryAdd(action.ExternalId, run))
                {
                    run.Dispose();
                    return;
                }
                Task.Run(() => RunCustomAction(action.ExternalId, action.CallMetadata, customAction, run));
            }
            else
            {
                QueueFailedAction(action.ExternalId, $"No action named '{actionName}' registered");
            }
        }

        /// <summary>
        /// Handle cancellation after generated task-action routing has not matched:
        /// cancel an in-flight custom execution by ID, or diagnose a stale task action.
        /// </summary>
        private void HandleActionCancellation(string externalId, string? actionName)
        {
            // The custom action's execution handler reports the eventual terminal status.
            if (_inFlightCustomActions.TryGetValue(externalId, out var run))
            {
                _ = run.CancelAsync(cts => CancelCustomAction(externalId, cts));
            }
            else if (actionName != null && !_customActions.ContainsKey(actionName))
            {
                // Prefixes only diagnose missing tasks; they must not route execution
                // or shadow a registered custom action with a Start/Stop-prefixed name.
                string? taskName = null;
                if (actionName.StartsWith(ActionNaming.StartPrefix, StringComparison.Ordinal))
                {
                    taskName = actionName.Substring(ActionNaming.StartPrefix.Length);
                }
                else if (actionName.StartsWith(ActionNaming.StopPrefix, StringComparison.Ordinal))
                {
                    taskName = actionName.Substring(ActionNaming.StopPrefix.Length);
                }
                if (taskName != null && !_taskNames.Contains(taskName))
                {
                    QueueFailedAction(externalId, $"No task named '{taskName}' is currently registered");
                }
            }
            // Not-in-flight custom cancellations and existing non-actionable tasks remain no-ops.
        }

        private void CancelCustomAction(string externalId, CancellationTokenSource cts)
        {
            try
            {
                cts.Cancel();
            }
            catch (ObjectDisposedException)
            {
                _logger.LogDebug("Ignoring cancellation for custom action {ExternalId}: its cancellation token source is already disposed",
                    externalId);
            }
            catch (AggregateException ex)
            {
                // The token is still cancelled if a callback throws; its handler owns reporting.
                _logger.LogError(ex, "Cancellation callback failed for custom action {ExternalId}", externalId);
            }
        }

        private void CancelStartTaskAction(string externalId, string taskName)
        {
            try
            {
                TaskScheduler.TryCancelTask(taskName, "Action cancelled");
            }
            catch (InvalidOperationException ex)
            {
                _logger.LogWarning(ex, "Task {TaskName} is no longer registered when cancelling action {ExternalId}", taskName, externalId);
                QueueFailedAction(externalId, $"No task named '{taskName}' is currently registered");
            }
            catch (Exception ex)
            {
                // Cancellation may already have taken effect. Let the original Start handler
                // report its outcome, rather than racing it with a spurious failed update.
                _logger.LogError(ex, "Failed to cancel Start action {ExternalId}", externalId);
            }
        }

        private async Task RunCustomAction(
            string externalId,
            IDictionary<string, string>? callMetadata,
            CustomAction<TConfig> customAction,
            InFlightAction run)
        {
            try
            {
                _sink.QueueActionUpdate(new ActionUpdate { ExternalId = externalId, Status = ActionStatus.running });

                var ctx = new ActionContext<TConfig>(
                    Config,
                    Destination,
                    _integrationExternalId,
                    externalId,
                    callMetadata == null ? null : new Dictionary<string, string>(callMetadata),
                    _sink);

                try
                {
                    await customAction.Target(ctx, run.Token).ConfigureAwait(false);
                    // No throw doesn't mean success: a well-behaved target may just return
                    // normally on cancellation instead of throwing, so check the token too.
                    // Uses whatever SetResult recorded (empty if never called) -- SetResult only
                    // records state, it doesn't queue anything.
                    _sink.QueueActionUpdate(new ActionUpdate
                    {
                        ExternalId = externalId,
                        Status = run.IsCancellationRequested ? ActionStatus.canceled : ActionStatus.succeeded,
                        ResultMessage = ctx.ResultMessage,
                        ResultMetadata = ctx.ResultMetadata?.ToDictionary(kv => kv.Key, kv => kv.Value),
                    });
                }
                catch (Exception ex)
                {
                    // Disambiguated by token state, not exception type: a target that throws
                    // while its own cancellation was requested is reporting the outcome of that
                    // cancellation (whether or not it threw a "clean" OperationCanceledException
                    // tied to this token), not a genuine failure.
                    if (run.IsCancellationRequested)
                    {
                        _sink.QueueActionUpdate(new ActionUpdate { ExternalId = externalId, Status = ActionStatus.canceled });
                    }
                    else if (ex is ActionError actionError)
                    {
                        _sink.QueueActionUpdate(new ActionUpdate
                        {
                            ExternalId = externalId,
                            Status = ActionStatus.failed,
                            ResultMessage = actionError.Message,
                            ResultMetadata = actionError.ResultMetadata.ToDictionary(kv => kv.Key, kv => kv.Value),
                        });
                    }
                    else
                    {
                        QueueFailedAction(externalId, ex.Message);
                    }
                }
            }
            catch (Exception ex)
            {
                // Mandatory catch-all for the entire unit of work: an unhandled exception
                // anywhere above (including in constructing ActionContext itself) must still
                // produce a terminal update, never a silently dropped action.
                _logger.LogError(ex, "Unhandled exception dispatching custom action '{Name}'", customAction.Name);
                QueueFailedAction(externalId, $"Internal error: {ex.Message}");
            }
            finally
            {
                // Tied to finally, not the happy-path return, so a leaked dedup entry (blocking
                // all future redelivery of this action id) or a silently-dropped action (if the
                // callback throws before reaching the code above that records an outcome) can't
                // happen.
                try
                {
                    // Keep ownership until callbacks finish; never dispose concurrently with Cancel.
                    await run.CompleteAsync().ConfigureAwait(false);
                }
                finally
                {
                    _inFlightCustomActions.TryRemove(externalId, out _);
                }
            }
        }

        private async Task RunStartTaskAction(string externalId, string taskName)
        {
            try
            {
                // Checked first, before registering a waiter or scheduling the task: doing this
                // after would leave a waiter that only clears once the task runs (a permanent
                // leak if it never becomes runnable) and a task armed to start on its own later,
                // with no action having reported success for that run.
                try
                {
                    if (!TaskScheduler.CanTaskRunNow(taskName))
                    {
                        QueueFailedAction(externalId, $"Task '{taskName}' is not currently able to run");
                        return;
                    }
                }
                catch (InvalidOperationException)
                {
                    // No task with this name is currently registered -- can happen if the action
                    // was advertised for a task that existed at a previous startup but not this
                    // one. CanTaskRunNow's other failure, ArgumentNullException, can't happen here
                    // since taskName is never null. Anything else unexpected still gets reported,
                    // via the catch-all below.
                    QueueFailedAction(externalId, $"No task named '{taskName}' is currently registered");
                    return;
                }

                Task waitTask;
                try
                {
                    // Register interest in this task's *next* completion before triggering it
                    // below, not after -- otherwise a fast-completing task could run to
                    // completion and flush its waiters before this handler starts listening,
                    // which -- combined with the deliberate no-timeout wait further down --
                    // would hang this handler forever. This mirrors the "register the wait, then
                    // trigger, then await" convention this codebase's own scheduler tests already
                    // use (see e.g. TaskSchedulerTest.TestScheduler), and matches
                    // WaitForNextEndOfTask's documented behavior of being safe to call before the
                    // task has (re)started, not just while it's already running.
                    waitTask = TaskScheduler.WaitForNextEndOfTask(taskName, Timeout.InfiniteTimeSpan);
                }
                catch (ArgumentException)
                {
                    // No task with this name is currently registered -- same case as above, in
                    // the narrow window where it was registered a moment ago but was removed
                    // between the two checks.
                    QueueFailedAction(externalId, $"No task named '{taskName}' is currently registered");
                    return;
                }

                if (!TaskScheduler.TryScheduleTaskNow(taskName))
                {
                    // waitTask above is left un-awaited here -- it will still eventually resolve
                    // when whatever run is *actually* in progress finishes, just with nothing
                    // listening; that's harmless (no unobserved-exception crash risk on any
                    // target framework this library multi-targets), and simpler than trying to
                    // unregister it.
                    QueueFailedAction(externalId, $"Task '{taskName}' is already running");
                    return;
                }

                _sink.QueueActionUpdate(new ActionUpdate { ExternalId = externalId, Status = ActionStatus.running });

                try
                {
                    // No timeout: a dedicated Task.Run per action means blocking here has no
                    // cost to other actions, and there is no principled upper bound on how long
                    // a legitimate task should be allowed to run -- an operator-issued Stop
                    // action or a cancel_pending redelivery (routed above, via TryCancelTask) is
                    // the intended way to end this early, not a client-side timeout.
                    await waitTask.ConfigureAwait(false);
                    _sink.QueueActionUpdate(new ActionUpdate { ExternalId = externalId, Status = ActionStatus.succeeded });
                }
                catch (Exception ex)
                {
                    // WaitForNextEndOfTask can surface a cancellation two different shapes,
                    // depending on which of two independent paths in ExtractorTaskScheduler
                    // resolves this waiter first: RegisteredTask.FinishTask (the task actually
                    // finished, exception rethrown directly via ExceptionDispatchInfo -- a bare
                    // TaskCanceledException) or RegisteredTask.Cancel (an explicit cancel request
                    // was made and immediately unblocks any current waiter via TrySetCanceled,
                    // before the task itself has necessarily finished -- accessing .Result on
                    // that then throws AggregateException wrapping a TaskCanceledException; the
                    // existing TaskSchedulerTest.TestWaitWhenCancel documents this same shape).
                    // Unwrap once so both exception shapes are inspected the same way below.
                    var actual = (ex as AggregateException)?.Flatten().InnerException ?? ex;
                    if (actual is TaskCanceledException)
                    {
                        _sink.QueueActionUpdate(new ActionUpdate { ExternalId = externalId, Status = ActionStatus.canceled });
                    }
                    else
                    {
                        QueueFailedAction(externalId, actual.Message);
                    }
                }
            }
            catch (Exception ex)
            {
                // Mandatory catch-all for the entire unit of work: an unhandled exception
                // anywhere above must still produce a terminal update, never a silently dropped
                // action.
                _logger.LogError(ex, "Unhandled exception dispatching Start action for task {TaskName}", taskName);
                QueueFailedAction(externalId, $"Internal error: {ex.Message}");
            }
        }

        private void RunStopTaskAction(string externalId, string taskName)
        {
            try
            {
                bool isTaskCancelled;
                try
                {
                    isTaskCancelled = TaskScheduler.TryCancelTask(taskName, "Stop action");
                }
                catch (InvalidOperationException)
                {
                    // No task with this name is currently registered -- same reasoning as the
                    // equivalent catch in RunStartTaskAction.
                    QueueFailedAction(externalId, $"No task named '{taskName}' is currently registered");
                    return;
                }

                if (isTaskCancelled)
                {
                    // `succeeded`, not `canceled` -- `canceled` means the target task's own run
                    // was interrupted, a distinct outcome from "the stop request succeeded". Not
                    // enforced server-side; purely for cross-language parity with
                    // python-extractor-utils.
                    _sink.QueueActionUpdate(new ActionUpdate { ExternalId = externalId, Status = ActionStatus.succeeded });
                }
                else
                {
                    QueueFailedAction(externalId, $"Task '{taskName}' is not currently running");
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Unhandled exception dispatching Stop action for task {TaskName}", taskName);
                QueueFailedAction(externalId, $"Internal error: {ex.Message}");
            }
        }

        private void QueueFailedAction(string externalId, string message)
        {
            _sink.QueueActionUpdate(new ActionUpdate { ExternalId = externalId, Status = ActionStatus.failed, ResultMessage = message });
        }

        /// <summary>
        /// Start the extractor and wait for it to finish.
        /// </summary>
        /// <param name="token"></param>
        /// <returns></returns>
        public async Task Start(CancellationToken token)
        {
            try
            {
                await Init(token).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to initialize extractor: {Message}", ex.Message);
                Fatal($"Failed to initialize extractor: {ex.Message}", ex.StackTrace?.ToString());
                throw;
            }
            StartTime = DateTime.UtcNow;
            // Register the action dispatcher before the check-in worker starts, so no pending
            // actions in the very first startup response can arrive with nothing registered to
            // handle them.
            _sink.SetActionDispatcher(DispatchActions);
            // Start monitoring the task scheduler and run sink.
            AddMonitoredTask(TaskScheduler.Run, "TaskScheduler");
            AddMonitoredTask(t => _sink.RunPeriodicCheckIn(t, GetStartupRequest()), SchedulerTaskResult.Unexpected, "CheckInWorker");

            try
            {
                await Scheduler.WaitForAll().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                var flattened = CommonUtils.SimplifyException(ex);
                _logger.LogError(flattened, "Extractor failed: {Message}", flattened.Message);
                NewError(ErrorLevel.error, $"{flattened.Message}", flattened.StackTrace?.ToString()).Instant();
                throw;
            }
        }

        /// <summary>
        /// Verify that the extractor is configured correctly.
        ///
        /// Does nothing by default.
        /// </summary>
        /// <returns>Task</returns>
        protected virtual Task TestConfig()
        {
            return Task.CompletedTask;
        }


        /// <inheritdoc />
        public override ExtractorError NewError(ErrorLevel level, string description, string? details = null, DateTime? now = null, string? type = null, int? configRevision = null)
        {
            return new ExtractorError(level, description, _sink, details, null, now, type, configRevision);
        }

        /// <summary>
        /// Flush the sink, writing any pending task events to integrations.
        /// </summary>
        /// <param name="token"></param>
        /// <returns></returns>
        protected async Task FlushSink(CancellationToken token)
        {
            await _sink.Flush(token).ConfigureAwait(false);
        }

        /// <summary>
        /// Perform graceful shutdown.
        ///
        /// By default this method cancels the task scheduler and flushes the sink,
        /// you may wish to override this entirely to perform a different sequence of
        /// cleanup tasks.
        ///
        /// Shutdown is required to be idempotent, and should not throw exceptions.
        /// </summary>
        /// <returns></returns>
        protected virtual Task ShutdownInternal()
        {
            lock (_actionLock)
            {
                if (_shutdownTask != null) return _shutdownTask;
                // Admission and custom registration use this same gate. Every action admitted
                // before shutdown is visible to the cancellation pass; none can enter after it.
                _stopping = true;
                _shutdownTask = Task.Run(ShutdownActionsAndFlush);
                return _shutdownTask;
            }
        }

        private async Task ShutdownActionsAndFlush()
        {
            // Signal independently: one blocking callback must not prevent the remaining
            // actions or the scheduler from receiving their own cancellation requests.
            foreach (var action in _inFlightCustomActions)
            {
                _ = action.Value.CancelAsync(cts => CancelCustomAction(action.Key, cts));
            }

            async Task CancelSchedulerAsync()
            {
                try
                {
                    await TaskScheduler.CancelInnerAndWait(20000, this).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error cancelling task scheduler during shutdown");
                    // Cancel() sets the token before invoking callbacks. If a callback threw,
                    // tasks may still be unwinding even though CancelInnerAndWait skipped its wait.
                    try
                    {
                        await TaskScheduler.CompletedTask.ConfigureAwait(false);
                    }
                    catch (Exception completionException)
                    {
                        _logger.LogError(completionException, "Task scheduler failed during shutdown");
                    }
                }
            }

            async Task WaitForSchedulerAsync()
            {
                var cancellation = Task.Run(CancelSchedulerAsync);
                // CancelInnerAndWait's timeout starts after synchronous token callbacks.
                // Bound those callbacks too, without disposing resources still in use.
                if (await Task.WhenAny(cancellation, Task.Delay(20000)).ConfigureAwait(false) != cancellation)
                {
                    _logger.LogWarning("Task scheduler cancellation did not finish within the shutdown timeout");
                }
                else
                {
                    await cancellation.ConfigureAwait(false);
                }
            }

            async Task WaitForCustomActionsAsync()
            {
                var wait = Stopwatch.StartNew();
                while (!_inFlightCustomActions.IsEmpty && wait.ElapsedMilliseconds < 5000)
                {
                    await Task.Delay(50).ConfigureAwait(false);
                }
                if (!_inFlightCustomActions.IsEmpty)
                {
                    _logger.LogWarning("Custom actions did not finish within the shutdown timeout");
                }
            }

            await Task.WhenAll(WaitForSchedulerAsync(), WaitForCustomActionsAsync()).ConfigureAwait(false);
            // Next, flush any remaining task updates.
            try
            {
                await FlushSink(CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                // A sink failure must not poison the cached shutdown task and prevent outer
                // lifetime cancellation/disposal on every subsequent attempt.
                _logger.LogError(ex, "Failed to flush action and task updates during shutdown");
            }
        }

        /// <summary>
        /// Shut down the extractor.
        ///
        /// This calls `ShutdownInternal` then cancels the token.
        ///
        /// If you wish to change shutdown behavior, override `ShutdownInternal`.
        /// </summary>
        /// <returns></returns>
        public async Task Shutdown()
        {
            await ShutdownInternal().ConfigureAwait(false);
            Source?.Cancel();
        }

        /// <summary>
        /// Dispose asynchronously, override this to clean up your resources
        /// on shutdown.
        ///
        /// Prefer overriding `ShutdownInternal` instead or in addition to this method,
        /// if what you are doing is performing a graceful shutdown.
        ///
        /// Typically, you will want to override this to call `Dispose` on any disposable
        /// resources, and `ShutdownInternal` to perform graceful cleanup.
        /// </summary>
        /// <returns></returns>
        protected virtual async ValueTask DisposeAsyncCore()
        {
            await ShutdownInternal().ConfigureAwait(false);
            // Finally, cancel the outer token source.
            Source?.Cancel();
            Source?.Dispose();
            Source = null!;
        }

        /// <summary>
        /// Dispose the extractor asynchronously.
        /// </summary>
        /// <returns></returns>
        public async ValueTask DisposeAsync()
        {
            try
            {
                await DisposeAsyncCore().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Failed to dispose of extractor: {}", ex.Message);
            }
            GC.SuppressFinalize(this);
        }
    }
}
