using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
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

        /// <summary>
        /// Custom actions currently running, keyed by action externalId, mapped to the
        /// CancellationTokenSource passed to that action's target.
        ///
        /// - Dedup: skip re-dispatching an action redelivered before it finishes (see DispatchAction).
        /// - Cancel-in-flight: cancel a run when its `cancel_pending` redelivery arrives (see RunCustomAction).
        /// - Start/Stop don't need this: they key off task name via <see cref="TaskScheduler"/> instead.
        /// </summary>
        private readonly ConcurrentDictionary<string, CancellationTokenSource> _inFlightCustomActions
            = new ConcurrentDictionary<string, CancellationTokenSource>();

        // True once ShutdownInternal has started. Stops DispatchAction from starting new custom
        // actions after its one-time cancel-everything loop has already run.
        private volatile bool _shuttingDown;

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
            // ConnectionConfig may be missing (e.g. in unit tests, or an offline extractor) --
            // fall back to empty rather than throw. Harmless either way: with no real
            // integration, DispatchActions can never be invoked, since it's only reachable via a
            // checkin/startup response.
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
            // Only blocks an exact match with a real task's Start/Stop name -- "Stop Backup" is
            // fine to register unless a task is actually named "Backup" (DispatchAction falls
            // through to custom actions otherwise, see IsActionableTask). Doesn't catch a task
            // added later, after startup.
            foreach (var task in TaskScheduler.GetRegisteredTasks())
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
            RegisterBuiltInActions();
            await InitActions().ConfigureAwait(false);
        }

        /// <summary>
        /// Register the framework's own built-in actions, before any user-defined
        /// <see cref="InitActions"/> override runs -- so built-ins are always earlier in
        /// registration order, matching the ordering convention the Python reference
        /// implementation made deterministic and test-covered.
        /// </summary>
        private void RegisterBuiltInActions()
        {
            RegisterAction(new CustomAction<TConfig>(
                FetchLogsAction.Name,
                (ctx, token) => FetchLogsAction.RunAsync(
                    ctx,
                    Provider.GetService<Cognite.Extractor.Logging.LoggerConfig>(),
                    Provider.GetService<System.Net.Http.IHttpClientFactory>(),
                    token),
                "Upload rotated log files covering a requested date range to CDF Files."));
        }

        /// <summary>
        /// Build the AvailableActions list to advertise at startup: an auto-generated Start/Stop
        /// pair for every actionable task (in task-registration order), followed by custom
        /// actions in registration order.
        /// </summary>
        private IEnumerable<AvailableActionWrite> GetAvailableActions()
        {
            foreach (var task in TaskScheduler.GetRegisteredTasks())
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
                Tasks = TaskScheduler.GetRegisteredTasks().ToList(),
                Extractor = version,
                // StartTime is not null here, as this is called after Init.
                Timestamp = CogniteTime.ToUnixTimeMilliseconds(StartTime!.Value),
                AvailableActions = GetAvailableActions().ToList(),
            };
        }

        /// <summary>
        /// Entry point registered with <see cref="IIntegrationSink.SetActionDispatcher"/>: routes
        /// each pending action to its handler on its own independent <see cref="Task.Run(Action)"/>,
        /// and returns promptly without waiting for any of them to finish. Overlapping dispatch,
        /// both across actions and across check-in cycles, is intentional -- action externalIds
        /// are server-assigned and the check-in interval is far larger than typical handling
        /// time, so there is no queue to preserve ordering in here.
        /// </summary>
        /// <param name="actions">Actions pending execution, from a check-in or startup response.</param>
        private Task DispatchActions(IReadOnlyList<IntegrationAction> actions)
        {
            if (actions == null) throw new ArgumentNullException(nameof(actions));
            foreach (var action in actions)
            {
                DispatchAction(action);
            }
            return Task.CompletedTask;
        }

        private void DispatchAction(IntegrationAction action)
        {
            var actionName = action.ActionName;
            if (actionName != null)
            {
                if (actionName.StartsWith(ActionNaming.StartPrefix, StringComparison.Ordinal)
                    && IsActionableTask(actionName.Substring(ActionNaming.StartPrefix.Length)))
                {
                    var taskName = actionName.Substring(ActionNaming.StartPrefix.Length);
                    if (action.Status == ActionStatus.cancel_pending)
                    {
                        // Cancel via TryCancelTask; RunStartTaskAction's own wait reports the
                        // result. No-op if nothing is running yet (a microseconds-wide race,
                        // irrelevant next to a seconds-wide checkin interval). No try/catch
                        // needed: IsActionableTask already confirmed the task exists.
                        TaskScheduler.TryCancelTask(taskName, "Action cancelled");
                        return;
                    }
                    Task.Run(() => RunStartTaskAction(action.ExternalId, taskName));
                    return;
                }
                if (actionName.StartsWith(ActionNaming.StopPrefix, StringComparison.Ordinal)
                    && IsActionableTask(actionName.Substring(ActionNaming.StopPrefix.Length)))
                {
                    var taskName = actionName.Substring(ActionNaming.StopPrefix.Length);
                    if (action.Status == ActionStatus.cancel_pending)
                    {
                        // Stop's own dispatch is near-instant, so this only fires if the Stop was
                        // cancelled before ever being dispatched -- report canceled explicitly,
                        // or the server redelivers this forever (nothing else ever resolves it).
                        _sink.QueueActionUpdate(new ActionUpdate { ExternalId = action.ExternalId, Status = ActionStatus.canceled });
                        return;
                    }
                    Task.Run(() => RunStopTaskAction(action.ExternalId, taskName));
                    return;
                }
                // Prefix matched but no such task exists -- e.g. a custom action named "Stop
                // Backup" with no task "Backup". Fall through to custom-action dispatch below.
            }

            if (action.Status == ActionStatus.cancel_pending)
            {
                // Custom action cancel-in-flight: find its CancellationTokenSource and cancel it.
                // RunCustomAction's own try/catch/finally reports the final status once the
                // target unwinds -- nothing is queued here.
                if (_inFlightCustomActions.TryGetValue(action.ExternalId, out var cts))
                {
                    lock (cts)
                    {
                        try
                        {
                            cts.Cancel();
                        }
                        catch (ObjectDisposedException)
                        {
                            // Safe to ignore: the action completed and disposed its CTS concurrently.
                        }
                        catch (AggregateException ex)
                        {
                            // Unlike ObjectDisposedException above, this isn't expected: a
                            // cancellation callback threw. Cancellation still took effect -- log
                            // it rather than swallow it.
                            _logger.LogWarning(ex, "A cancellation callback threw while cancelling action {ExternalId}", action.ExternalId);
                        }
                    }
                }
                else
                {
                    // Nothing in flight: either already finished (harmless no-op, already
                    // terminal) or never dispatched by this process (e.g. cancelled before we
                    // ever saw it, or after a restart). Report canceled either way, or the server
                    // redelivers this forever.
                    _sink.QueueActionUpdate(new ActionUpdate { ExternalId = action.ExternalId, Status = ActionStatus.canceled });
                }
                return;
            }

            if (actionName != null && _customActions.TryGetValue(actionName, out var customAction))
            {
                if (_shuttingDown)
                {
                    // Shutdown's own cancel loop already ran once -- don't start new work it
                    // would never see.
                    return;
                }
                // - Guards against the same action being redelivered before it finishes --
                //   custom-action callbacks may not be safe to run twice at once.
                // - Start doesn't need this: TryScheduleTaskNow's own ActiveTask check already
                //   stops a task from starting twice, so a redelivered Start just fails fast.
                // - Linked to Source (not standalone) as a backstop: if ShutdownInternal's cancel
                //   loop ever misses this action, Source.Cancel() still catches it.
                var cts = CancellationTokenSource.CreateLinkedTokenSource(Source.Token);
                if (!_inFlightCustomActions.TryAdd(action.ExternalId, cts))
                {
                    cts.Dispose();
                    return;
                }
                Task.Run(() => RunCustomAction(action.ExternalId, action.CallMetadata, customAction, cts));
                return;
            }

            QueueFailedAction(action.ExternalId, $"No action named '{actionName}' registered");
        }

        /// <summary>
        /// Whether "Start/Stop {taskName}" refers to a real, registered actionable task -- not
        /// just a custom action name that happens to share the reserved prefix.
        /// </summary>
        private bool IsActionableTask(string taskName)
        {
            return TaskScheduler.GetRegisteredTasks().Any(t => t.Action && t.Name == taskName);
        }

        private async Task RunCustomAction(
            string externalId,
            IDictionary<string, string>? callMetadata,
            CustomAction<TConfig> customAction,
            CancellationTokenSource cts)
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
                    await customAction.Target(ctx, cts.Token).ConfigureAwait(false);
                    // No throw doesn't mean success -- a target may just return normally on
                    // cancellation. Uses whatever SetResult recorded (empty if never called).
                    _sink.QueueActionUpdate(new ActionUpdate
                    {
                        ExternalId = externalId,
                        Status = cts.IsCancellationRequested ? ActionStatus.canceled : ActionStatus.succeeded,
                        ResultMessage = ctx.ResultMessage,
                        ResultMetadata = ctx.ResultMetadata?.ToDictionary(kv => kv.Key, kv => kv.Value),
                    });
                }
                catch (Exception ex)
                {
                    // Judged by token state, not exception type: if cancellation was requested,
                    // any exception here is that cancellation playing out, not a real failure.
                    if (cts.IsCancellationRequested)
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
                // Catches anything unhandled above, including ActionContext construction itself
                // -- this action must never go without a terminal update.
                _logger.LogError(ex, "Unhandled exception dispatching custom action '{Name}'", customAction.Name);
                QueueFailedAction(externalId, $"Internal error: {ex.Message}");
            }
            finally
            {
                // In finally, not after the happy path, so this always runs even if the code
                // above throws before recording an outcome.
                _inFlightCustomActions.TryRemove(externalId, out _);
                // Locked: DispatchAction/ShutdownInternal may be looking this cts up right now.
                lock (cts)
                {
                    cts.Dispose();
                }
            }
        }

        private async Task RunStartTaskAction(string externalId, string taskName)
        {
            try
            {
                // Checked before registering a waiter or scheduling the task: otherwise a failure
                // here would leave a waiter that never clears, and a task armed to start later
                // with no action ever reporting success for it.
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
                    // Task not registered -- e.g. advertised at a previous startup, not this one.
                    QueueFailedAction(externalId, $"No task named '{taskName}' is currently registered");
                    return;
                }

                Task waitTask;
                try
                {
                    // Register the wait before triggering the task below, not after -- a
                    // fast-completing task could otherwise finish and clear its waiters before
                    // this starts listening, hanging forever given the no-timeout wait below.
                    waitTask = TaskScheduler.WaitForNextEndOfTask(taskName, Timeout.InfiniteTimeSpan);
                }
                catch (ArgumentException)
                {
                    // Same "task not registered" case as above, in the narrow window between the
                    // two checks.
                    QueueFailedAction(externalId, $"No task named '{taskName}' is currently registered");
                    return;
                }

                if (!TaskScheduler.TryScheduleTaskNow(taskName))
                {
                    // waitTask is left un-awaited -- harmless; it resolves on its own once
                    // whatever run is actually in progress finishes.
                    QueueFailedAction(externalId, $"Task '{taskName}' is already running");
                    return;
                }

                _sink.QueueActionUpdate(new ActionUpdate { ExternalId = externalId, Status = ActionStatus.running });

                try
                {
                    // No timeout: one Task.Run per action means blocking costs nothing else, and
                    // a Stop action or cancel_pending (handled above) is the intended way to end
                    // this early, not a client-side timeout.
                    await waitTask.ConfigureAwait(false);
                    _sink.QueueActionUpdate(new ActionUpdate { ExternalId = externalId, Status = ActionStatus.succeeded });
                }
                catch (Exception ex)
                {
                    // A cancellation can arrive as a bare TaskCanceledException or one wrapped in
                    // AggregateException, depending which of two paths in ExtractorTaskScheduler
                    // resolves first (see TaskSchedulerTest.TestWaitWhenCancel) -- unwrap once so
                    // both are treated the same way below.
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
                // Catches anything unhandled above -- this action must never go without a
                // terminal update.
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
                    // Same "task not registered" case as RunStartTaskAction.
                    QueueFailedAction(externalId, $"No task named '{taskName}' is currently registered");
                    return;
                }

                if (isTaskCancelled)
                {
                    // `succeeded`, not `canceled` -- `canceled` means the task's own run was
                    // interrupted, a different thing from "the stop request succeeded". Not
                    // required server-side; matches python-extractor-utils' convention.
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
            // Register before the check-in worker starts, so the first startup response can't
            // arrive with no dispatcher registered yet.
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
        protected virtual async Task ShutdownInternal()
        {
            // Set first, so nothing new can be dispatched after the cancel loop below runs.
            _shuttingDown = true;
            // Signal in-flight custom actions now -- Source isn't cancelled until the very end of
            // Shutdown()/DisposeAsyncCore(), so without this they'd keep running, unsignalled,
            // for this whole window (each token is also linked to Source as a backstop).
            foreach (var kvp in _inFlightCustomActions)
            {
                var externalId = kvp.Key;
                var cts = kvp.Value;
                lock (cts)
                {
                    try
                    {
                        cts.Cancel();
                    }
                    catch (ObjectDisposedException)
                    {
                        // Safe to ignore: the action completed and disposed its CTS concurrently.
                    }
                    catch (AggregateException ex)
                    {
                        // Unlike above, this isn't expected: a cancellation callback threw. Log
                        // it, rather than swallow it -- cancellation still took effect.
                        _logger.LogWarning(ex, "A cancellation callback threw while shutting down action {ExternalId}", externalId);
                    }
                }
            }
            // Wait for the scheduler and custom actions concurrently, not back-to-back -- both
            // were already signalled above, and custom actions need this wait or their terminal
            // `canceled` update could arrive after the flush below and be lost.
            async Task WaitForCustomActionsAsync()
            {
                var waitStart = DateTime.UtcNow;
                while (!_inFlightCustomActions.IsEmpty && (DateTime.UtcNow - waitStart).TotalMilliseconds < 5000)
                {
                    await Task.Delay(50).ConfigureAwait(false);
                }
            }
            await Task.WhenAll(TaskScheduler.CancelInnerAndWait(20000, this), WaitForCustomActionsAsync()).ConfigureAwait(false);
            // Next, flush any remaining task updates.
            await FlushSink(CancellationToken.None).ConfigureAwait(false);
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
