using System;
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

        private readonly Dictionary<string, CustomAction<TConfig>> _customActions = new Dictionary<string, CustomAction<TConfig>>();

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
            foreach (var task in TaskScheduler.GetRegisteredTasks())
            {
                if (!task.Action) continue;
                if (action.Name == ActionNaming.StartActionName(task.Name) || action.Name == ActionNaming.StopActionName(task.Name))
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
            await InitActions().ConfigureAwait(false);
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
            // First, shut down the task scheduler.
            await TaskScheduler.CancelInnerAndWait(20000, this).ConfigureAwait(false);
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
