using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extractor.Common;
using Cognite.Extractor.Utils;
using Cognite.ExtractorUtils.Unstable.Tasks;
using CogniteSdk.Alpha;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Cognite.ExtractorUtils.Unstable
{

    /// <summary>
    /// Result of a long-running extractor task.
    /// 
    /// Used to indicate whether a task is expected to exit, and whether
    /// an exit should be considered the extractor crashing.
    /// </summary>
    public enum ExtractorTaskResult
    {
        /// <summary>
        /// Task was expected to shut down.
        /// </summary>
        Expected,
        /// <summary>
        /// Task should not have shut down.
        /// </summary>
        Unexpected,
    }

    /// <summary>
    /// Class wrapping a task being monitored by the extractor.
    /// This is simply a way to run background tasks that
    /// don't get reported to integrations.
    /// </summary>
    class MonitoredTask
    {
        /// <summary>
        /// Task to monitor.
        /// </summary>
        public Task<ExtractorTaskResult> Task { get; }
        /// <summary>
        /// Task name, does not have to be unique.
        /// </summary>
        public string Name { get; }

        private CancellationTokenSource _source;

        public MonitoredTask(Func<CancellationToken, Task<ExtractorTaskResult>> task, string name, CancellationToken token)
        {
            _source = CancellationTokenSource.CreateLinkedTokenSource(token);
            Task = task(_source.Token);
            Name = name;
        }

        public void Cancel()
        {
            _source.Cancel();
        }

        public async Task<ExtractorTaskResult> WaitForCompletion()
        {
            return await Task.ConfigureAwait(false);
        }

        public bool IsCanceled()
        {
            return _source.IsCancellationRequested;
        }
    }

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
        private readonly List<MonitoredTask> _monitoredTaskList = new List<MonitoredTask>();
        private readonly IIntegrationSink _sink;

        /// <summary>
        /// Task scheduler containing all public extractor tasks.
        /// </summary>
        protected ExtractorTaskScheduler TaskScheduler { get; }

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

        private object _lock = new object();

        private ManualResetEvent _triggerEvent = new ManualResetEvent(false);


        /// <summary>
        /// Constructor, usable with dependency injection.
        /// </summary>
        /// <param name="config">Configuration object</param>
        /// <param name="provider">Service provider used to build this</param>
        /// <param name="taskScheduler">Task scheduler.</param>
        /// <param name="sink">Sink for extractor task updates and errors.</param>
        /// <param name="destination">Cognite destination.</param>
        public BaseExtractor(
            TConfig config,
            IServiceProvider provider,
            ExtractorTaskScheduler taskScheduler,
            IIntegrationSink sink,
            CogniteDestination? destination = null
        )
        {
            Config = config;
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

        private void InitBase(CancellationToken token)
        {
            if (Source != null) throw new InvalidOperationException("Extractor already started");
            Source = CancellationTokenSource.CreateLinkedTokenSource(token);
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
        protected void AddMonitoredTask(Func<CancellationToken, Task<ExtractorTaskResult>> task, string name)
        {
            if (task == null) throw new ArgumentNullException(nameof(task));
            if (name == null) throw new ArgumentNullException(nameof(name));
            if (Source == null) throw new InvalidOperationException("Attempt to add monitored task without starting the extractor first.");
            lock (_lock)
            {
                _monitoredTaskList.Add(new MonitoredTask(task, name, Source.Token));
                _triggerEvent.Set();
            }
        }

        /// <summary>
        /// Cancel a monitored task, then wait for it to complete.
        /// 
        /// This is typically used for ordered shutdown.
        /// </summary>
        /// <param name="name">Name of task to cancel. Note that names are not
        /// required to be unique here. If there are duplicates, this will cancel the
        /// first matching task.
        /// </param>
        /// <returns></returns>
        protected async Task CancelMonitoredTaskAndWait(string name)
        {
            MonitoredTask? task;
            lock (_lock)
            {
                task = _monitoredTaskList.FirstOrDefault(t => t.Name == name);
            }
            if (task == null) return;
            task.Cancel();
            try
            {
                await task.WaitForCompletion().ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // ignore
            }
        }

        /// <summary>
        /// Add a monitored task that should be watched by the extractor.
        /// 
        /// Use this for tasks that will not be reported to integrations,
        /// but that you still want to monitor, so that the extractor can crash
        /// if they fail or exit unexpectedly.
        /// 
        /// This variant takes a static ExtractorTaskResult, to indicate whether the
        /// task is expected to terminate on its own or not.
        /// </summary>
        /// <param name="task">Task to monitor.</param>
        /// <param name="staticResult">Whether the task exiting on its own without cancellation
        /// should be considered an error.</param>
        /// <param name="name">Task name, just used for logging.</param>
        /// <exception cref="ArgumentNullException"></exception>
        protected void AddMonitoredTask(Func<CancellationToken, Task> task, ExtractorTaskResult staticResult, string name)
        {
            if (task == null) throw new ArgumentNullException(nameof(task));
            if (name == null) throw new ArgumentNullException(nameof(name));
            if (Source == null) throw new InvalidOperationException("Attempt to add monitored task without starting the extractor first.");
            lock (_lock)
            {
                _monitoredTaskList.Add(new MonitoredTask(async (token) =>
                {
                    await task(token).ConfigureAwait(false);
                    return staticResult;
                }, name, Source.Token));
                _triggerEvent.Set();
            }
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
        }

        /// <summary>
        /// Start the extractor and wait for it to finish.
        /// </summary>
        /// <param name="token"></param>
        /// <returns></returns>
        public async Task Start(CancellationToken token)
        {
            await Init(token).ConfigureAwait(false);
            // TODO: Post startup message to integrations.

            // Start monitoring the task scheduler and run sink.
            AddMonitoredTask(TaskScheduler.Run, "TaskScheduler");
            AddMonitoredTask(t => _sink.RunPeriodicCheckin(t), ExtractorTaskResult.Unexpected, "CheckInWorker");

            while (!Source.IsCancellationRequested)
            {
                var toWaitFor = new List<Task>();
                lock (_lock)
                {
                    _triggerEvent.Reset();
                    var toRemove = new HashSet<MonitoredTask>();
                    foreach (var monitored in _monitoredTaskList)
                    {
                        if (!monitored.Task.IsCompleted) continue;

                        if (!monitored.Task.IsFaulted && !monitored.Task.IsCanceled && !monitored.IsCanceled())
                        {
                            var result = monitored.Task.Result;
                            if (result == ExtractorTaskResult.Unexpected)
                            {
                                _logger.LogError("Internal task {Name} completed, but was not expected to stop, restarting extractor.", monitored.Name);
                                Fatal($"Internal task {monitored.Name} completed, but was not expected to stop, restarting extractor.");
                                return;
                            }
                        }
                        else if (monitored.Task.IsFaulted)
                        {
                            var exc = monitored.Task.Exception;
                            var flattenedExc = exc?.InnerException ?? exc;
                            _logger.LogError(flattenedExc, "Internal task {Name} failed, restarting extractor: {Message}", monitored.Name, flattenedExc?.Message);
                            Fatal($"Internal task {monitored.Name} failed, restarting extractor: {flattenedExc?.Message}", flattenedExc?.StackTrace?.ToString());
                            return;
                        }
                        _logger.LogDebug("Internal task {Name} completed.", monitored.Name);
                        toRemove.Add(monitored);
                    }
                    _monitoredTaskList.RemoveAll(r => toRemove.Contains(r));

                    toWaitFor.AddRange(_monitoredTaskList.Select(mt => mt.Task));
                }

                toWaitFor.Add(CommonUtils.WaitAsync(_triggerEvent, Timeout.InfiniteTimeSpan, Source.Token));

                await Task.WhenAny(toWaitFor).ConfigureAwait(false);
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
        public override ExtractorError NewError(ErrorLevel level, string description, string? details = null, DateTime? now = null)
        {
            return new ExtractorError(level, description, _sink, details, null, now);
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