using System;
using System.Collections.Generic;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extensions;
using Cognite.Extractor.Common;
using Microsoft.Extensions.Logging;

namespace Cognite.ExtractorUtils.Unstable.Tasks
{
    /// <summary>
    /// Base class for schedulable tasks.
    /// </summary>
    public abstract class BaseSchedulableTask
    {
        /// <summary>
        /// Return whether this task failing should be considered fatal
        /// and terminate the task scheduler completely.
        /// </summary>
        public abstract bool ErrorIsFatal { get; }

        /// <summary>
        /// Return whether the task can run now.
        /// 
        /// The task should make sure to call the callback provided in `RegisterReadyCallback`
        /// once this will return `true`.
        /// 
        /// Once this returns `true`, the task may be started immediately.
        /// 
        /// Note that this may be called frequently, and should not do any expensive calculations.
        /// </summary>
        public abstract bool CanRunNow();

        /// <summary>
        /// Callback registered on task startup, should be called
        /// to notify the task runner that this task is now ready to run.
        /// </summary>
        protected Action? ReadyCallback { get; private set; }

        /// <summary>
        /// Register a callback that should be invoked once `CanRunNow` returns `true`.
        /// 
        /// You may call this callback even if `CanRunNow` isn't guaranteed to return `true`.
        /// </summary>
        /// <param name="callback"></param>
        public virtual void RegisterReadyCallback(Action callback)
        {
            ReadyCallback = callback;
        }

        /// <summary>
        /// Run the task, this may throw an exception which is treated as a fatal error.
        /// </summary>
        /// <param name="task">Callbacks for reporting errors.</param>
        /// <param name="token">Optional cancellation token.</param>
        public abstract Task<TaskUpdatePayload?> Run(BaseErrorReporter task, CancellationToken token);

        /// <summary>
        /// Unique name of the task.
        /// </summary>
        public abstract string Name { get; }

        /// <summary>
        /// Task schedule, how often it runs automatically.
        /// Should be null if the task only runs manually or on startup.
        /// </summary>
        public virtual ITimeSpanProvider? Schedule { get; }
    }

    internal sealed class RunningTaskInfo : IDisposable
    {
        public Task<TaskUpdatePayload?> Task { get; }
        public CancellationTokenSource Source { get; }

        public RunningTaskInfo(Task<TaskUpdatePayload?> activeTask, CancellationTokenSource tokenSource)
        {
            Source = tokenSource;
            Task = activeTask;
        }

        public void Dispose()
        {
            Source.Dispose();
        }
    }

    internal sealed class RegisteredTask : IDisposable
    {
        public DateTime? NextRun { get; set; }
        public BaseSchedulableTask Operation { get; }

        public RunningTaskInfo? ActiveTask { get; private set; }

        private List<TaskCompletionSource<Exception?>> _waiters = new List<TaskCompletionSource<Exception?>>();

        private object _lock = new object();
        private TaskReporter _reporter;

        public RegisteredTask(BaseSchedulableTask operation, TaskReporter reporter, bool runImmediately)
        {
            Operation = operation;
            _reporter = reporter;
            if (runImmediately)
            {
                NextRun = DateTime.UtcNow;
            }
            else if (operation.Schedule != null)
            {
                NextRun = DateTime.UtcNow + operation.Schedule.Value;
            }
        }

        public void Run(DateTime now, CancellationToken token)
        {
            if (ActiveTask != null) throw new InvalidOperationException("Attempt to start an already running task");
            var source = CancellationTokenSource.CreateLinkedTokenSource(token);
            var task = Task.Run(() => Operation.Run(_reporter, source.Token), source.Token);

            if (Operation.Schedule != null)
            {
                NextRun = now + Operation.Schedule.Value;
            }
            else
            {
                NextRun = null;
            }

            ActiveTask = new RunningTaskInfo(task, source);
            _reporter.ReportStart(null, now);
        }

        public void Cancel()
        {
            lock (_lock)
            {
                if (ActiveTask != null)
                {
                    ActiveTask.Source.Cancel();
                    foreach (var waiter in _waiters)
                    {
                        waiter.TrySetCanceled();
                    }
                    _waiters.Clear();
                }
            }
        }

        public void AddWaiter(TaskCompletionSource<Exception?> waiter)
        {
            lock (_lock)
            {
                _waiters.Add(waiter);
            }
        }

        public void FinishTask(DateTime now)
        {
            if (ActiveTask == null || !ActiveTask.Task.IsCompleted) throw new InvalidOperationException("Attempt to finish a task that isn't completed");
            lock (_lock)
            {
                var finished = ActiveTask;
                finished.Dispose();
                ActiveTask = null;

                Exception? exc = finished.Task.Exception?.Flatten();

                if (exc?.InnerException != null)
                {
                    exc = exc.InnerException;
                }

                // Report a fatal error to integrations if the task exited non-cleanly.
                // This typically means a crash or manual cancellation.
                if (finished.Task.IsCanceled)
                {
                    _reporter.Fatal("Task was cancelled", null, now);
                    exc = new TaskCanceledException();
                }
                else if (exc != null)
                {
                    _reporter.Fatal(exc.Message, exc.StackTrace?.ToString(), now);
                }
                else if (finished.Task.IsFaulted)
                {
                    // Should be impossible.
                    _reporter.Fatal("Task failed without throwing an exception.", null, now);
                    exc = new CogniteUtilsException("Task failed without throwing an exception");
                }

                // Report that the task ended.
                _reporter.ReportEnd(
                    !finished.Task.IsFaulted && !finished.Task.IsCanceled
                    ? finished.Task.Result
                    : null,
                    now);

                // Wake up any waiters and tell them the task has finished running.
                foreach (var cb in _waiters)
                {
                    cb.TrySetResult(exc);
                }
                _waiters.Clear();

                // If the task is critical, then at this stage we should throw an exception.
                if (exc != null && Operation.ErrorIsFatal)
                {
                    ExceptionDispatchInfo.Capture(exc).Throw();
                }
            }
        }

        public void Dispose()
        {
            lock (_lock)
            {
                if (ActiveTask != null)
                {
                    var now = DateTime.UtcNow;
                    ActiveTask.Source.Cancel();
                    ActiveTask.Dispose();
                    ActiveTask = null;
                    _reporter.Fatal("Task was cancelled", null, now);
                    _reporter.ReportEnd(null, now);
                }
                foreach (var waiter in _waiters)
                {
                    waiter.TrySetCanceled();
                }
                _waiters.Clear();
            }
        }
    }

    /// <summary>
    /// Task scheduler that runs tasks and reports their status to the integrations API.
    /// </summary>
    public class ExtractorTaskScheduler : IDisposable
    {
        // The token source must be initialized when you call `Run`.
        private CancellationTokenSource? _source;
        private readonly IIntegrationSink _sink;
        private readonly Dictionary<string, RegisteredTask> _tasks = new Dictionary<string, RegisteredTask>();


        private object _lock = new object();
        private ManualResetEvent _evt = new ManualResetEvent(false);

        private TaskCompletionSource<bool> _runMethodClosed = new TaskCompletionSource<bool>();

        private bool disposedValue;

        /// <summary>
        /// Task that terminates once the run method terminates.
        /// </summary>
        public Task CompletedTask => _runMethodClosed.Task;

        private ILogger _logger;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="sink">Sink for task updates.</param>
        /// <param name="logger">Logger object.</param>
        public ExtractorTaskScheduler(IIntegrationSink sink, ILogger<ExtractorTaskScheduler> logger)
        {
            _sink = sink;
            _logger = logger;
        }

        /// <summary>
        /// Wake up the scheduler and tell it to check its tasks again.
        /// </summary>
        public void Notify()
        {
            _evt.Set();
        }

        /// <summary>
        /// Add a task to the scheduler.
        /// </summary>
        /// <param name="operation">Type implementing the runnable task.</param>
        /// <param name="runImmediately">Whether to run the task immediately.</param>
        /// <exception cref="InvalidOperationException">If a task with the same name already exists.</exception>
        public void AddScheduledTask(BaseSchedulableTask operation, bool runImmediately)
        {
            if (operation == null) throw new ArgumentNullException(nameof(operation));

            lock (_lock)
            {
                if (_tasks.ContainsKey(operation.Name))
                {
                    throw new InvalidOperationException($"Task {operation.Name} already exists");
                }

                operation.RegisterReadyCallback(Notify);
                var task = new RegisteredTask(operation, new TaskReporter(operation.Name, _sink), runImmediately);
                _tasks.Add(operation.Name, task);
                _evt.Set();
            }
        }

        /// <summary>
        /// Cancel a task if it is currently running.
        /// </summary>
        /// <param name="name">Name of the task to cancel</param>
        public void CancelTask(string name)
        {
            if (name == null) throw new ArgumentNullException(nameof(name));

            lock (_lock)
            {
                if (!_tasks.TryGetValue(name, out var task))
                {
                    throw new InvalidOperationException($"No task with name {name}");
                }
                task.Cancel();
            }
        }

        /// <summary>
        /// Schedule a task now. If it is already running it will 
        /// </summary>
        /// <param name="name">Name of the task to schedule</param>
        /// <param name="reScheduleIfRunning">If true, re-schedule the
        /// task to run again once it finishes. If false, and the task is already
        /// running, nothing will happen.</param>
        public void ScheduleTaskNow(string name, bool reScheduleIfRunning = false)
        {
            if (name == null) throw new ArgumentNullException(nameof(name));

            lock (_lock)
            {
                if (!_tasks.TryGetValue(name, out var task))
                {
                    throw new InvalidOperationException($"No task with name {name}");
                }
                if (reScheduleIfRunning || task.ActiveTask == null)
                {
                    task.NextRun = DateTime.UtcNow;
                    _evt.Set();
                }
            }
        }

        /// <summary>
        /// Wait for the next time the task given by <paramref name="task"/> ends.
        /// 
        /// If the task is not currently running, this will wait until it starts running and
        /// then ends.
        /// 
        /// Note that if the task fails, this will re-throw the exception that caused
        /// the task failure, or a TaskCanceledException if it was canceled.
        /// </summary>
        /// <param name="task">Task to wait for, must be added to the scheduler.</param>
        /// <param name="timeout">Timeout, set to Timeout.InfiniteTimespan to wait forever.</param>
        /// <param name="token">Optional cancellation token.</param>
        /// <exception cref="ArgumentException">If the task does not exist.</exception>
        /// <exception cref="TimeoutException">If waiting for the task to end timed out.</exception>
        public async Task WaitForNextEndOfTask(string task, TimeSpan timeout, CancellationToken token = default)
        {
            var evt = new TaskCompletionSource<Exception?>();
            lock (_lock)
            {
                if (!_tasks.TryGetValue(task, out var t))
                {
                    throw new ArgumentException($"Task with name {task} does not exist");
                }
                t.AddWaiter(evt);
            }

            var delay = Task.Delay(timeout, token);
            var r = await Task.WhenAny(evt.Task, delay).ConfigureAwait(false);
            token.ThrowIfCancellationRequested();
            if (r == delay)
            {
                throw new TimeoutException($"Waiting for end of task {task} timed out after {timeout}");
            }
            else
            {
                var exc = evt.Task.Result;
                if (exc != null)
                {
                    ExceptionDispatchInfo.Capture(exc).Throw();
                }
            }
        }

        private bool _started;



        /// <summary>
        /// Run the scheduler.
        /// 
        /// This should only be called once on a given scheduler.
        /// 
        /// To re-run it after a crash, the scheduler must be re-initialized.
        /// </summary>
        /// <param name="token">Global cancellation token for stopping the entire scheduler.</param>
        public async Task<ExtractorTaskResult> Run(CancellationToken token)
        {
            if (_started) throw new InvalidOperationException("Attempt to run scheduler multiple times");
            _started = true;

            try
            {
                await RunInner(token).ConfigureAwait(false);
                _runMethodClosed.TrySetResult(true);
            }
            catch (Exception ex)
            {
                if (token.IsCancellationRequested)
                {
                    _runMethodClosed.TrySetResult(true);
                }
                else
                {
                    _runMethodClosed.TrySetException(ex);
                }
            }

            bool isCancelled = _source?.Token.IsCancellationRequested ?? false;

            return isCancelled ? ExtractorTaskResult.Expected : ExtractorTaskResult.Unexpected;
        }


        private async Task RunInner(CancellationToken token)
        {
            _source = CancellationTokenSource.CreateLinkedTokenSource(token);
            // Waits for the outer token, not the internal source.
            // This way we can cancel the task scheduler first, without
            // canceling everything else.
            while (!_source.Token.IsCancellationRequested)
            {
                _evt.Reset();
                var tickTime = DateTime.UtcNow;
                var toAwait = new List<Task>();

                lock (_lock)
                {

                    DateTime? minNextRun = null;
                    var tasksToWaitFor = new List<Task>();
                    foreach (var task in _tasks.Values)
                    {
                        // If the task has finished, take steps to mark it as completed.
                        if (task.ActiveTask != null && task.ActiveTask.Task.IsCompleted)
                        {
                            _logger.LogDebug("Finish run of task {Name}", task.Operation.Name);
                            task.FinishTask(tickTime);
                        }

                        // Start the task again if it is now not running but it is scheduled to run.
                        if (task.NextRun.HasValue
                            && task.ActiveTask == null
                            && task.Operation.CanRunNow())
                        {
                            if (task.NextRun.Value <= tickTime)
                            {
                                _logger.LogDebug("Start new run of task {Name}", task.Operation.Name);
                                task.Run(tickTime, token);
                            }
                            else if (minNextRun == null || minNextRun > task.NextRun.Value)
                            {
                                minNextRun = task.NextRun.Value;
                            }
                        }

                        // If the task is now running, add it to the list of things we are going to listen to.
                        if (task.ActiveTask != null)
                        {
                            tasksToWaitFor.Add(task.ActiveTask.Task);
                        }
                    }

                    // If there is a task that is going to run in the future, add a task to wait for that time.
                    if (minNextRun != null)
                    {
#pragma warning disable CA2016 // Forward the 'CancellationToken' parameter to methods
                        toAwait.Add(Task.Delay(tickTime - minNextRun.Value));
#pragma warning restore CA2016 // Forward the 'CancellationToken' parameter to methods
                    }
                    if (tasksToWaitFor.Count > 0)
                    {
                        toAwait.Add(Task.WhenAny(tasksToWaitFor));
                    }
                }

                // Always wait for the event to trigger.
                toAwait.Add(CommonUtils.WaitAsync(_evt, Timeout.InfiniteTimeSpan, _source.Token));
                await Task.WhenAny(toAwait).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Cancel the running task, if it exists.
        /// </summary>
        public async Task CancelInnerAndWait(int timeoutms, BaseErrorReporter outerReporter)
        {
            if (!_started) return;
            _source?.Cancel();
            var waitTask = Task.Delay(timeoutms);
            var completed = await Task.WhenAny(waitTask, CompletedTask).ConfigureAwait(false);
            if (completed == waitTask)
            {
                _logger.LogWarning("Failed to shut down gracefully within timeout, continuing shutdown");
                outerReporter?.Warning("Failed to shut down gracefully within timeout, continuing shutdown");
            }

            if (completed.Exception != null)
            {
                outerReporter?.Fatal($"Failed to shut down gracefully: {completed.Exception.Message}", completed.Exception.StackTrace?.ToString());
            }
        }

        /// <summary>
        /// Dispose of the scheduler.
        /// 
        /// Can be overridden in base classes.
        /// </summary>
        /// <param name="disposing">Whether to dispose managed resources.</param>
        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    _source?.Cancel();
                    _source?.Dispose();
                    _evt.Dispose();
                    foreach (var task in _tasks)
                    {
                        task.Value.Dispose();
                    }
                }

                disposedValue = true;
            }
        }

        /// <inheritdoc />
        public void Dispose()
        {
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }
    }
}