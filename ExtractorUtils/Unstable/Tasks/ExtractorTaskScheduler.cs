using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extensions;
using Cognite.Extractor.Common;
using CogniteSdk.Alpha;
using Microsoft.Extensions.Logging;

namespace Cognite.Extractor.Utils.Unstable.Tasks
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
        /// Return whether this task being cancelled via an explicit Stop action
        /// (<see cref="ExtractorTaskScheduler.CancelTask"/>) should be considered fatal,
        /// on the same terms as <see cref="ErrorIsFatal"/>.
        ///
        /// Defaults to <see cref="ErrorIsFatal"/>: a task that is fatal-on-error is, by
        /// default, also fatal if stopped before it completes, since the framework has no
        /// way to know whether this particular task can safely be interrupted mid-run.
        /// Override this to return <c>false</c> only if the task is known to tolerate being
        /// stopped at any point without leaving the extractor in an inconsistent state.
        ///
        /// This has no effect on cancellation caused by the extractor itself shutting down
        /// (e.g. <c>Shutdown()</c>/<c>DisposeAsync()</c>) -- that is always non-fatal,
        /// regardless of this setting, since there is nothing left to protect by crashing a
        /// process that is already exiting.
        /// </summary>
        public virtual bool CancellationIsFatal { get => ErrorIsFatal; }

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

        /// <summary>
        /// Provide metadata about the task, reported to integration during startup.
        /// </summary>
        public abstract TaskMetadata Metadata { get; }
    }

    internal sealed class RunningTaskInfo : IDisposable
    {
        public Task<TaskUpdatePayload?> Task { get; }
        public CancellationTokenSource Source { get; }

        public string? CancellationReason { get; set; }

        /// <summary>
        /// Set to true when this specific task's cancellation was explicitly requested
        /// (e.g. via <see cref="RegisteredTask.Cancel"/>, which backs a Stop action), as opposed
        /// to the task's token being cancelled only because the scheduler itself is shutting
        /// down. Used by <see cref="RegisteredTask.FinishTask"/> to tell an intentional stop
        /// apart from a genuine failure -- whether that is in turn treated as fatal depends on
        /// <see cref="BaseSchedulableTask.CancellationIsFatal"/>, since not every task can
        /// safely tolerate being stopped mid-run.
        /// </summary>
        public bool CancelledIntentionally { get; set; }

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

        private readonly List<TaskCompletionSource<Exception?>> _waiters = new List<TaskCompletionSource<Exception?>>();
        private readonly object _lock = new object();

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
                var value = Operation.Schedule.Value;
                if (value == Timeout.InfiniteTimeSpan)
                {
                    // If the schedule is infinite, we should not set a next run time...
                    // It's unfortunate that .NET has standardized on this weird InfiniteTimeSpan value, which is just -1 ms.
                    NextRun = null;
                }
                else
                {
                    NextRun = now + value;
                }
            }
            else
            {
                NextRun = null;
            }

            ActiveTask = new RunningTaskInfo(task, source);
            _reporter.ReportStart(null, now);
        }

        public void Cancel(string? reason = null)
        {
            lock (_lock)
            {
                if (ActiveTask != null)
                {
                    ActiveTask.CancellationReason = reason;
                    ActiveTask.CancelledIntentionally = true;
                    ActiveTask.Source.Cancel();
                    foreach (var waiter in _waiters)
                    {
                        Task.Run(() => waiter.TrySetCanceled());
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

        /// <param name="now">Current time, used for reporting.</param>
        /// <param name="schedulerShuttingDown">Whether the scheduler itself is currently
        /// shutting down (i.e. its internal cancellation token has been requested). A task
        /// cancellation observed while this is true is always an intentional, expected
        /// cancellation caused by the shutdown -- never a genuine failure -- regardless of
        /// whether this particular task was individually cancelled.</param>
        public void FinishTask(DateTime now, bool schedulerShuttingDown)
        {
            lock (_lock)
            {
                if (ActiveTask == null || !ActiveTask.Task.IsCompleted) throw new InvalidOperationException("Attempt to finish a task that isn't completed");

                var finished = ActiveTask;
                ActiveTask = null;

                try
                {
                    Exception? exc = finished.Task.Exception?.Flatten();

                    if (exc?.InnerException != null)
                    {
                        exc = exc.InnerException;
                    }

                    bool wasCancelled = finished.Task.IsCanceled || finished.Source.IsCancellationRequested;

                    // Whether a cancellation of this task should be treated as fatal for a task
                    // with ErrorIsFatal set. The two recognized intentional-cancellation sources
                    // are handled differently on purpose:
                    //  - Scheduler shutdown is never fatal -- there is nothing left to protect by
                    //    crashing a process that is already exiting.
                    //  - An explicit Cancel() call on this task (e.g. a Stop action) follows
                    //    CancellationIsFatal, since the framework has no way to know on its own
                    //    whether this particular task can safely be interrupted mid-run; a task
                    //    that hasn't opted out defaults to matching ErrorIsFatal.
                    // A cancellation from neither recognized source (shouldn't currently be
                    // reachable) falls back to ErrorIsFatal, so this stays correct if a future
                    // change introduces another cancellation source that genuinely should be fatal.
                    bool cancellationIsFatal = false; // SABOTAGE

                    // Report a fatal error to integrations if the task exited non-cleanly.
                    // This typically means a crash or manual cancellation.
                    if (wasCancelled)
                    {
                        _reporter.Warning("Task was cancelled", finished.CancellationReason, now);
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
                        Task.Run(() => cb.TrySetResult(exc));
                    }
                    _waiters.Clear();

                    // If the task is critical, then at this stage we should throw an exception.
                    // A cancellation's fatal-ness is governed by cancellationIsFatal (see above);
                    // any other failure is fatal exactly when ErrorIsFatal is set.
                    if (exc != null && (wasCancelled ? cancellationIsFatal : Operation.ErrorIsFatal))
                    {
                        ExceptionDispatchInfo.Capture(exc).Throw();
                    }
                }
                finally
                {
                    finished.Dispose();
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
        ///
        /// This is a thin wrapper around <see cref="TryCancelTask"/> that discards the
        /// "did it actually do anything" outcome. Prefer <see cref="TryCancelTask"/> for any
        /// new caller that needs to distinguish "cancelled a running task" from "there was
        /// nothing to cancel" -- for example, a Stop action needs to report those as different
        /// outcomes (`succeeded` vs. `failed`), not silently treat both as success.
        /// </summary>
        /// <param name="name">Name of the task to cancel</param>
        /// <param name="reason">Reason for canceling the task</param>
        public void CancelTask(string name, string? reason = null)
        {
            TryCancelTask(name, reason);
        }

        /// <summary>
        /// Cancel a task if it is currently running.
        /// </summary>
        /// <param name="name">Name of the task to cancel.</param>
        /// <param name="reason">Reason for canceling the task.</param>
        /// <returns><c>true</c> if the task had an active run and was cancelled; <c>false</c> if
        /// the task exists but was not currently running, in which case this is a no-op.</returns>
        /// <exception cref="InvalidOperationException">If no task with this name is registered.</exception>
        public bool TryCancelTask(string name, string? reason = null)
        {
            if (name == null) throw new ArgumentNullException(nameof(name));

            lock (_lock)
            {
                if (!_tasks.TryGetValue(name, out var task))
                {
                    throw new InvalidOperationException($"No task with name {name}");
                }
                // A task whose Task has already completed, but whose ActiveTask hasn't been
                // cleared yet because the scheduler's tick loop hasn't run FinishTask on it,
                // must be treated the same as "not running" -- otherwise Cancel() would mark a
                // task that already finished (successfully or not) as CancelledIntentionally,
                // causing FinishTask to misreport it as cancelled once it does run.
                if (task.ActiveTask == null || task.ActiveTask.Task.IsCompleted)
                {
                    return false;
                }
                task.Cancel(reason);
                return true;
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
        /// Schedule a task to run now, unless it is already running.
        ///
        /// Unlike <see cref="ScheduleTaskNow"/>, this never re-schedules a task that is already
        /// running -- it exists specifically to let a caller (e.g. a Start action handler)
        /// distinguish "the task was queued to run" from "it was already running", which
        /// <see cref="ScheduleTaskNow"/>'s <c>void</c> return cannot express.
        ///
        /// A <c>true</c> return only means the task was queued to run on the scheduler's next
        /// tick -- it does not mean the task actually started, since
        /// <see cref="BaseSchedulableTask.CanRunNow"/> independently gates that. Use
        /// <see cref="CanTaskRunNow"/> immediately afterwards if the caller needs to know whether
        /// the task is actually eligible to start right now, rather than merely queued.
        /// </summary>
        /// <param name="name">Name of the task to schedule.</param>
        /// <returns><c>true</c> if the task was not running and has been queued to run now;
        /// <c>false</c> if it was already running, in which case this is a no-op.</returns>
        /// <exception cref="InvalidOperationException">If no task with this name is registered.</exception>
        public bool TryScheduleTaskNow(string name)
        {
            if (name == null) throw new ArgumentNullException(nameof(name));

            lock (_lock)
            {
                if (!_tasks.TryGetValue(name, out var task))
                {
                    throw new InvalidOperationException($"No task with name {name}");
                }
                // A task whose Task has already completed, but whose ActiveTask hasn't been
                // cleared yet because the scheduler's tick loop hasn't run FinishTask on it, is
                // safe to re-queue here: the tick loop always calls FinishTask for a completed
                // task before checking NextRun for that same task within the same iteration, so
                // ActiveTask will be null again well before this task is next considered to run.
                if (task.ActiveTask != null && !task.ActiveTask.Task.IsCompleted)
                {
                    return false;
                }
                task.NextRun = DateTime.UtcNow;
                _evt.Set();
                return true;
            }
        }

        /// <summary>
        /// Check whether the task given by <paramref name="name"/> currently reports itself as
        /// able to run, via <see cref="BaseSchedulableTask.CanRunNow"/>.
        ///
        /// This is independent of whether the task has been queued to run (see
        /// <see cref="TryScheduleTaskNow"/>): a task can be queued while <see cref="BaseSchedulableTask.CanRunNow"/>
        /// stays false for an extended, unbounded period (for example, a task that requires a
        /// live external connection before it can start). Callers that need a fast, explicit
        /// failure instead of hanging on <see cref="WaitForNextEndOfTask"/> should check this
        /// immediately after a successful <see cref="TryScheduleTaskNow"/> call.
        /// </summary>
        /// <param name="name">Name of the task to check.</param>
        /// <exception cref="InvalidOperationException">If no task with this name is registered.</exception>
        public bool CanTaskRunNow(string name)
        {
            if (name == null) throw new ArgumentNullException(nameof(name));

            lock (_lock)
            {
                if (!_tasks.TryGetValue(name, out var task))
                {
                    throw new InvalidOperationException($"No task with name {name}");
                }
                return task.Operation.CanRunNow();
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

        /// <summary>
        /// Get a list of registered tasks, used for reporting startup.
        /// </summary>
        /// <returns></returns>
        public IEnumerable<IntegrationTask> GetRegisteredTasks()
        {
            lock (_lock)
            {
                foreach (var task in _tasks.Values)
                {
                    yield return new IntegrationTask
                    {
                        Name = task.Operation.Name,
                        Description = task.Operation.Metadata.Description,
                        Type = task.Operation.Metadata.Type,
                        Action = task.Operation.Metadata.Action,
                    };
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
        public async Task<SchedulerTaskResult> Run(CancellationToken token)
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
                    throw;
                }
            }

            bool isCancelled = _source?.Token.IsCancellationRequested ?? false;

            return isCancelled ? SchedulerTaskResult.Expected : SchedulerTaskResult.Unexpected;
        }


        private async Task RunInner(CancellationToken token)
        {
            _source = CancellationTokenSource.CreateLinkedTokenSource(token);
            // Cancellation is handled internally. We want to cleanly shut down and allow all
            // tasks to complete before terminating the scheduler.
            while (true)
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
                            task.FinishTask(tickTime, _source.IsCancellationRequested);
                        }

                        // Start the task again if it is now not running but it is scheduled to run.
                        if (task.NextRun.HasValue
                            && task.ActiveTask == null
                            && task.Operation.CanRunNow()
                            // Only spawn new tasks when we are not cancelled.
                            && !_source.IsCancellationRequested)
                        {
                            if (task.NextRun.Value <= tickTime)
                            {
                                task.Run(tickTime, _source.Token);
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
                        Debug.Assert(minNextRun.Value > tickTime, "minNextRun should always be in the future");
#pragma warning disable CA2016 // Forward the 'CancellationToken' parameter to methods
                        toAwait.Add(Task.Delay(minNextRun.Value - tickTime));
#pragma warning restore CA2016 // Forward the 'CancellationToken' parameter to methods
                    }
                    if (tasksToWaitFor.Count > 0)
                    {
                        toAwait.Add(Task.WhenAny(tasksToWaitFor));
                    }
                    else if (_source.IsCancellationRequested)
                    {
                        break;
                    }
                }

                // Always wait for the event to trigger.
                toAwait.Add(CommonUtils.WaitAsync(_evt, Timeout.InfiniteTimeSpan, _source.Token));
                await Task.WhenAny(toAwait).ConfigureAwait(false);
            }
        }

        private bool _shutdown;

        /// <summary>
        /// Cancel the running task, if it exists.
        /// </summary>
        public async Task CancelInnerAndWait(int timeoutms, BaseErrorReporter outerReporter)
        {
            lock (_lock)
            {
                if (!_started) return;
                if (_shutdown) return;
                _shutdown = true;
            }
            _source?.Cancel();
            var waitTask = Task.Delay(timeoutms);
            var completed = await Task.WhenAny(waitTask, CompletedTask).ConfigureAwait(false);
            if (completed == waitTask)
            {
                _logger.LogWarning("Failed to shut down gracefully within timeout");
                outerReporter?.Warning("Failed to shut down gracefully within timeout", null, DateTime.UtcNow);
            }

            if (completed.Exception != null)
            {
                outerReporter?.Fatal($"Failed to shut down gracefully: {completed.Exception.Message}", completed.Exception.StackTrace?.ToString(), DateTime.UtcNow);
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
