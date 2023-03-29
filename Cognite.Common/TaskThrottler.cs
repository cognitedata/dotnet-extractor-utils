using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.Extractor.Common
{
    /// <summary>
    /// Result of an executed task
    /// </summary>
    public class TaskResult
    {
        /// <summary>
        /// Executed task. Null if not completed.
        /// </summary>
        public Task? Task { get; }
        /// <summary>
        /// Triggered exception. Null if the task completed successfully or if it is not complete.
        /// </summary>
        public Exception? Exception { get; set; }
        /// <summary>
        /// Time of completion. Null if the task has not completed.
        /// </summary>
        public DateTime? CompletionTime { get; private set; }
        /// <summary>
        /// Time the task was scheduled.
        /// </summary>
        public DateTime StartTime { get; }
        /// <summary>
        /// Index of the originally scheduled task.
        /// </summary>
        public int Index { get; }
        /// <summary>
        /// True if the task has completed.
        /// </summary>
        public bool IsCompleted => CompletionTime.HasValue;
        internal TaskResult(DateTime startTime, int index, Task? task)
        {
            Task = task;
            StartTime = startTime;
            Index = index;
        }

        internal void ReportResult()
        {
            Exception = Task!.Exception;
            CompletionTime = DateTime.UtcNow;
        }

        internal void ReportResult(Exception ex)
        {
            Exception = ex;
            CompletionTime = DateTime.UtcNow;
        }
    }
    /// <summary>
    /// Tool to throttle the execution of tasks based on max perallelism, and max number of tasks
    /// scheduled per time unit.
    /// 
    /// Maximum parallelism simply limits the number of parallel tasks.
    /// 
    /// Per unit sets the maximum number of tasks scheduled per time unit.
    ///
    /// Tasks are enqueued and scheduled for execution in order.
    /// </summary>
    public sealed class TaskThrottler : IDisposable
    {
        private readonly int _maxParallelism;
        private readonly int _maxPerUnit;
        private readonly TimeSpan _timeUnit;
        private readonly TimeSpan _timeUnitChunk;
        // Default unederlying collection is a ConcurrentQueue
        private readonly BlockingCollection<Func<TaskResult>> _generators
            = new BlockingCollection<Func<TaskResult>>();

        private readonly CancellationTokenSource _source = new CancellationTokenSource();
        private readonly CancellationTokenSource _completionSource;
        /// <summary>
        /// Task for the main task loop in the throttler.
        /// </summary>
        public Task RunTask { get; }

        private readonly List<TaskResult> _runningTasks = new List<TaskResult>();

        private readonly List<TaskResult> _results = new List<TaskResult>();
        private readonly object _lock = new object();

        private readonly ManualResetEvent _taskCompletionEvent = new ManualResetEvent(false);
        private readonly bool _quitOnFailure;
        private readonly bool _keepAllResults;

        private int _taskIndex;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="maxParallelism">Maximum number of parallel threads</param>
        /// <param name="quitOnFailure">True if </param>
        /// <param name="perUnit"></param>
        /// <param name="timeUnit"></param>
        /// <param name="keepAllResults">Keep all task result objects, not those who have failed or are within the
        /// last <paramref name="timeUnit"/>. This means that the size in memory of the task throttler will grow forever,
        /// do not use this unless you intend to dispose of the throttler within a short period of time.</param>
        public TaskThrottler(int maxParallelism,
            bool quitOnFailure = false,
            int perUnit = 0,
            TimeSpan? timeUnit = null,
            bool keepAllResults = false)
        {
            _maxParallelism = maxParallelism;
            _maxPerUnit = perUnit;
            if (timeUnit != null && perUnit > 0)
            {
                _timeUnit = TimeSpan.FromTicks(timeUnit.Value.Ticks);
                _timeUnitChunk = TimeSpan.FromTicks(timeUnit.Value.Ticks / perUnit);
            }
            else
            {
                _timeUnit = TimeSpan.Zero;
                _timeUnitChunk = TimeSpan.Zero;
            }

            _quitOnFailure = quitOnFailure;
            _completionSource = CancellationTokenSource.CreateLinkedTokenSource(_source.Token);
            _keepAllResults = keepAllResults;
            RunTask = Task.Run(async () => await Run().ConfigureAwait(false));
        }

        private static Task ToTask(WaitHandle waitHandle)
        {
            if (waitHandle == null) throw new ArgumentNullException(nameof(waitHandle));

            var tcs = new TaskCompletionSource<bool>();
            var rwh = ThreadPool.RegisterWaitForSingleObject(waitHandle,
                delegate { tcs.TrySetResult(true); }, null, -1, true);
            var t = tcs.Task;
            t.ContinueWith((antecedent) => rwh.Unregister(null), TaskScheduler.Default);
            return t;
        }

        /// <summary>
        /// Enqueue a new generator in the task queue to be executed
        /// </summary>
        /// <param name="generator"></param>
        public void EnqueueTask(Func<Task> generator)
        {
            int localIndex;
            lock (_lock)
            {
                localIndex = _taskIndex++;
            }
            TaskResult wrappedGenerator()
            {
                Task? result = null;
                TaskResult taskResult;
                try
                {
                    result = generator();
                    taskResult = new TaskResult(DateTime.UtcNow, localIndex, result);
                }
                catch (Exception ex)
                {
                    taskResult = new TaskResult(DateTime.UtcNow, localIndex, null);
                    taskResult.ReportResult(ex);
                }

                lock (_lock)
                {
                    _results.Add(taskResult);
                }

                if (result == null)
                {
                    _taskCompletionEvent.Set();
                    return taskResult;
                }

                result.ContinueWith(task =>
                {
                    taskResult.ReportResult();
                    _taskCompletionEvent.Set();
                }, TaskScheduler.Default);
                return taskResult;
            }

            _generators.Add(wrappedGenerator);
        }

        /// <summary>
        /// Enqueue a task, then wait until it completes before returning its result
        /// </summary>
        /// <param name="generator">Task to enqueue</param>
        /// <returns>Result of task</returns>
        public async Task<TaskResult> EnqueueAndWait(Func<Task> generator)
        {
            using (var localCompletionEvent = new ManualResetEvent(false))
            {
                TaskResult? localResult = null;
                int localIndex;
                lock (_lock)
                {
                    localIndex = _taskIndex++;
                }
                TaskResult wrappedGenerator()
                {
                    Task? result = null;
                    try
                    {
                        result = generator();
                        localResult = new TaskResult(DateTime.UtcNow, localIndex, result);
                    }
                    catch (Exception ex)
                    {
                        localResult = new TaskResult(DateTime.UtcNow, localIndex, null);
                        localResult.ReportResult(ex);
                    }
                    lock (_lock)
                    {
                        _results.Add(localResult);
                    }

                    if (result == null)
                    {
                        _taskCompletionEvent.Set();
                        localCompletionEvent.Set();
                        return localResult;
                    }
                    result.ContinueWith(task =>
                    {
                        localResult.ReportResult();
                        _taskCompletionEvent.Set();
                        localCompletionEvent.Set();
                    }, TaskScheduler.Default);
                    return localResult;
                }

                _generators.Add(wrappedGenerator);
                await ToTask(localCompletionEvent).ConfigureAwait(false);
                if (_quitOnFailure && localResult!.Exception != null)
                {
                    throw new AggregateException("Failure in TaskThrottler", localResult.Exception);
                }
                return localResult!;
            }
        }

        private bool AllowSchedule()
        {
            lock (_lock)
            {
                if (_maxParallelism > 0 && _runningTasks.Count >= _maxParallelism) return false;
                if (_timeUnit > TimeSpan.Zero)
                {
                    var now = DateTime.UtcNow;
                    var scheduledInLastTimePeriod = _results.Count(res => res.StartTime > now - _timeUnit);
                    var totalInLastChunk = _results.Count(res => res.StartTime > now - _timeUnitChunk);

                    // Allow 2 in each "chunk" of time, as flex. This helps distribute tasks a bit more over time.
                    if (totalInLastChunk > 1 || scheduledInLastTimePeriod >= _maxPerUnit) return false;
                }
                
            }
            return true;
        }

        private DateTime _lastWaitTime;

        private async Task Run()
        {
            while (!_source.IsCancellationRequested && !_generators.IsCompleted)
            {
                // To allow canceling once, so that subsequent calls to _generators.Take() will fail if the collection is empty
                // Setting BlockingCollection to complete does not terminate active calls to Take().
                // Canceling _source indicates that we are killing the entire scheduler, which is different.
                var token = _completionSource.IsCancellationRequested ? _source.Token : _completionSource.Token;
                if (AllowSchedule())
                {
                    try
                    {
                        var generator = _generators.Take(token);
                        lock (_lock)
                        {
                            _runningTasks.Add(generator());
                        }
                    }
                    catch (InvalidOperationException)
                    {
                    }
                    catch (OperationCanceledException)
                    {
                    }
                }
                else
                {
                    if (_timeUnit > TimeSpan.Zero)
                    {
                        // If waiting is interrupted, we want to continue waiting, instead of restarting,
                        // this helps ensure even distribution of tasks. I.e. if we run tasks that takes 200ms,
                        // and use a time unit of 500ms, then we will wake up after 200ms, not schedule a new task,
                        // wait another 500ms, then wake up again, so we effectively only wake up every 700ms,
                        // This wake-up period should be a minimum value.
                        TimeSpan waitTime = _timeUnitChunk;
                        var now = DateTime.UtcNow;
                        if (_lastWaitTime + _timeUnitChunk > now)
                        {
                            waitTime = _timeUnitChunk - (now - _lastWaitTime);
                        }

                        if (_lastWaitTime == DateTime.MinValue) _lastWaitTime = DateTime.UtcNow;
                        try
                        {
                            bool timedOut = await CommonUtils
                                .WaitAsync(_taskCompletionEvent, waitTime, token)
                                .ConfigureAwait(false);
                            if (timedOut) _lastWaitTime = DateTime.MinValue;
                        }
                        catch (TaskCanceledException)
                        {
                        }
                    }
                    else
                    {
                        try
                        {
                            await CommonUtils
                                .WaitAsync(_taskCompletionEvent, Timeout.InfiniteTimeSpan, token)
                                .ConfigureAwait(false);
                        }
                        catch (TaskCanceledException)
                        {
                        }
                        
                    }
                    _taskCompletionEvent.Reset();
                }

                lock (_lock)
                {
                    if (_quitOnFailure && _runningTasks.Any(result => result.Task != null && result.Task.IsFaulted)) break;
                }

                lock (_lock)
                {
                    var now = DateTime.UtcNow;
                    // Remove all results we are no longer interested in.
                    if (!_keepAllResults)
                    {
                        _results.RemoveAll(result =>
                            (result.Exception == null || !_quitOnFailure)
                            && (_timeUnit == TimeSpan.Zero || _maxPerUnit <= 0 || result.StartTime <= now - _timeUnit));
                    }
                    
                    _runningTasks.RemoveAll(result =>
                        result.Task == null
                        || result.Task.IsCompleted
                        || result.Task.IsCanceled
                        || result.Task.IsFaulted);
                }
            }

            await Task.WhenAll(
                _runningTasks.Select(result => result.Task)
                .Where(task => task != null && !task.IsCompleted)!).ConfigureAwait(false);
        }

        /// <summary>
        /// Check the result of the scheduler. Throw an error if the task scheduler has failed or
        /// if quitOnFailure is true and an error has occured in a task.
        /// </summary>
        public void CheckResult()
        {
            if (_quitOnFailure)
            {
                var failures = _results.Select(result => result.Exception!).Where(exc => exc != null).ToList();
                if (failures.Any())
                {
                    if (RunTask.Exception != null)
                    {
                        failures.Add(RunTask.Exception);
                    }
                    throw new AggregateException("Failure in TaskThrottler", failures);
                }
            }
            if (RunTask.Exception != null)
            {
                throw new AggregateException("Failure in TaskThrottler", RunTask.Exception);
            }
        }

        /// <summary>
        /// Lock the task queue and wait for all remaining tasks to finish.
        /// Returns a list of <see cref="TaskResult"/> containing information about all scheduled tasks.
        /// If any task has failed and quitOnFailure is true, this will throw an exception.
        /// </summary>
        /// <returns>A list of <see cref="TaskResult"/></returns>
        /// <throws><see cref="AggregateException"/></throws>
        public async Task<IEnumerable<TaskResult>> WaitForCompletion()
        {
            _generators.CompleteAdding();
            _completionSource.Cancel();
            await RunTask.ConfigureAwait(false);
            CheckResult();
            return _results;
        }

        /// <summary>
        /// Dispose of the scheduler. Does not wait for completion and so might leave loose threads.
        /// </summary>
        public void Dispose()
        {
            _taskCompletionEvent.Dispose();
            _generators.CompleteAdding();
            _generators.Dispose();
            _source.Cancel();
            _source.Dispose();
            _completionSource.Dispose();
        }
    }
}
