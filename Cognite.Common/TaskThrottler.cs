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
        public Task Task { get; private set; }
        /// <summary>
        /// Triggered exception. Null if the task completed successfully or if it is not complete.
        /// </summary>
        public Exception Exception { get; private set; }
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
        internal TaskResult(DateTime startTime, int index)
        {
            StartTime = startTime;
            Index = index;
        }

        internal void ReportResult(Task task)
        {
            Task = task;
            Exception = task.Exception;
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
        // Default unederlying collection is a ConcurrentQueue
        private readonly BlockingCollection<Func<Task>> _generators = new BlockingCollection<Func<Task>>();

        /// <summary>
        /// Task for the main task loop in the throttler.
        /// </summary>
        public Task RunTask { get; }

        private readonly List<Task> _runningTasks = new List<Task>();

        private readonly List<TaskResult> _results = new List<TaskResult>();
        private readonly object _lock = new object();

        private readonly ManualResetEvent _taskCompletionEvent = new ManualResetEvent(false);
        private readonly bool _quitOnFailure;

        private int _taskIndex;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="maxParallelism">Maximum number of parallel threads</param>
        /// <param name="quitOnFailure">True if </param>
        /// <param name="perUnit"></param>
        /// <param name="timeUnit"></param>
        public TaskThrottler(int maxParallelism, bool quitOnFailure = false, int perUnit = 0, TimeSpan? timeUnit = null)
        {
            _maxParallelism = maxParallelism;
            _maxPerUnit = perUnit;
            _timeUnit = timeUnit == null ? TimeSpan.Zero : TimeSpan.FromTicks(timeUnit.Value.Ticks);
            _quitOnFailure = quitOnFailure;
            RunTask = Run();
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
            Task wrappedGenerator()
            {
                var taskResult = new TaskResult(DateTime.UtcNow, localIndex);
                var result = generator();
                _results.Add(taskResult);
                return result.ContinueWith(task =>
                {
                    taskResult.ReportResult(task);
                    _taskCompletionEvent.Set();
                }, TaskScheduler.Default);
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
                TaskResult localResult = null;
                int localIndex;
                lock (_lock)
                {
                    localIndex = _taskIndex++;
                }
                Task wrappedGenerator()
                {
                    localResult = new TaskResult(DateTime.UtcNow, localIndex);
                    var result = generator();
                    _results.Add(localResult);
                    return result.ContinueWith(task =>
                    {
                        localResult.ReportResult(task);
                        _taskCompletionEvent.Set();
                        localCompletionEvent.Set();
                    }, TaskScheduler.Default);
                }

                _generators.Add(wrappedGenerator);
                await ToTask(localCompletionEvent).ConfigureAwait(false);
                if (_quitOnFailure && localResult.Exception != null)
                {
                    throw new AggregateException("Failure in TaskThrottler", localResult.Exception);
                }
                return localResult;
            }
        }

        private bool AllowSchedule()
        {
            lock (_lock)
            {
                if (_maxParallelism > 0 && _runningTasks.Count >= _maxParallelism) return false;
                if (_timeUnit > TimeSpan.Zero && _maxPerUnit > 0)
                {
                    var now = DateTime.UtcNow;
                    var scheduledWithinLastTimeUnit = _results.Count(res => res.StartTime > now - _timeUnit);

                    if (scheduledWithinLastTimeUnit > _maxPerUnit) return false;
                }
            }
            return true;
        }

        private async Task Run()
        {
            bool running = true;
            while (running)
            {
                using (var source = new CancellationTokenSource())
                {
                    var _toWaitFor = new List<Task>();
                    if (AllowSchedule())
                    {
                        _toWaitFor.Add(Task.Run(() =>
                        {
                            try
                            {
                                var generator = _generators.Take(source.Token);
                                lock (_lock)
                                {
                                    _runningTasks.Add(generator());
                                }
                            }
                            catch (InvalidOperationException)
                            {
                                running = false;
                            }
                            catch (OperationCanceledException) { }
                        }));
                    }
                    else if (_timeUnit > TimeSpan.Zero)
                    {
                        _toWaitFor.Add(Task.Delay(_timeUnit, source.Token));
                    }
                    _toWaitFor.Add(ToTask(_taskCompletionEvent));

                    await Task.WhenAny(_toWaitFor).ConfigureAwait(false);

                    _taskCompletionEvent.Reset();
                    source.Cancel();
                }

                lock (_lock)
                {
                    if (_quitOnFailure && _runningTasks.Any(task => task.IsFaulted)) break;
                    if (_runningTasks.Any(task => task.IsCanceled)) break;
                }

                if (!running)
                {
                    await Task.WhenAll(_runningTasks).ConfigureAwait(false);
                }

                lock (_lock)
                {
                    var toRemove = _runningTasks.Where(task => task.IsCompleted || task.IsCanceled || task.IsFaulted).ToList();
                    foreach (var task in toRemove)
                    {
                        _runningTasks.Remove(task);
                    }
                }
            }
        }

        /// <summary>
        /// Check the result of the scheduler. Throw an error if the task scheduler has failed or
        /// if quitOnFailure is true and an error has occured in a task.
        /// </summary>
        public void CheckResult()
        {
            if (_quitOnFailure)
            {
                var failures = _results.Select(result => result.Exception).Where(exc => exc != null).ToList();
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
            _generators.Dispose();
        }
    }
}
