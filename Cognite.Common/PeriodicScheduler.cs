using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.ExceptionServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.Extractor.Common
{
    /// <summary>
    /// Interface for time span providers used in the periodic scheduler.
    /// Allows flexible scheduling.
    /// </summary>
    public interface ITimeSpanProvider
    {
        /// <summary>
        /// Next timespan value.
        /// </summary>
        TimeSpan Value { get; }
    }

    internal class BasicTimeSpanProvider : ITimeSpanProvider
    {
        public TimeSpan Value { get; }
        public BasicTimeSpanProvider(TimeSpan value)
        {
            Value = value;
        }
    }

    internal sealed class PeriodicTask : IDisposable
    {
        public Task Task { get; set; } = null!;
        public Func<CancellationToken, Task> Operation { get; }
        public ManualResetEvent Event { get; } = new ManualResetEvent(false);
        public ITimeSpanProvider Interval { get; }
        public bool Paused { get; set; }
        public bool ShouldRun { get; set; } = true;
        public string Name { get; }
        public PeriodicTask(Func<CancellationToken, Task> operation, ITimeSpanProvider interval, string name)
        {
            Operation = operation;
            Interval = interval;
            Name = name;
        }
        public void Dispose()
        {
            Event.Dispose();
        }
    }

    /// <summary>
    /// Utility to schedule and manage periodic tasks
    /// </summary>
    public class PeriodicScheduler : IDisposable
    {
        private CancellationTokenSource _source;
        private readonly Dictionary<string, PeriodicTask> _tasks = new Dictionary<string, PeriodicTask>();
        private bool disposedValue;
        private readonly ManualResetEvent _newTaskEvent = new ManualResetEvent(false);
        private readonly object _taskListMutex = new object();
        private readonly Task _internalLoopTask;

        /// <summary>
        /// Number of currently active tasks
        /// </summary>
        public int Count => _tasks.Count;

        private int _anonymousCounter;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="token">Cancellation token linked to all running tasks</param>
        public PeriodicScheduler(CancellationToken token)
        {
            _source = CancellationTokenSource.CreateLinkedTokenSource(token);
            _internalLoopTask = InternalPendingLoop();
        }

        /// <summary>
        /// Schedule a new periodic task to run with interval <paramref name="interval"/>.
        /// Exceptions are not caught, so <paramref name="operation"/> should catch its own errors, or the
        /// task from WaitForAll should be watched.
        /// </summary>
        /// <param name="name">Name of task, used to refer to it later</param>
        /// <param name="interval">Interval to schedule on</param>
        /// <param name="operation">Function to call on each iteration</param>
        /// <param name="runImmediately">True to execute the periodic task immediately, false to first
        /// wait until triggered by interval or manually</param>
        public void SchedulePeriodicTask(string? name, ITimeSpanProvider interval,
            Func<CancellationToken, Task> operation, bool runImmediately = true)
        {
            lock (_taskListMutex)
            {
                if (name == null) name = $"anonymous-periodic{_anonymousCounter++}";
                if (_tasks.ContainsKey(name)) throw new InvalidOperationException($"A task with name {name} already exists");
                var task = new PeriodicTask(operation, interval, name);
                _tasks[name] = task;
                task.Task = RunPeriodicTaskAsync(task, runImmediately);
                _newTaskEvent.Set();
            }
        }

        /// <summary>
        /// Schedule a new periodic task to run with interval <paramref name="interval"/>.
        /// Exceptions are not caught, so <paramref name="operation"/> should catch its own errors, or the
        /// task from WaitForAll should be watched.
        /// </summary>
        /// <param name="name">Name of task, used to refer to it later</param>
        /// <param name="interval">Interval to schedule on</param>
        /// <param name="operation">Function to call on each iteration</param>
        /// <param name="runImmediately">True to execute the periodic task immediately, false to first
        /// wait until triggered by interval or manually</param>
        public void SchedulePeriodicTask(string? name, TimeSpan interval,
            Func<CancellationToken, Task> operation, bool runImmediately = true)
        {
            SchedulePeriodicTask(name, new BasicTimeSpanProvider(interval), operation, runImmediately);
        }


        /// <summary>
        /// Returns true if a task with the given name exists
        /// </summary>
        /// <param name="name">Task to check</param>
        /// <returns>True if task identified by <paramref name="name"/> exists</returns>
        public bool ContainsTask(string name)
        {
            lock (_taskListMutex)
            {
                return _tasks.ContainsKey(name);
            }
        }

        /// <summary>
        /// Schedule a new periodic task to run with interval <paramref name="interval"/>.
        /// Exceptions are not caught, so <paramref name="operation"/> should catch its own errors, or the
        /// task from WaitForAll should be watched.
        /// </summary>
        /// <param name="name">Name of task, used to refer to it later</param>
        /// <param name="interval">Interval to schedule on</param>
        /// <param name="operation">Function to call on each iteration</param>
        /// <param name="runImmediately">True to execute the periodic task immediately, false to first
        /// wait until triggered by interval or manually</param>
        public void SchedulePeriodicTask(string? name, TimeSpan interval,
            Action<CancellationToken> operation, bool runImmediately = true)
        {
            SchedulePeriodicTask(name, interval, token => Task.Run(() => operation(token), CancellationToken.None), runImmediately);
        }

        /// <summary>
        /// Schedule a new periodic task to run with interval <paramref name="interval"/>.
        /// Exceptions are not caught, so <paramref name="operation"/> should catch its own errors, or the
        /// task from WaitForAll should be watched.
        /// </summary>
        /// <param name="name">Name of task, used to refer to it later</param>
        /// <param name="interval">Interval to schedule on</param>
        /// <param name="operation">Function to call on each iteration</param>
        /// <param name="runImmediately">True to execute the periodic task immediately, false to first
        /// wait until triggered by interval or manually</param>
        public void SchedulePeriodicTask(string? name, ITimeSpanProvider interval,
            Action<CancellationToken> operation, bool runImmediately = true)
        {
            SchedulePeriodicTask(name, interval, token => Task.Run(() => operation(token), CancellationToken.None), runImmediately);
        }

        /// <summary>
        /// Schedule a new task to run with on the scheduler.
        /// Exceptions are not caught, so <paramref name="operation"/> should catch its own errors, or the
        /// task from WaitForAll should be watched. Note that this method waits on <paramref name="operation"/> to yield,
        /// so make sure that it does not contain too much synchronous code.
        /// </summary>
        /// <param name="name"></param>
        /// <param name="operation"></param>
        public void ScheduleTask(string? name, Func<CancellationToken, Task> operation)
        {
            if (operation == null) throw new ArgumentNullException(nameof(operation));
            lock (_taskListMutex)
            {
                if (name == null) name = $"anonymous{_anonymousCounter++}";
                if (_tasks.ContainsKey(name)) throw new InvalidOperationException($"A task with name {name} already exists");
                var task = new PeriodicTask(operation, new BasicTimeSpanProvider(TimeSpan.Zero), name);
                _tasks[name] = task;
                task.Task = operation(_source.Token);
                _newTaskEvent.Set();
            }
        }

        /// <summary>
        /// Schedule a new task to run with on the scheduler.
        /// Exceptions are not caught, so <paramref name="operation"/> should catch its own errors, or the
        /// task from WaitForAll should be watched. Note that this method waits on <paramref name="operation"/> to yield,
        /// so make sure that it does not contain too much synchronous code.
        /// </summary>
        /// <param name="name"></param>
        /// <param name="operation"></param>
        public void ScheduleTask(string? name, Action<CancellationToken> operation)
        {
            ScheduleTask(name, token => Task.Run(() => operation(token), CancellationToken.None));
        }

        /// <summary>
        /// Signal a task should terminate then wait. This will not cancel the operation if it is running,
        /// but wait for it to terminate.
        /// Will throw an exception if the task has failed.
        /// </summary>
        /// <param name="name">Name of task to cancel</param>
        /// <returns>Task which completes once the task has terminated</returns>
        public Task ExitAndWaitForTermination(string name)
        {
            PeriodicTask task;
            lock (_taskListMutex)
            {
                if (!_tasks.TryGetValue(name, out task)) return Task.CompletedTask;
                task.ShouldRun = false;
                task.Event.Set();
            }
            return task.Task;
        }

        /// <summary>
        /// Waits for a task to terminate. If it is periodic this may never happen.
        /// </summary>
        /// <param name="name">Name of task to wait for</param>
        /// <returns></returns>
        public async Task WaitForTermination(string name)
        {
            PeriodicTask task;
            lock (_taskListMutex)
            {
                if (!_tasks.TryGetValue(name, out task)) return;
            }
            await task.Task.ConfigureAwait(false);
        }

        /// <summary>
        /// Same as calling ExitAndWaitForTermination on all tasks.
        /// </summary>
        /// <returns>Task which completes once all tasks are done</returns>
        public async Task ExitAllAndWait()
        {
            var tasks = new List<PeriodicTask>(_tasks.Count);
            lock (_taskListMutex)
            {
                foreach (var task in _tasks.Values)
                {
                    task.ShouldRun = false;
                    task.Event.Set();
                    tasks.Add(task);
                }
            }
            try
            {
                await Task.WhenAll(tasks.Select(tsk => tsk.Task)).ConfigureAwait(false);
            }
            catch (TaskCanceledException) { }
            catch (AggregateException aex)
            {
                if (aex.Flatten().InnerExceptions.First() is TaskCanceledException) return;
                throw aex.Flatten();
            }
            
        }

        /// <summary>
        /// Returns the internal task monitoring all running tasks.
        /// It will fail if any internal task fails.
        /// </summary>
        /// <returns></returns>
        public Task WaitForAll()
        {
            return _internalLoopTask;
        }

        /// <summary>
        /// Cleans up terminated tasks and watches for errors
        /// </summary>
        /// <returns></returns>
        private async Task InternalPendingLoop()
        {
            var tasks = new List<Task>();
            PeriodicTask? failedTask = null;

            var waitTask = CommonUtils.WaitAsync(_newTaskEvent, Timeout.InfiniteTimeSpan, _source.Token);
            tasks.Add(waitTask);

            while (!_source.IsCancellationRequested)
            {
                await Task.WhenAny(tasks).ConfigureAwait(false);

                lock (_taskListMutex)
                {
                    failedTask = _tasks.Values.FirstOrDefault(kvp => kvp.Task.IsFaulted);

                    if (failedTask != null) break;
                    if (_source.IsCancellationRequested) break;

                    var toRemove = _tasks.Values.Where(task => task.Task.IsCompleted).ToList();
                    foreach (var task in toRemove)
                    {
                        _tasks.Remove(task.Name);
                    }
                    tasks.Clear();
                    foreach (var task in _tasks.Values)
                    {
                        tasks.Add(task.Task);
                    }
                    if (_newTaskEvent.WaitOne(0))
                    {
                        _newTaskEvent.Reset();
                        waitTask = CommonUtils.WaitAsync(_newTaskEvent, Timeout.InfiniteTimeSpan, _source.Token);
                    }
                    tasks.Add(waitTask);
                }
            }
            if (_source.IsCancellationRequested) return;
            if (failedTask != null) ExceptionDispatchInfo.Capture(failedTask.Task.Exception).Throw();
        }

        /// <summary>
        /// Set the paused state of the named task. In this state it will only trigger
        /// when manually triggered. Same as setting the timespan to infinite when creating the task.
        /// The task will trigger when unpaused.
        /// </summary>
        /// <param name="name">Name of task to pause</param>
        /// <param name="paused">True to pause the task, false to unpause</param>
        /// <returns>True if the task was paused</returns>
        public bool TryPauseTask(string name, bool paused)
        {
            lock (_taskListMutex)
            {
                if (!_tasks.TryGetValue(name, out var task)) return false;
                if (task.Paused && !paused)
                {
                    task.Paused = paused;
                    task.Event.Set();
                }
                task.Paused = paused;
                return true;
            }
        }
        /// <summary>
        /// Manually trigger a task. The task will always run after this, but may run twice if it is about to run due to timeout.
        /// Either way the task will always run after this.  
        /// </summary>
        /// <param name="name"></param>
        /// <returns>True if the task was triggered</returns>
        public bool TryTriggerTask(string name)
        {
            lock (_taskListMutex)
            {
                if (!_tasks.TryGetValue(name, out var task)) return false;
                task.Event.Set();
                return true;
            }
        }

        private async Task RunPeriodicTaskAsync(PeriodicTask task, bool runImmediately)
        {
            bool shouldRunNow = runImmediately;
            while (!_source.IsCancellationRequested && task.ShouldRun)
            {
                var interval = task.Interval.Value;
                var timeout = task.Paused ? Timeout.InfiniteTimeSpan : interval;
                var waitTask = CommonUtils.WaitAsync(task.Event, interval, _source.Token).ConfigureAwait(false);
                if (!task.Paused && shouldRunNow) await task.Operation(_source.Token).ConfigureAwait(false);
                shouldRunNow = true;
                await waitTask;
                task.Event.Reset();
            }
        }

        /// <summary>
        /// Dispose of scheduler. Will cancel all running tasks.
        /// Override this in subclasses.
        /// </summary>
        /// <param name="disposing">True to dispose</param>
        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    _source.Cancel();
                    _source.Dispose();
                    foreach (var kvp in _tasks) kvp.Value.Dispose();
                    _tasks.Clear();
                }

                disposedValue = true;
            }
        }

        /// <summary>
        /// Dispose of scheduler. Will cancel all running tasks.
        /// Do not override this in subclasses.
        /// </summary>
        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }
    }
}
