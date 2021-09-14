using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.Extractor.Common
{
    internal sealed class PeriodicTask : IDisposable
    {
        public Task Task { get; set; }
        public Func<CancellationToken, Task> Operation { get; }
        public ManualResetEvent Event { get; } = new ManualResetEvent(false);
        public TimeSpan Interval { get; }
        public bool Paused { get; set; }
        public bool ShouldRun { get; set; } = true;
        public PeriodicTask(Func<CancellationToken, Task> operation, TimeSpan interval)
        {
            Operation = operation;
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
        private Dictionary<string, PeriodicTask> _tasks = new Dictionary<string, PeriodicTask>();
        private bool disposedValue;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="token">Cancellation token linked to all running tasks</param>
        public PeriodicScheduler(CancellationToken token)
        {
            _source = CancellationTokenSource.CreateLinkedTokenSource(token);
        }

        /// <summary>
        /// Schedule a new periodic task to run with interval <paramref name="interval"/>.
        /// Exceptions are not caught, so <paramref name="operation"/> should catch its own errors.
        /// </summary>
        /// <param name="name">Name of task, used to refer to it later</param>
        /// <param name="interval">Interval to schedule on</param>
        /// <param name="operation">Function to call on each iteration</param>
        public void SchedulePeriodicTask(string name, TimeSpan interval, Func<CancellationToken, Task> operation)
        {
            if (_tasks.ContainsKey(name)) throw new InvalidOperationException($"A task with name {name} already exists");
            var task = new PeriodicTask(operation, interval);
            task.Task = RunPeriodicTask(task);
            _tasks[name] = task;
        }

        /// <summary>
        /// Signal a task should terminate then wait. This will not cancel the operation if it is running,
        /// but wait for it to terminate.
        /// Will throw an exception if the task has failed.
        /// </summary>
        /// <param name="name">Name of task to cancel</param>
        /// <returns>Task which completes once the task has terminated</returns>
        public async Task ExitAndWaitForTermination(string name)
        {
            if (!_tasks.TryGetValue(name, out var task)) throw new InvalidOperationException($"No such task: {name}");
            task.ShouldRun = false;
            task.Event.Set();
            await task.Task.ConfigureAwait(false);
            _tasks.Remove(name);
        }

        /// <summary>
        /// Same as calling ExitAndWaitForTermination on all tasks.
        /// </summary>
        /// <returns>Task which completes once all tasks are done</returns>
        public async Task ExitAllAndWait()
        {
            var tasks = new List<Task>(_tasks.Count);
            foreach (var task in _tasks.Values)
            {
                task.ShouldRun = false;
                task.Event.Set();
                tasks.Add(task.Task);
            }
            await Task.WhenAll(tasks).ConfigureAwait(false);
            _tasks.Clear();
        }

        /// <summary>
        /// Set the paused state of the named task. In this state it will only trigger
        /// when manually triggered. Same as setting the timespan to infinite when creating the task.
        /// </summary>
        /// <param name="name">Name of task to pause</param>
        /// <param name="paused">True to pause the task, false to unpause</param>
        public void PauseTask(string name, bool paused)
        {
            if (!_tasks.TryGetValue(name, out var task)) throw new InvalidOperationException($"No such task: {name}");
            task.Paused = paused;
        }
        /// <summary>
        /// Manually trigger a task. The task will always run after this, but may run twice if it is about to run due to timeout.
        /// Either way the task will always run after this.  
        /// </summary>
        /// <param name="name"></param>
        public void TriggerTask(string name)
        {
            if (!_tasks.TryGetValue(name, out var task)) throw new InvalidOperationException($"No such task: {name}");
            task.Event.Set();
        }

        private async Task RunPeriodicTask(PeriodicTask task)
        {
            while (!_source.IsCancellationRequested && task.ShouldRun)
            {
                var timeout = task.Paused ? Timeout.InfiniteTimeSpan : task.Interval;
                var waitTask = WaitAsync(task.Event, task.Interval, _source.Token);
                if (!task.Paused) await task.Operation(_source.Token).ConfigureAwait(false);
                await waitTask.ConfigureAwait(false);
                task.Event.Reset();
            }
        }



        /// <summary>
        /// Convenient method to efficiently wait for a wait handle and cancellation token with timeout
        /// asynchronously. From https://thomaslevesque.com/2015/06/04/async-and-cancellation-support-for-wait-handles/.
        /// </summary>
        /// <param name="handle">WaitHandle to wait for</param>
        /// <param name="timeout">Wait timeout</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>True if wait handle or cancellation token was triggered, false otherwise</returns>
        private static async Task<bool> WaitAsync(WaitHandle handle, TimeSpan timeout, CancellationToken token)
        {
            RegisteredWaitHandle registeredHandle = null;
            CancellationTokenRegistration tokenRegistration = default(CancellationTokenRegistration);
            try
            {
                var tcs = new TaskCompletionSource<bool>();
                registeredHandle = ThreadPool.RegisterWaitForSingleObject(
                    handle,
                    (state, timedOut) => ((TaskCompletionSource<bool>)state).TrySetResult(!timedOut),
                    tcs,
                    timeout,
                    true);
                tokenRegistration = token.Register(
                    state => ((TaskCompletionSource<bool>)state).TrySetCanceled(),
                    tcs);
                return await tcs.Task.ConfigureAwait(true);
            }
            finally
            {
                if (registeredHandle != null)
                    registeredHandle.Unregister(null);
                tokenRegistration.Dispose();
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
