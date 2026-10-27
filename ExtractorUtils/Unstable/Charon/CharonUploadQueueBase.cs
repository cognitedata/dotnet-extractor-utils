using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Cognite.Extractor.Utils.Unstable.Charon
{
    /// <summary>
    /// Base upload queue that batches items and sends them to Charon through a single task.
    ///
    /// Implements <see cref="IUploadQueue{T}"/> directly (rather than extending
    /// <c>BaseUploadQueue</c>, which is bound to the concrete direct-to-CDF destination). Charon does
    /// no chunking, so each chunk this queue sends must already fit within CDF limits.
    /// </summary>
    /// <typeparam name="T">Queue item type.</typeparam>
    public abstract class CharonUploadQueueBase<T> : IUploadQueue<T>
    {
        /// <summary>Charon client used for uploads.</summary>
        protected ICharonClient Client { get; }
        /// <summary>Integration external id.</summary>
        protected string IntegrationId { get; }
        /// <summary>Task name this queue writes to.</summary>
        protected string TaskName { get; }
        /// <summary>Logger.</summary>
        protected ILogger Logger { get; }
        /// <summary>Callback invoked after every upload.</summary>
        protected Func<QueueUploadResult<T>, Task>? Callback { get; }

        private readonly ConcurrentQueue<T> _items = new ConcurrentQueue<T>();
        private readonly int _maxSize;
        private readonly ManualResetEventSlim _pushEvent = new ManualResetEventSlim(false);
        private readonly System.Timers.Timer? _timer;
        private CancellationTokenSource? _tokenSource;
        private Task? _uploadLoopTask;
        private Task? _uploadTask;

        /// <summary>
        /// Create a Charon upload queue.
        /// </summary>
        /// <param name="client">Charon client.</param>
        /// <param name="integrationId">Integration external id.</param>
        /// <param name="taskName">Task name to write to.</param>
        /// <param name="interval">Max interval between uploads. Zero or infinite disables the timer.</param>
        /// <param name="maxSize">Max queue size before an upload is forced. Zero disables the size trigger.</param>
        /// <param name="logger">Logger.</param>
        /// <param name="callback">Optional callback after each upload.</param>
        protected CharonUploadQueueBase(
            ICharonClient client,
            string integrationId,
            string taskName,
            TimeSpan interval,
            int maxSize,
            ILogger logger,
            Func<QueueUploadResult<T>, Task>? callback)
        {
            Client = client ?? throw new ArgumentNullException(nameof(client));
            IntegrationId = integrationId ?? throw new ArgumentNullException(nameof(integrationId));
            TaskName = taskName ?? throw new ArgumentNullException(nameof(taskName));
            Logger = logger ?? throw new ArgumentNullException(nameof(logger));
            Callback = callback;
            _maxSize = maxSize;

            if (interval == TimeSpan.Zero || interval == Timeout.InfiniteTimeSpan) return;
            _timer = new System.Timers.Timer { Interval = interval.TotalMilliseconds, AutoReset = false };
            _timer.Elapsed += (sender, e) => _pushEvent.Set();
        }

        /// <summary>Split items into chunks that each fit within CDF limits.</summary>
        /// <param name="items">Items to chunk.</param>
        /// <returns>Chunks, each a list of items.</returns>
        protected abstract IEnumerable<IReadOnlyList<T>> Chunk(IEnumerable<T> items);

        /// <summary>Convert a queue item to the JSON item body sent to Charon.</summary>
        /// <param name="item">Queue item.</param>
        /// <returns>JSON element for the Charon payload.</returns>
        protected abstract JsonElement ToPayloadItem(T item);

        /// <inheritdoc />
        public virtual void Enqueue(T item)
        {
            _items.Enqueue(item);
            if (_maxSize > 0 && _items.Count >= _maxSize) _pushEvent.Set();
        }

        /// <inheritdoc />
        public virtual void Enqueue(IEnumerable<T> items)
        {
            if (items == null) return;
            foreach (var item in items) _items.Enqueue(item);
            if (_maxSize > 0 && _items.Count >= _maxSize) _pushEvent.Set();
        }

        /// <inheritdoc />
        public virtual IEnumerable<T> Dequeue()
        {
            var items = new List<T>();
            while (_items.TryDequeue(out T item)) items.Add(item);
            return items;
        }

        /// <inheritdoc />
        public async Task<QueueUploadResult<T>> Trigger(CancellationToken token)
        {
            var items = Dequeue();
            return await UploadEntries(items, token).ConfigureAwait(false);
        }

        /// <summary>
        /// Send the given items to Charon, chunking to CDF limits. One <c>/payload</c> call per chunk.
        /// </summary>
        /// <param name="items">Items to upload.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>Upload result with uploaded/failed items, or a fatal exception.</returns>
        protected async Task<QueueUploadResult<T>> UploadEntries(IEnumerable<T> items, CancellationToken token)
        {
            var uploaded = new List<T>();
            var failed = new List<T>();
            CharonException? fatal = null;

            foreach (var chunk in Chunk(items))
            {
                if (chunk.Count == 0) continue;
                var body = new List<JsonElement>(chunk.Count);
                foreach (var item in chunk) body.Add(ToPayloadItem(item));
                var request = new CharonPayloadRequest
                {
                    IntegrationId = IntegrationId,
                    Tasks = new Dictionary<string, List<JsonElement>> { [TaskName] = body },
                };

                try
                {
                    var response = await Client.SendPayloadAsync(request, token).ConfigureAwait(false);
                    if (response.Tasks.TryGetValue(TaskName, out var result) && IsSuccess(result.Status))
                    {
                        uploaded.AddRange(chunk);
                    }
                    else
                    {
                        var status = response.Tasks.TryGetValue(TaskName, out var r) ? r.Status : 0;
                        Logger.LogWarning("Charon task {TaskName} chunk of {Count} items failed with status {Status}",
                            TaskName, chunk.Count, status);
                        failed.AddRange(chunk);
                    }
                }
                catch (CharonPayloadValidationException ex)
                {
                    Logger.LogWarning("Charon task {TaskName} chunk of {Count} items failed validation ({Count2} errors)",
                        TaskName, chunk.Count, ex.Errors.Count);
                    failed.AddRange(chunk);
                }
                catch (CharonException ex)
                {
                    Logger.LogError("Charon task {TaskName} upload failed with status {Status}", TaskName, ex.Status);
                    fatal = ex;
                    failed.AddRange(chunk);
                }
            }

            if (fatal != null && uploaded.Count == 0)
            {
                return new QueueUploadResult<T>(fatal);
            }
            return new QueueUploadResult<T>(uploaded, failed);
        }

        /// <summary>Whether a per-task status counts as success (any 2xx).</summary>
        /// <param name="status">Per-task status code.</param>
        /// <returns>True if 2xx.</returns>
        private static bool IsSuccess(int status) => status >= 200 && status < 300;

        /// <inheritdoc />
        public async Task Start(CancellationToken token)
        {
            _tokenSource = CancellationTokenSource.CreateLinkedTokenSource(token);
            _timer?.Start();
            Logger.LogDebug("Charon queue of type {Type} started for task {TaskName}", GetType().Name, TaskName);

            var tokenCancellationTask = Task.Run(async () =>
            {
                try { await Task.Delay(Timeout.Infinite, token).ConfigureAwait(false); }
                catch { }
            }, CancellationToken.None);

            _uploadLoopTask = Task.Run(async () =>
            {
                try
                {
                    while (!_tokenSource.IsCancellationRequested)
                    {
                        _pushEvent.Wait(_tokenSource.Token);
                        _uploadTask = TriggerUploadAndCallback(token);
                        await Task.WhenAny(_uploadTask, tokenCancellationTask).ConfigureAwait(false);
                        _pushEvent.Reset();
                        _timer?.Start();
                    }
                }
                catch (OperationCanceledException)
                {
                    Logger.LogDebug("Charon queue of type {Type} cancelled with {QueueSize} items left",
                        GetType().Name, _items.Count);
                }
            }, CancellationToken.None);
            await _uploadLoopTask.ConfigureAwait(false);
        }

        /// <summary>Run one upload and invoke the callback with its result.</summary>
        /// <param name="token">Cancellation token.</param>
        /// <returns>A task that completes when the upload and callback complete.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Reliability", "CA2007: Do not directly await a Task", Justification = "Awaiter configured by the caller")]
        private Task TriggerUploadAndCallback(CancellationToken token)
        {
            return Task.Run(async () =>
            {
                var result = await Trigger(token);
                if (Callback != null) await Callback(result);
            }, CancellationToken.None);
        }

        /// <summary>Flush remaining items on shutdown, bounded by a timeout.</summary>
        /// <returns>A task that completes when the queue is drained or times out.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Reliability", "CA2007: Do not directly await a Task", Justification = "Fine to wait in the same context")]
        private async Task FinalizeQueue()
        {
            try
            {
                _timer?.Stop();
                if (_tokenSource != null && !_tokenSource.IsCancellationRequested) _tokenSource.Cancel();
                if (_uploadLoopTask != null) await _uploadLoopTask;
                if (_uploadTask != null) await WaitOrTimeout(_uploadTask);
                await WaitOrTimeout(TriggerUploadAndCallback(CancellationToken.None));
            }
            catch (Exception ex)
            {
                Logger.LogError(ex, "Exception when disposing of Charon upload queue: {Message}", ex.Message);
            }
        }

        /// <summary>Await a task but give up after 60 seconds, logging on timeout.</summary>
        /// <param name="task">Task to await.</param>
        /// <returns>A task that completes when the inner task completes or the timeout elapses.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Reliability", "CA2007: Do not directly await a Task", Justification = "Awaiter configured by the caller")]
        private async Task WaitOrTimeout(Task task)
        {
            var t = await Task.WhenAny(task, Task.Delay(60_000));
            if (t != task || t.Status != TaskStatus.RanToCompletion)
            {
                Logger.LogError("Charon queue of type {Type} aborted before finishing uploading: Timeout", GetType().Name);
            }
        }

        /// <summary>Dispose, flushing remaining items.</summary>
        /// <param name="disposing">True when called from Dispose.</param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Reliability", "CA2007: Do not directly await a Task", Justification = "Fine to wait in the same context")]
        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                Task.Run(async () => await FinalizeQueue()).Wait();
                _pushEvent.Dispose();
                _timer?.Close();
                _tokenSource?.Dispose();
            }
        }

        /// <summary>Async dispose core: flush remaining items then release resources.</summary>
        /// <returns>A value task that completes when disposal finishes.</returns>
        protected async ValueTask DisposeAsyncCore()
        {
            await FinalizeQueue().ConfigureAwait(false);
            _pushEvent.Dispose();
            _timer?.Close();
            _tokenSource?.Dispose();
        }

        /// <summary>Dispose of the queue, uploading all remaining entries. Prefer DisposeAsync.</summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>Dispose of the queue asynchronously, uploading all remaining entries.</summary>
        /// <returns>A value task that completes when disposal finishes.</returns>
        public async ValueTask DisposeAsync()
        {
            await DisposeAsyncCore().ConfigureAwait(false);
            Dispose(false);
#pragma warning disable CA1816 // Dispose methods should call SuppressFinalize
            GC.SuppressFinalize(this);
#pragma warning restore CA1816
        }
    }
}
