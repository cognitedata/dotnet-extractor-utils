﻿using CogniteSdk;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.Extractor.Utils
{
    /// <summary>
    /// Base interface for upload queue
    /// </summary>
    public interface IUploadQueue : IDisposable, IAsyncDisposable
    {
        /// <summary>
        /// Start automatically uploading queue entries
        /// </summary>
        Task Start(CancellationToken token);
    }

    /// <summary>
    /// Interface for generic upload queue
    /// </summary>
    /// <typeparam name="T">Type of resource to upload</typeparam>
    public interface IUploadQueue<T> : IUploadQueue
    {
        /// <summary>
        /// Enqueue a single item in the internal queue.
        /// </summary>
        /// <param name="item">Item to enqueue</param>
        void Enqueue(T item);
        /// <summary>
        /// Enqueue a list of items in the internal queue.
        /// </summary>
        /// <param name="items">Items to enqueue</param>
        void Enqueue(IEnumerable<T> items);
        /// <summary>
        /// Empty the queue and return the contents
        /// </summary>
        /// <returns>Contents of the queue</returns>
        IEnumerable<T> Dequeue();
        /// <summary>
        /// Trigger queue upload and return the result instead of calling the callback.
        /// Can be used as alternative for callback entirely.
        /// </summary>
        /// <param name="token"></param>
        /// <returns>A <see cref="QueueUploadResult{T}"/> containing an error or the uploaded entries</returns>
        Task<QueueUploadResult<T>> Trigger(CancellationToken token);
    }


    /// <summary>
    /// Result of an attempt to upload from an upload queue.
    /// </summary>
    /// <typeparam name="T">Queue item type</typeparam>
    public class QueueUploadResult<T>
    {
        /// <summary>
        /// List of items to be uploaded, may be null if upload failed, or empty if no objects were uploaded.
        /// </summary>
        public IEnumerable<T>? Uploaded { get; }
        /// <summary>
        /// True if upload failed completely.
        /// </summary>
        public bool IsFailed => Exception != null;
        /// <summary>
        /// Exception if upload failed completely.
        /// </summary>
        public Exception? Exception { get; }
        /// <summary>
        /// Items that failed to upload
        /// </summary>
        public IEnumerable<T>? Failed { get; }
        /// <summary>
        /// Constructor for successfull or empty upload.
        /// </summary>
        /// <param name="uploaded"></param>
        /// <param name="failed"></param>
        public QueueUploadResult(IEnumerable<T> uploaded, IEnumerable<T> failed)
        {
            Uploaded = uploaded;
            Failed = failed;
        }
        /// <summary>
        /// Constructor for failed upload.
        /// </summary>
        /// <param name="ex">Fatal exception</param>
        public QueueUploadResult(Exception? ex)
        {
            Exception = ex;
        }
    }

    /// <summary>
    /// Generic base class for upload queues
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public abstract class BaseUploadQueue<T> : IUploadQueue<T>
    {
        /// <summary>
        /// CogniteDestination to use
        /// </summary>
        protected CogniteDestination Destination { get; private set; }
        /// <summary>
        /// Callback on upload
        /// </summary>
        protected Func<QueueUploadResult<T>, Task>? Callback { get; private set; }

        /// <summary>
        /// Logger to use
        /// </summary>
        protected ILogger DestLogger { get; private set; }

        private readonly ConcurrentQueue<T> _items;
        private readonly int _maxSize;
        private readonly ManualResetEventSlim _pushEvent;
        private readonly System.Timers.Timer? _timer;
        private CancellationTokenSource? _tokenSource;
        private CancellationTokenSource? _internalSource;
        private Task? _uploadLoopTask;
        private Task? _uploadTask;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="destination">Cognite destination used for uploads</param>
        /// <param name="interval">Maximum interval between uploads. Set to zero or Timeout.InfiniteTimeSpan to disable periodic uploads</param>
        /// <param name="maxSize">Maximum size of the upload queue, once it reaches this size an upload will always be triggered</param>
        /// <param name="logger">Logger to use</param>
        /// <param name="callback">Callback on finished upload, whether it failed or not</param>
        protected BaseUploadQueue(
            CogniteDestination destination,
            TimeSpan interval,
            int maxSize,
            ILogger logger,
            Func<QueueUploadResult<T>, Task>? callback)
        {
            _maxSize = maxSize;
            Destination = destination;
            _items = new ConcurrentQueue<T>();
            DestLogger = logger;
            _pushEvent = new ManualResetEventSlim(false);
            Callback = callback;

            if (interval == TimeSpan.Zero || interval == Timeout.InfiniteTimeSpan) return;

            _timer = new System.Timers.Timer
            {
                Interval = interval.TotalMilliseconds,
                AutoReset = false
            };
            _timer.Elapsed += (sender, e) => _pushEvent.Set();
        }

        /// <inheritdoc />
        public virtual void Enqueue(T item)
        {
            _items.Enqueue(item);
            if (_maxSize > 0 && _items.Count >= _maxSize)
            {
                _pushEvent.Set();
            }
        }

        /// <inheritdoc />
        public virtual void Enqueue(IEnumerable<T> items)
        {
            if (items == null) return;
            foreach (var item in items)
            {
                _items.Enqueue(item);
            }
            if (_maxSize > 0 && _items.Count >= _maxSize)
            {
                _pushEvent.Set();
            }
        }

        /// <inheritdoc />
        public virtual IEnumerable<T> Dequeue()
        {
            var items = new List<T>();
            while (_items.TryDequeue(out T item))
            {
                items.Add(item);
            }
            return items;
        }

        /// <inheritdoc />
        public async Task<QueueUploadResult<T>> Trigger(CancellationToken token)
        {
            var items = Dequeue();
            return await UploadEntries(items, token).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public async Task Start(CancellationToken token)
        {
            _tokenSource = CancellationTokenSource.CreateLinkedTokenSource(token);
            _timer?.Start();
            DestLogger.LogDebug("Queue of type {Type} started", GetType().Name);

            // Use a separate token source to allow cancelling the uploader separate from the upload loop.
            _internalSource = new CancellationTokenSource();

            var tokenCancellationTask = Task.Run(async () =>
            {
                try
                {
                    await Task.Delay(Timeout.Infinite, token).ConfigureAwait(false);
                }
                catch { }
            }, CancellationToken.None);
            _uploadLoopTask = Task.Run(async () =>
            {
                try
                {
                    while (!_tokenSource.IsCancellationRequested)
                    {
                        _pushEvent?.Wait(_tokenSource.Token);
                        _uploadTask = TriggerUploadAndCallback(_internalSource.Token);
                        // stop waiting if the source token gets cancelled, but do not
                        // cancel the upload task
                        await Task.WhenAny(_uploadTask, tokenCancellationTask).ConfigureAwait(false);
                        _pushEvent?.Reset();
                        _timer?.Start();
                    }
                }
                catch (OperationCanceledException)
                {
                    // ignore cancel exceptions, but throw any other exception
                    DestLogger.LogDebug("Upload queue of type {Type} cancelled with {QueueSize} items left", GetType().Name, _items.Count);
                }
            }, CancellationToken.None);
            await _uploadLoopTask.ConfigureAwait(false);
        }


        [System.Diagnostics.CodeAnalysis.SuppressMessage("Reliability", "CA2007: Do not directly await a Task", Justification = "Awaiter configured by the caller")]
        private Task TriggerUploadAndCallback(CancellationToken token)
        {
            return Task.Run(async () =>
            {
                var result = await Trigger(token);
                if (Callback != null) await Callback(result);
            }, CancellationToken.None);
        }

        /// <summary>
        /// Method called to upload entries
        /// </summary>
        /// <param name="items">Items to upload</param>
        /// <param name="token"></param>
        /// <returns>A <see cref="QueueUploadResult{T}"/> containing an error or the uploaded entries</returns>
        protected abstract Task<QueueUploadResult<T>> UploadEntries(IEnumerable<T> items, CancellationToken token);

        #region IDisposable Support

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Reliability", "CA2007: Do not directly await a Task", Justification = "Fine to wait in the same context")]
        private async Task FinalizeQueue()
        {
            try
            {
                _timer?.Stop();

                if (_tokenSource != null && !_tokenSource.IsCancellationRequested)
                {
                    _tokenSource.Cancel();
                }
                if (_uploadLoopTask != null)
                {
                    await _uploadLoopTask;
                }
                if (_uploadTask != null)
                {
                    // Wait for the current upload task to end or timeout, if any
                    await WaitOrTimeout(_uploadTask);
                }

                // If there is anything left in the queue, push it,
                await WaitOrTimeout(TriggerUploadAndCallback(CancellationToken.None));
            }
            catch (Exception ex)
            {
                DestLogger.LogError(ex, "Exception when disposing of upload queue: {msg}", ex.Message);
            }
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Reliability", "CA2007: Do not directly await a Task", Justification = "Awaiter configured by the caller")]
        private async Task WaitOrTimeout(Task task)
        {
            var t = await Task.WhenAny(task, Task.Delay(60_000));
            if (t != task || t.Status != TaskStatus.RanToCompletion)
            {
                DestLogger.LogError("Upload queue of type {Type} aborted before finishing uploading: Timeout", GetType().Name);
            }
        }

        /// <summary>
        /// Dispose of the queue, uploading all remaining entries.
        /// </summary>
        /// <param name="disposing"></param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Reliability", "CA2007: Do not directly await a Task", Justification = "Fine to wait in the same context")]
        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                Task.Run(async () => await FinalizeQueue()).Wait();
                _pushEvent.Dispose();
                _timer?.Close();
                _tokenSource?.Dispose();
                _internalSource?.Dispose();
            }
        }

        /// <summary>
        /// Internal method to dispose asynchronously.
        /// </summary>
        protected async ValueTask DisposeAsyncCore()
        {
            await FinalizeQueue().ConfigureAwait(false);
            _pushEvent.Dispose();
            _timer?.Close();
            _tokenSource?.Dispose();
            _internalSource?.Dispose();
        }

        /// <summary>
        /// Dispose of the queue, uploading all remaining entries. Use DisposeAsync instead if possible.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Dispose of the queue, uploading all remaining entries. Preferred over synchronous dispose.
        /// </summary>
        public async ValueTask DisposeAsync()
        {
            await DisposeAsyncCore().ConfigureAwait(false);
            Dispose(false);
#pragma warning disable CA1816 // Dispose methods should call SuppressFinalize
            GC.SuppressFinalize(this);
#pragma warning restore CA1816 // Dispose methods should call SuppressFinalize
        }
        #endregion
    }
}
