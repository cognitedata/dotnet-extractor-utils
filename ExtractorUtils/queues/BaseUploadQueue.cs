using CogniteSdk;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.Extractor.Utils
{
    /// <summary>
    /// Result of an attempt to upload from an upload queue.
    /// </summary>
    /// <typeparam name="T">Queue item type</typeparam>
    public class QueueUploadResult<T>
    {
        /// <summary>
        /// List of items to be uploaded, may be null if upload failed, or empty if no objects were uploaded.
        /// </summary>
        public IEnumerable<T> Uploaded { get; }
        /// <summary>
        /// True if upload failed completely.
        /// </summary>
        public bool IsFailed => Exception != null;
        /// <summary>
        /// Exception if upload failed completely.
        /// </summary>
        public Exception Exception { get; }
        /// <summary>
        /// Constructor for successfull or empty upload.
        /// </summary>
        /// <param name="uploaded"></param>
        public QueueUploadResult(IEnumerable<T> uploaded)
        {
            Uploaded = uploaded;
        }
        /// <summary>
        /// Constructor for failed upload.
        /// </summary>
        /// <param name="ex">Fatal exception</param>
        public QueueUploadResult(Exception ex)
        {
            Exception = ex;
        }
    }

    /// <summary>
    /// Generic base class for upload queues
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public abstract class BaseUploadQueue<T> : IDisposable
    {
        private readonly int _maxSize;
        /// <summary>
        /// CogniteDestination to use
        /// </summary>
        protected readonly CogniteDestination _destination;
        /// <summary>
        /// Callback on upload
        /// </summary>
        protected readonly Func<QueueUploadResult<T>, Task> _callback;

        private readonly ConcurrentQueue<T> _items;
        /// <summary>
        /// Logger to use
        /// </summary>
        protected readonly ILogger<CogniteDestination> _logger;
        private readonly ManualResetEventSlim _pushEvent;
        private readonly System.Timers.Timer _timer;

        private CancellationTokenSource _tokenSource;
        private Task _uploadTask;

        internal BaseUploadQueue(
            CogniteDestination destination,
            TimeSpan interval,
            int maxSize,
            ILogger<CogniteDestination> logger,
            Func<QueueUploadResult<T>, Task> callback)
        {
            _maxSize = maxSize;
            _destination = destination;
            _items = new ConcurrentQueue<T>();
            _logger = logger;
            _pushEvent = new ManualResetEventSlim(false);
            _callback = callback;

            if (interval == TimeSpan.Zero || interval == Timeout.InfiniteTimeSpan) return;

            _timer = new System.Timers.Timer
            {
                Interval = interval.TotalMilliseconds,
                AutoReset = false
            };
            _timer.Elapsed += (sender, e) => _pushEvent.Set();
        }

        /// <summary>
        /// Enqueue a single item in the internal queue.
        /// </summary>
        /// <param name="item">Item to enqueue</param>
        public virtual void Enqueue(T item)
        {
            _items.Enqueue(item);
            if (_maxSize > 0 && _items.Count >= _maxSize)
            {
                _pushEvent.Set();
            }
        }

        /// <summary>
        /// Empty the queue and return the contents
        /// </summary>
        /// <returns>Contents of the queue</returns>
        public virtual IEnumerable<T> Dequeue()
        {
            var items = new List<T>();
            while (_items.TryDequeue(out T item))
            {
                items.Add(item);
            }
            return items;
        }

        /// <summary>
        /// Trigger queue upload and return the result instead of calling the callback.
        /// Can be used as alternative for callback entirely.
        /// </summary>
        /// <param name="token"></param>
        /// <returns>A <see cref="QueueUploadResult{T}"/> containing an error or the uploaded entries</returns>
        public async Task<QueueUploadResult<T>> Trigger(CancellationToken token)
        {
            var items = Dequeue();
            return await UploadEntries(items, token);
        }

        /// <summary>
        /// Start automatically uploading queue entries
        /// </summary>
        public async Task Start(CancellationToken token)
        {
            _tokenSource = CancellationTokenSource.CreateLinkedTokenSource(token);
            _timer.Start();
            _logger.LogDebug("Queue of type {Type} started", GetType().Name);
            _uploadTask = Task.Run(async () =>
            {
                try
                {
                    while (!_tokenSource.IsCancellationRequested)
                    {
                        _pushEvent.Wait(_tokenSource.Token);
                        var result = await Trigger(_tokenSource.Token);
                        _pushEvent.Reset();
                        _timer.Start();
                        if (_callback != null) await _callback(result);
                    }
                }
                catch (OperationCanceledException)
                {
                    // ignore cancel exceptions, but throw any other exception
                    _logger.LogDebug("Upload queue of type {Type} cancelled with {QueueSize} items left", GetType().Name, _items.Count);
                }
            });
            await _uploadTask;
        }
        /// <summary>
        /// Method called to upload entries
        /// </summary>
        /// <param name="items">Items to upload</param>
        /// <param name="token"></param>
        /// <returns>A <see cref="QueueUploadResult{T}"/> containing an error or the uploaded entries</returns>
        protected abstract Task<QueueUploadResult<T>> UploadEntries(IEnumerable<T> items, CancellationToken token);

        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls

        /// <summary>
        /// Dispose of the queue, uploading all remaining entries.
        /// </summary>
        /// <param name="disposing"></param>
        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    try
                    {
                        _timer.Stop();
                        var items = Dequeue();
                        var result = UploadEntries(items, _tokenSource.Token).GetAwaiter().GetResult();
                        if (_callback != null) _callback(result).GetAwaiter().GetResult();
                        if (!_tokenSource.IsCancellationRequested)
                        {
                            _tokenSource.Cancel();
                        }
                        _uploadTask.GetAwaiter().GetResult();
                    }
                    finally
                    {
                        _pushEvent.Dispose();
                        _timer.Close();
                        _tokenSource.Dispose();
                    }
                }

                disposedValue = true;
            }
        }

        /// <summary>
        /// Dispose of the queue, uploading all remaining entries.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
        }
        #endregion
    }
}
