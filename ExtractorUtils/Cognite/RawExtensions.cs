using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extractor.Common;
using CogniteSdk;
using CogniteSdk.Resources;
using Microsoft.Extensions.Logging;
using Prometheus;

namespace Cognite.Extractor.Utils
{
    /// <summary>
    /// Extension utility methods for <see cref="Client.Raw"/>
    /// </summary>
    public static class RawExtensions
    {
        /// <summary>
        /// Insert the rows <paramref name="columns"/> in CDF Raw. The rows are a dictionary of 
        /// keys and DTOs (data type objects). The DTOs are serialized to JSON before it is sent to
        /// Raw.
        /// </summary>
        /// <param name="raw">Raw client</param>
        /// <param name="database">Raw database</param>
        /// <param name="table">Raw table</param>
        /// <param name="columns">Columns</param>
        /// <param name="chunkSize">Chunk size</param>
        /// <param name="throttleSize">Throttle size</param>
        /// <param name="token">Cancelation token</param>
        /// <typeparam name="T">DTO type</typeparam>
        public static async Task InsertRowsAsync<T>(
            this RawResource raw,
            string database, 
            string table, 
            IDictionary<string, T> columns, 
            int chunkSize, 
            int throttleSize,
            CancellationToken token)
        {
            var chunks = columns
                .Select(kvp =>  new RawRowCreateJson() { Key = kvp.Key, Columns = DtoToJson(kvp.Value) })
                .ChunkBy(chunkSize);
            var generators = chunks.
                Select<IEnumerable<RawRowCreateJson>, Func<Task>>(
                    chunk => async () => await raw.CreateRowsJsonAsync(database, table, chunk, true, token)
                );
            await generators.RunThrottled(throttleSize, token);
        }

        internal static JsonElement DtoToJson<T>(T dto)
        {
            var bytes = JsonSerializer.SerializeToUtf8Bytes(dto);
            var document = JsonDocument.Parse(bytes);
            return document.RootElement;
        }
    }

    /// <summary>
    /// Interface to CDF Raw upload queues. The items in the queue are
    /// DTOs (data type objects) of type <typeparamref name="T"/>.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public interface IRawUploadQueue<T> : IDisposable
    {
        /// <summary>
        /// Enqueue the DTO of type <typeparamref name="T"/> to be uploaded
        /// as a row to CDF Raw.
        /// </summary>
        /// <param name="key">The row key</param>
        /// <param name="columns">The row columns</param>
        void EnqueueRow(string key, T columns);
        
        /// <summary>
        /// Starts a <see cref="Task"/> to insert rows into CDF Raw at regular intervals. Waiting on the returned 
        /// task will throw any eventual exceptions. To stop the upload queue, dispose the upload queue object or
        /// cancel the provided <paramref name="token"/>.
        /// </summary>
        /// <param name="token">Cancellation token</param>
        /// <returns>Upload queue task</returns>
        Task Start(CancellationToken token);
    }

    internal class RawUploadQueue<T> : IRawUploadQueue<T>
    {
        private readonly string _db;
        private readonly string _table;
        private readonly int _maxSize;
        private readonly CogniteDestination _destination;
        private readonly ConcurrentQueue<(string key, T column)> _items;
        private readonly ILogger<CogniteDestination> _logger;
        private readonly ManualResetEventSlim _pushEvent;
        private readonly System.Timers.Timer _timer;

        private Task _uploadTask;
        private CancellationTokenSource _tokenSource;

        private static readonly Counter _numberRows = Prometheus.Metrics.CreateCounter("extractor_utils_raw_rows", "Number of rows uploaded to CDF Raw", "type");
        private static readonly Gauge _queueSize = Prometheus.Metrics.CreateGauge("extractor_utils_raw_queue_size", "Number of elements in the upload queue to CDF Raw", "type");

        internal RawUploadQueue(
            string db, 
            string table, 
            CogniteDestination destination, 
            TimeSpan interval,
            int maxSize,
            ILogger<CogniteDestination> logger)
        {
            _db = db;
            _table = table;
            _maxSize = maxSize;
            _destination = destination;
            _items = new ConcurrentQueue<(string key, T column)>();
            _logger = logger;
            _pushEvent = new ManualResetEventSlim(false);
            _queueSize.WithLabels(typeof(T).Name).Set(0);

            _timer = new System.Timers.Timer {
                Interval = interval.TotalMilliseconds,
                AutoReset = false
            };
            _timer.Elapsed += (sender, e) =>_pushEvent.Set();

        }

        public async Task Start(CancellationToken token)
        {
            _tokenSource = CancellationTokenSource.CreateLinkedTokenSource(token);
            _timer.Start();
            _logger.LogDebug("Started uploading {Type} rows to CDF Raw", typeof(T).Name);
            _uploadTask = Task.Run(async () =>
            {
                while (!_tokenSource.IsCancellationRequested)
                {
                    _pushEvent.Wait(_tokenSource.Token);
                    await OnTimedEvent(_tokenSource.Token);
                    _pushEvent.Reset();
                    _timer.Start();
                }
            });
            await _uploadTask;
        }

        private async Task OnTimedEvent(CancellationToken token)
        {
            var rows = GetRawRows();
            if (!rows.Any())
            {
                return;
            }
            _logger.LogTrace("Dequeued {Number} {Type} rows to upload to CDF Raw", rows.Count, typeof(T).Name);
            await _destination.InsertRawRowsAsync(_db, _table, rows, token);
            _numberRows.WithLabels(typeof(T).Name).Inc(rows.Count);
        }

        public void EnqueueRow(string key, T columns)
        {
            _items.Enqueue((key, columns));
            _queueSize.WithLabels(typeof(T).Name).Inc();
            if (_maxSize > 0 && _items.Count >= _maxSize)
            {
                _pushEvent.Set();
            }
        }

        private IDictionary<string, T> GetRawRows()
        {
            var dict = new Dictionary<string, T>();
            while (_items.TryDequeue(out (string key, T column) row))
            {
                dict.Add(row.key, row.column);
            }
            return dict;
        }

        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    try
                    {
                        _timer.Stop();
                        OnTimedEvent(_tokenSource.Token).GetAwaiter().GetResult();
                        if (!_tokenSource.IsCancellationRequested)
                        {
                            _tokenSource.Cancel();
                            _uploadTask.GetAwaiter().GetResult();
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        _logger.LogDebug("Upload queue {Type} cancelled with {QueueSize} items left", typeof(T).Name, _items.Count);
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

        public void Dispose()
        {
            Dispose(true);
        }
        #endregion
    }

}