using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extractor.Common;
using Cognite.Extractor.Logging;
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
        private static ILogger _logger = LoggingUtils.GetDefault();

        internal static void SetLogger(ILogger logger)
        {
            _logger = logger;
        }
        /// <summary>
        /// Insert the provided <paramref name="rows"/> into CDF Raw. The rows are a dictionary of 
        /// keys and DTOs (data type objects). The DTOs  of type <typeparamref name="T"/> are serialized to JSON 
        /// before they are sent to Raw. If the <paramref name="database"/> or <paramref name="table"/> do not
        /// exist, they are created
        /// </summary>
        /// <param name="raw">Raw client</param>
        /// <param name="database">Raw database name</param>
        /// <param name="table">Raw table name</param>
        /// <param name="rows">Rows of keys and columns</param>
        /// <param name="chunkSize">Chunk size</param>
        /// <param name="throttleSize">Throttle size</param>
        /// <param name="token">Cancelation token</param>
        /// <param name="options">Optional JSON options parameter, to be used when converting dto to JsonElement</param>
        /// <typeparam name="T">DTO type</typeparam>
        public static async Task InsertRowsAsync<T>(
            this RawResource raw,
            string database, 
            string table, 
            IDictionary<string, T> rows, 
            int chunkSize, 
            int throttleSize,
            CancellationToken token,
            JsonSerializerOptions options = null)
        {
            var chunks = rows
                .Select(kvp =>  new RawRowCreateJson() { Key = kvp.Key, Columns = DtoToJson(kvp.Value, options) })
                .ChunkBy(chunkSize);

            var generators = chunks.
                Select<IEnumerable<RawRowCreateJson>, Func<Task>>(
                    chunk => async () => {
                        using (CdfMetrics.Raw.WithLabels("create_rows"))
                        {
                            await raw.CreateRowsJsonAsync(database, table, chunk, true, token);
                        }
                    }
                );
            int numTasks = 0;
            await generators.RunThrottled(throttleSize, (_) =>
                _logger.LogDebug("{MethodName} completed {Num}/{Total} tasks", nameof(InsertRowsAsync), ++numTasks,
                    Math.Ceiling((double)rows.Count / chunkSize)),  token);
        }

        internal static JsonElement DtoToJson<T>(T dto, JsonSerializerOptions options)
        {
            if (dto is JsonElement) return (JsonElement)(object)dto;
            var bytes = JsonSerializer.SerializeToUtf8Bytes(dto, options);
            var document = JsonDocument.Parse(bytes);
            return document.RootElement;
        }

        /// <summary>
        /// Returns all rows from the given database and table. <paramref name="chunkSize"/> items are fetched with each request.
        /// </summary>
        /// <param name="raw">Raw client</param>
        /// <param name="dbName">Database to read from</param>
        /// <param name="tableName">Table to read from</param>
        /// <param name="chunkSize">Max number of items per request</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>All rows</returns>
        public static async Task<IDictionary<string, IDictionary<string, JsonElement>>> GetRowsAsync(
            this RawResource raw,
            string dbName,
            string tableName,
            int chunkSize,
            CancellationToken token)
        {
            // This might be able to be improved with the ability to pre-fetch cursors for parallel read. Missing from the SDK.
            var result = new Dictionary<string, IDictionary<string, JsonElement>>();
            string cursor = null;
            do
            {
                var query = new RawRowQuery
                {
                    Limit = chunkSize
                };
                if (cursor != null)
                {
                    query.Cursor = cursor;
                }
                ItemsWithCursor<RawRow> rows;
                using (CdfMetrics.Raw.WithLabels("list_rows").NewTimer())
                {
                    rows = await raw.ListRowsAsync(dbName, tableName, query, token);
                }
                foreach (var row in rows.Items)
                {
                    result[row.Key] = row.Columns;
                }
                cursor = rows.NextCursor;
                _logger.LogDebug("Read: {count} rows from raw table: {raw}, database: {db}", rows.Items.Count(), tableName, dbName);
            } while (cursor != null);
            return result;
        }

        /// <summary>
        /// Delete the given rows from raw database and table.
        /// Will succeed even if database or table does not exist.
        /// </summary>
        /// <param name="raw">Raw client</param>
        /// <param name="dbName">Database to delete from</param>
        /// <param name="tableName">Table to delete from</param>
        /// <param name="rowKeys">Keys for rows to delete</param>
        /// <param name="chunkSize">Number of deleted rows per request</param>
        /// <param name="throttleSize">Nax number of parallel threads</param>
        /// <param name="token">Cancellation token</param>
        /// <returns></returns>
        public static async Task DeleteRowsAsync(
            this RawResource raw,
            string dbName,
            string tableName,
            IEnumerable<string> rowKeys,
            int chunkSize,
            int throttleSize,
            CancellationToken token)
        {
            var chunks = rowKeys
                .Select(key => new RawRowDelete { Key = key })
                .ChunkBy(chunkSize);
            var generators = chunks
                .Select<IEnumerable<RawRowDelete>, Func<Task>>(
                    chunk => async () =>
                    {
                        using (CdfMetrics.Raw.WithLabels("delete").NewTimer())
                        {
                            await raw.DeleteRowsAsync(dbName, tableName, chunk, token);
                        }
                    }
                );
            try
            {
                int numTasks = 0;
                await generators.RunThrottled(throttleSize, (_) =>
                    _logger.LogDebug("{MethodName} completed {Num}/{Total} tasks", nameof(DeleteRowsAsync), ++numTasks, rowKeys.Count()), token);
            }
            catch (ResponseException ex)
            {
                // In order to ignore missing tables/databases
                if (ex.Code == 404)
                {
                    _logger.LogDebug(ex.Message);
                    return;
                }
                throw;
            }
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
                try {
                    while (!_tokenSource.IsCancellationRequested)
                    {
                        _pushEvent.Wait(_tokenSource.Token);
                        await OnTimedEvent(_tokenSource.Token);
                        _pushEvent.Reset();
                        _timer.Start();
                    }
                }
                catch (OperationCanceledException)
                {
                    // ignore cancel exceptions, but throw any other exception
                    _logger.LogDebug("Upload queue {Type} cancelled with {QueueSize} items left", typeof(T).Name, _items.Count);
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
                _queueSize.WithLabels(typeof(T).Name).Dec();
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

        public void Dispose()
        {
            Dispose(true);
        }
        #endregion
    }

}