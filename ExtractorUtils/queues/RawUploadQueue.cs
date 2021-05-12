using Microsoft.Extensions.Logging;
using Prometheus;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.Extractor.Utils
{
    /// <summary>
    /// Interface to CDF Raw upload queues. The items in the queue are
    /// DTOs (data type objects) of type <typeparamref name="T"/>.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public interface IRawUploadQueue<T> : IDisposable
    {
        /// <summary>
        /// Trigger upload immediately, returning upload result
        /// </summary>
        /// <param name="token"></param>
        /// <returns>Wrapper containing the list of columns or an error if upload failed</returns>
        Task<QueueUploadResult<(string key, T columns)>> Trigger(CancellationToken token);
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
    class RawUploadQueue<T> : BaseUploadQueue<(string key, T columns)>, IRawUploadQueue<T>
    {
        private readonly string _db;
        private readonly string _table;

        private static readonly Counter _numberRows = Prometheus.Metrics.CreateCounter("extractor_utils_raw_rows",
            "Number of rows uploaded to CDF Raw", "type");
        private static readonly Gauge _queueSize = Prometheus.Metrics.CreateGauge("extractor_utils_raw_queue_size",
            "Number of elements in the upload queue to CDF Raw", "type");

        internal RawUploadQueue(
            string db,
            string table,
            CogniteDestination destination,
            TimeSpan interval,
            int maxSize,
            ILogger<CogniteDestination> logger,
            Func<QueueUploadResult<(string key, T columns)>, Task> callback) : base(destination, interval, maxSize, logger, callback)
        {
            _db = db;
            _table = table;
        }

        public void EnqueueRow(string key, T columns)
        {
            Enqueue((key, columns));
            _queueSize.WithLabels(typeof(T).Name).Inc();
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Reliability", "CA2007: Do not directly await a Task", Justification = "Awaiter configured by the caller")]
        protected override async Task<QueueUploadResult<(string key, T columns)>> UploadEntries(
            IEnumerable<(string key, T columns)> items, CancellationToken token)
        {
            _queueSize.Dec(items.Count());
            var rows = items.ToDictionary(pair => pair.key, pair => pair.columns);
            if (!rows.Any())
            {
                return new QueueUploadResult<(string key, T columns)>(Enumerable.Empty<(string key, T columns)>());
            }
            DestLogger.LogTrace("Dequeued {Number} {Type} rows to upload to CDF Raw", rows.Count, typeof(T).Name);
            try
            {
                await Destination.InsertRawRowsAsync(_db, _table, rows, token);
            }
            catch (Exception ex)
            {
                return new QueueUploadResult<(string key, T columns)>(ex);
            }
            _numberRows.WithLabels(typeof(T).Name).Inc(rows.Count);
            return new QueueUploadResult<(string key, T columns)>(items);
        }
    }
}
