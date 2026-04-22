using CogniteSdk.Beta;
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
    /// Upload queue for stream records. Records are grouped by stream ID and uploaded
    /// to their respective streams.
    /// </summary>
    public class StreamRecordUploadQueue : BaseUploadQueue<(string streamId, StreamRecordWrite record)>
    {
        private static readonly Counter _numberRecords = Prometheus.Metrics.CreateCounter(
            "extractor_utils_queue_stream_records",
            "Number of stream records uploaded to CDF from the queue");
        private static readonly Gauge _queueSize = Prometheus.Metrics.CreateGauge(
            "extractor_utils_stream_records_queue_size",
            "Number of stream records in the upload queue to CDF");

        /// <summary>
        /// Upload queue for stream records.
        /// </summary>
        /// <param name="destination">CogniteDestination to use for uploading</param>
        /// <param name="interval">Interval between each automated push, leave at zero to disable timed pushing</param>
        /// <param name="maxSize">Max size of queue before pushing, set to zero to disable max size</param>
        /// <param name="logger">Logger to use</param>
        /// <param name="callback">Callback after uploading</param>
        public StreamRecordUploadQueue(
            CogniteDestination destination,
            TimeSpan interval,
            int maxSize,
            ILogger<CogniteDestination> logger,
            Func<QueueUploadResult<(string streamId, StreamRecordWrite record)>, Task>? callback)
            : base(destination, interval, maxSize, logger, callback)
        {
        }

        /// <summary>
        /// Enqueue a stream record.
        /// </summary>
        /// <param name="streamId">Stream external ID to ingest into</param>
        /// <param name="record">Stream record to enqueue</param>
        public void Enqueue(string streamId, StreamRecordWrite record)
        {
            Enqueue((streamId, record));
            _queueSize.Inc();
            _numberRecords.Inc();
        }

        /// <summary>
        /// Enqueue multiple stream records for the same stream.
        /// </summary>
        /// <param name="streamId">Stream external ID to ingest into</param>
        /// <param name="records">Stream records to enqueue</param>
        public void Enqueue(string streamId, IEnumerable<StreamRecordWrite> records)
        {
            if (records == null) return;
            foreach (var record in records)
            {
                Enqueue(streamId, record);
            }
        }

        /// <summary>
        /// Upload stream records to CDF. Records are grouped by stream ID.
        /// </summary>
        /// <param name="items">Stream records to upload</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>An error or the uploaded stream records</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Reliability", "CA2007: Do not directly await a Task", Justification = "Awaiter configured by the caller")]
        protected override async Task<QueueUploadResult<(string streamId, StreamRecordWrite record)>> UploadEntries(
            IEnumerable<(string streamId, StreamRecordWrite record)> items,
            CancellationToken token)
        {
            var itemsList = items.ToList();
            _queueSize.Dec(itemsList.Count);

            if (!itemsList.Any())
            {
                return new QueueUploadResult<(string streamId, StreamRecordWrite record)>(
                    Enumerable.Empty<(string streamId, StreamRecordWrite record)>(),
                    Enumerable.Empty<(string streamId, StreamRecordWrite record)>());
            }

            DestLogger.LogTrace("Dequeued {Number} stream records to upload to CDF", itemsList.Count);

            // Group records by stream ID
            var recordsByStream = itemsList
                .GroupBy(item => item.streamId)
                .ToDictionary(g => g.Key, g => g.Select(x => x.record).ToList());

            var uploaded = new List<(string streamId, StreamRecordWrite record)>();
            var failed = new List<(string streamId, StreamRecordWrite record)>();
            Exception? fatalException = null;

            foreach (var kvp in recordsByStream)
            {
                var streamId = kvp.Key;
                var records = kvp.Value;

                try
                {
                    await Destination.InsertRecordsAsync(streamId, records, token);
                    uploaded.AddRange(records.Select(r => (streamId, r)));
                }
                catch (Exception ex)
                {
                    DestLogger.LogWarning(ex, "Failed to upload {Count} stream records to stream {StreamId}: {Message}",
                        records.Count, streamId, ex.Message);
                    failed.AddRange(records.Select(r => (streamId, r)));
                    fatalException ??= ex;
                }
            }

            // If all records failed, return a fatal error
            if (!uploaded.Any() && failed.Any())
            {
                return new QueueUploadResult<(string streamId, StreamRecordWrite record)>(fatalException);
            }

            return new QueueUploadResult<(string streamId, StreamRecordWrite record)>(uploaded, failed);
        }
    }
}
