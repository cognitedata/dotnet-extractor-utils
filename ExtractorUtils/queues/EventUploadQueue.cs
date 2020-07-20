using CogniteSdk;
using Microsoft.Extensions.Logging;
using Prometheus;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.Extractor.Utils
{
    /// <summary>
    /// Queue for uploading events to CDF.
    /// </summary>
    public class EventUploadQueue : BaseUploadQueue<EventCreate>
    {
        private static readonly Counter _numberEvents = Prometheus.Metrics.CreateCounter("extractor_utils_queue_events",
            "Number of events uploaded to CDF from the queue");
        private static readonly Gauge _queueSize = Prometheus.Metrics.CreateGauge("extractor_utils_events_queue_size",
            "Number of events in the upload queue to CDF");

        private readonly string _bufferPath;
        private bool _bufferEnabled;
        private bool _bufferAny;
        /// <summary>
        /// Upload queue for events
        /// </summary>
        /// <param name="destination">CogniteDestination to use for uploading</param>
        /// <param name="interval">Interval between each automated push, leave at zero to disable timed pushing</param>
        /// <param name="maxSize">Max size of queue before pushing, set to zero to disable max size</param>
        /// <param name="logger">Logger to use</param>
        /// <param name="callback">Callback after uploading</param>
        /// <param name="bufferPath">Path to local buffer file for binary buffering of events</param>
        public EventUploadQueue(
            CogniteDestination destination,
            TimeSpan interval,
            int maxSize,
            ILogger<CogniteDestination> logger,
            Func<QueueUploadResult<EventCreate>, Task> callback,
            string bufferPath) : base(destination, interval, maxSize, logger, callback)
        {
            _bufferPath = bufferPath;
            if (!string.IsNullOrWhiteSpace(_bufferPath))
            {
                _bufferEnabled = true;
                if (!System.IO.File.Exists(_bufferPath))
                {
                    System.IO.File.Create(_bufferPath).Close();
                }
                _bufferAny = new FileInfo(_bufferPath).Length > 0;
                _bufferEnabled = true;
            }
        }

        /// <summary>
        /// Enqueue an event
        /// </summary>
        /// <param name="evt"></param>
        public override void Enqueue(EventCreate evt)
        {
            base.Enqueue(evt);
            _numberEvents.Inc();
        }

        private async Task WriteToBuffer(IEnumerable<EventCreate> events, CancellationToken token)
        {
            try
            {
                using (var stream = new FileStream(_bufferPath, FileMode.Append, FileAccess.Write, FileShare.None))
                {
                    await CogniteUtils.WriteEventsAsync(events, stream, token);
                }
                _bufferAny = true;
            }
            catch (Exception ex)
            {
                _logger.LogWarning("Failed to write to buffer: {msg}", ex.Message);
            }
        }

        private async Task ReadFromBuffer(CancellationToken token)
        {
            IEnumerable<EventCreate> events;
            try
            {
                using (var stream = new FileStream(_bufferPath, FileMode.OpenOrCreate, FileAccess.Read, FileShare.None))
                {

                    do
                    {
                        // Chunk size is less about CDF chunking, and more about memory usage.
                        // If the queue is offline for a day, and generates a hundred gigabytes of events,
                        // the file could become unreadable.
                        events = await CogniteUtils.ReadEventsAsync(stream, token, 10_000);
                        if (events.Any())
                        {
                            await _destination.EnsureEventsExistsAsync(events, true, token);
                        }
                    } while (events.Any());
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning("Failed to read from buffer: {msg}", ex.Message);
                return;
            }
            System.IO.File.Create(_bufferPath).Close();
            _bufferAny = false;
        }

        /// <summary>
        /// Upload events to CDF.
        /// </summary>
        /// <param name="items">Events to upload</param>
        /// <param name="token"></param>
        /// <returns>An error or the uploaded events</returns>
        protected override async Task<QueueUploadResult<EventCreate>> UploadEntries(IEnumerable<EventCreate> items, CancellationToken token)
        {
            _queueSize.Dec(items.Count());

            if (!items.Any()) return new QueueUploadResult<EventCreate>(Enumerable.Empty<EventCreate>());

            _logger.LogTrace("Dequeued {Number} events to upload to CDF", items.Count());

            try
            {
                await _destination.EnsureEventsExistsAsync(items, true, token);
                _numberEvents.Inc(items.Count());
            }
            catch (Exception ex)
            {
                // We retry on errors, if we hit a 400 here, it is probably an user error, and we don't really want to buffer.
                if (_bufferEnabled && (!(ex is ResponseException rex) || rex.Code >= 500))
                {
                    await WriteToBuffer(items, token);
                }
                return new QueueUploadResult<EventCreate>(ex);
            }
            if (_bufferAny)
            {
                await ReadFromBuffer(token);
            }
            return new QueueUploadResult<EventCreate>(items);
        }
    }
}
