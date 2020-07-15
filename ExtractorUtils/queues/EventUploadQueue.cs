using CogniteSdk;
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
    /// Queue for uploading events to CDF.
    /// </summary>
    public class EventUploadQueue : BaseUploadQueue<EventCreate>
    {
        private static readonly Counter _numberEvents = Prometheus.Metrics.CreateCounter("extractor_utils_queue_events",
            "Number of events uploaded to CDF from the queue");
        private static readonly Gauge _queueSize = Prometheus.Metrics.CreateGauge("extractor_utils_events_queue_size",
            "Number of events in the upload queue to CDF");
        /// <summary>
        /// Upload queue for events
        /// </summary>
        /// <param name="destination">CogniteDestination to use for uploading</param>
        /// <param name="interval">Interval between each automated push, leave at zero to disable timed pushing</param>
        /// <param name="maxSize">Max size of queue before pushing, set to zero to disable max size</param>
        /// <param name="logger">Logger to use</param>
        public EventUploadQueue(
            CogniteDestination destination,
            TimeSpan interval,
            int maxSize,
            ILogger<CogniteDestination> logger,
            Func<QueueUploadResult<EventCreate>, Task> callback) : base(destination, interval, maxSize, logger, callback)
        { }

        /// <summary>
        /// Enqueue an event
        /// </summary>
        /// <param name="evt"></param>
        public override void Enqueue(EventCreate evt)
        {
            base.Enqueue(evt);
            _numberEvents.Inc();
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
                return new QueueUploadResult<EventCreate>(ex);
            }
            return new QueueUploadResult<EventCreate>(items);
        }
    }
}
