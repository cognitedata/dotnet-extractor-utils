﻿using Cognite.Extensions;
using CogniteSdk;
using Microsoft.Extensions.Logging;
using Oryx.Cognite;
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

        private readonly string? _bufferPath;
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
            Func<QueueUploadResult<EventCreate>, Task>? callback,
            string? bufferPath) : base(destination, interval, maxSize, logger, callback)
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

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Reliability", "CA2007: Do not directly await a Task", Justification = "Awaiter configured by the caller")]
        private async Task WriteToBuffer(IEnumerable<EventCreate> events, CancellationToken token)
        {
            try
            {
                using (var stream = new FileStream(_bufferPath!, FileMode.Append, FileAccess.Write, FileShare.None))
                {
                    await CogniteUtils.WriteEventsAsync(events, stream, token);
                }
                _bufferAny = true;
            }
            catch (Exception ex)
            {
                DestLogger.LogWarning("Failed to write to buffer: {msg}", ex.Message);
            }
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Reliability", "CA2007: Do not directly await a Task", Justification = "Awaiter configured by the caller")]
        private async Task ReadFromBuffer(CancellationToken token)
        {
            IEnumerable<EventCreate> events;
            try
            {
                using (var stream = new FileStream(_bufferPath!, FileMode.OpenOrCreate, FileAccess.Read, FileShare.None))
                {

                    do
                    {
                        // Chunk size is less about CDF chunking, and more about memory usage.
                        // If the queue is offline for a day, and generates a hundred gigabytes of events,
                        // the file could become unreadable.
                        events = await CogniteUtils.ReadEventsAsync(stream, token, 10_000);

                        if (events.Any())
                        {
                            var result = await Destination.EnsureEventsExistsAsync(events, RetryMode.OnError, SanitationMode.Clean, token);

                            DestLogger.LogResult(result, RequestType.CreateEvents, true);

                            var fatalError = result.Errors?.FirstOrDefault(err => err.Type == ErrorType.FatalFailure);
                            if (fatalError != null)
                            {
                                DestLogger.LogWarning("Failed to create items from buffer: {msg}", fatalError.Message);
                                return;
                            }

                            var skipped = result.AllSkipped.ToList();
                            var uploaded = events.Except(skipped);

                            if (Callback != null) await Callback(new QueueUploadResult<EventCreate>(uploaded, skipped));
                        }
                    } while (events.Any());
                }
            }
            catch (Exception ex)
            {
                DestLogger.LogWarning("Failed to read from buffer: {msg}", ex.Message);
                return;
            }
            System.IO.File.Create(_bufferPath!).Close();
            _bufferAny = false;
        }

        /// <summary>
        /// Upload events to CDF.
        /// </summary>
        /// <param name="items">Events to upload</param>
        /// <param name="token"></param>
        /// <returns>An error or the uploaded events</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Reliability", "CA2007: Do not directly await a Task", Justification = "Awaiter configured by the caller")]
        protected override async Task<QueueUploadResult<EventCreate>> UploadEntries(IEnumerable<EventCreate> items, CancellationToken token)
        {
            _queueSize.Dec(items.Count());

            if (!items.Any())
            {
                if (_bufferAny)
                {
                    bool connected;
                    try
                    {
                        await Destination.TestCogniteConfig(token);
                        connected = true;
                    }
                    catch (Exception ex)
                    {
                        DestLogger.LogTrace(ex, "Failed to connect to CDF for inserting events: {msg}", ex.Message);
                        connected = false;
                    }
                    if (connected)
                    {
                        DestLogger.LogTrace("Reconnected to CDF, reading events from buffer");
                        await ReadFromBuffer(token);
                    }
                }
                return new QueueUploadResult<EventCreate>(Enumerable.Empty<EventCreate>(), Enumerable.Empty<EventCreate>());
            }

            DestLogger.LogTrace("Dequeued {Number} events to upload to CDF", items.Count());

            var result = await Destination.EnsureEventsExistsAsync(items, RetryMode.OnError, SanitationMode.Clean, token);

            DestLogger.LogResult(result, RequestType.CreateEvents, true);

            var fatalError = result.Errors?.FirstOrDefault(err => err.Type == ErrorType.FatalFailure);
            if (fatalError != null)
            {
                if (_bufferEnabled)
                {
                    await WriteToBuffer(items, token);
                }
                return new QueueUploadResult<EventCreate>(fatalError.Exception);
            }

            if (_bufferAny)
            {
                await ReadFromBuffer(token);
            }
            var skipped = result.AllSkipped.ToList();
            var uploaded = items.Except(skipped);
            return new QueueUploadResult<EventCreate>(uploaded, skipped);
        }
    }
}
