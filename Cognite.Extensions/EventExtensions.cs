using Cognite.Extractor.Common;
using CogniteSdk;
using CogniteSdk.Resources;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Prometheus;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.Extensions
{
    /// <summary>
    /// Extension utility methods for CogniteSdk Client.
    /// </summary>
    public static class EventExtensions
    {
        private static ILogger _logger = new NullLogger<Client>();

        internal static void SetLogger(ILogger logger)
        {
            _logger = logger;
        }

        /// <summary>
        /// Get or create the events with the provided <paramref name="externalIds"/> exist in CDF.
        /// If one or more do not exist, use the <paramref name="buildEvents"/> function to construct
        /// the missing event objects and upload them to CDF using the chunking of items and throttling
        /// passed as parameters
        /// </summary>
        /// <param name="resource">Cognite events resource</param>
        /// <param name="externalIds">External Ids</param>
        /// <param name="buildEvents">Async function that builds CogniteSdk EventCreate objects</param>
        /// <param name="chunkSize">Chunk size</param>
        /// <param name="throttleSize">Throttle size</param>
        /// <param name="token">Cancellation token</param>
        /// <returns></returns>
        public static Task<IEnumerable<Event>> GetOrCreateAsync(
            this EventsResource resource,
            IEnumerable<string> externalIds,
            Func<IEnumerable<string>, IEnumerable<EventCreate>> buildEvents,
            int chunkSize,
            int throttleSize,
            CancellationToken token)
        {
            Task<IEnumerable<EventCreate>> asyncBuildEvents(IEnumerable<string> ids)
            {
                return Task.FromResult(buildEvents(ids));
            }
            return resource.GetOrCreateAsync(externalIds, asyncBuildEvents, chunkSize, throttleSize, token);
        }
        /// <summary>
        /// Get or create the events with the provided <paramref name="externalIds"/> exist in CDF.
        /// If one or more do not exist, use the <paramref name="buildEvents"/> function to construct
        /// the missing event objects and upload them to CDF using the chunking of items and throttling
        /// passed as parameters
        /// </summary>
        /// <param name="resource">Cognite events resource</param>
        /// <param name="externalIds">External Ids</param>
        /// <param name="buildEvents">Async function that builds CogniteSdk EventCreate objects</param>
        /// <param name="chunkSize">Chunk size</param>
        /// <param name="throttleSize">Throttle size</param>
        /// <param name="token">Cancellation token</param>
        /// <returns></returns>
        public static async Task<IEnumerable<Event>> GetOrCreateAsync(
            this EventsResource resource,
            IEnumerable<string> externalIds,
            Func<IEnumerable<string>, Task<IEnumerable<EventCreate>>> buildEvents,
            int chunkSize,
            int throttleSize,
            CancellationToken token)
        {
            var result = new List<Event>();
            object mutex = new object();
            var chunks = externalIds
                .ChunkBy(chunkSize)
                .ToList();
            _logger.LogDebug("Getting or creating events. Number of external ids: {Number}. Number of chunks: {Chunks}", externalIds.Count(), chunks.Count());
            var generators = chunks
                .Select<IEnumerable<string>, Func<Task>>(
                    chunk => async () => {
                        var existing = await GetOrCreateEventsChunk(resource, chunk, buildEvents, 0, token);
                        lock (mutex)
                        {
                            result.AddRange(existing);
                        }
                    });

            int taskNum = 0;
            await generators.RunThrottled(
                throttleSize,
                (_) => {
                    if (chunks.Count > 1)
                        _logger.LogDebug("{MethodName} completed {NumDone}/{TotalNum} tasks",
                            nameof(GetOrCreateAsync), ++taskNum, chunks.Count);
                },
                token);
            return result;
        }

        private static async Task<IEnumerable<Event>> GetOrCreateEventsChunk(
            EventsResource resource,
            IEnumerable<string> externalIds,
            Func<IEnumerable<string>, Task<IEnumerable<EventCreate>>> buildEvents,
            int backoff,
            CancellationToken token)
        {
            IEnumerable<Event> found;
            using (CdfMetrics.Events.WithLabels("retrieve").NewTimer())
            {
                found = await resource.RetrieveAsync(externalIds.Select(Identity.Create), true, token);
            }
            _logger.LogDebug("Retrieved {Existing} events from CDF", found.Count());

            var existingEvents = found.ToList();
            var missing = externalIds.Except(existingEvents.Select(evt => evt.ExternalId)).ToList();

            if (!missing.Any())
            {
                return existingEvents;
            }

            _logger.LogDebug("Could not fetch {Missing} out of {Found} events. Attempting to create the missing ones", missing.Count, externalIds.Count());
            try
            {
                var toCreate = await buildEvents(missing);
                if (toCreate.Any())
                {
                    foreach (var evt in toCreate) evt.Sanitize();
                    IEnumerable<Event> newEvents;
                    using (CdfMetrics.Events.WithLabels("create").NewTimer())
                    {
                        newEvents = await resource.CreateAsync(toCreate, token);
                    }
                    existingEvents.AddRange(newEvents);
                    _logger.LogDebug("Created {New} new events in CDF", newEvents.Count());
                }
                return existingEvents;
            }
            catch (ResponseException e) when (e.Code == 409 && e.Duplicated.Any())
            {
                if (backoff > 10) // ~3.5 min total backoff time
                {
                    throw;
                }
                _logger.LogDebug("Found {NumDuplicated} duplicates, during the creation of {NumEvents} events", e.Duplicated.Count(), missing.Count);
            }

            await Task.Delay(TimeSpan.FromSeconds(0.1 * Math.Pow(2, backoff)));
            var ensured = await GetOrCreateEventsChunk(resource, missing, buildEvents, backoff + 1, token);
            existingEvents.AddRange(ensured);

            return existingEvents;
        }
        /// <summary>
        /// Ensures that all events in <paramref name="events"/> exist in CDF.
        /// Tries to create the events and returns when all are created or reported as 
        /// duplicates (already exist in CDF)
        /// </summary>
        /// <param name="resource">Cognite events resource</param>
        /// <param name="events">List of CogniteSdk EventCreate objects</param>
        /// <param name="chunkSize">Chunk size</param>
        /// <param name="throttleSize">Throttle size</param>
        /// <param name="failOnError">Fail if an error other than detected duplicates occurs</param>
        /// <param name="token">Cancellation token</param>
        public static async Task EnsureExistsAsync(
            this EventsResource resource,
            IEnumerable<EventCreate> events,
            int chunkSize,
            int throttleSize,
            bool failOnError,
            CancellationToken token)
        {
            foreach (var evt in events) evt.Sanitize();
            var chunks = events
                .ChunkBy(chunkSize)
                .ToList();

            _logger.LogDebug("Ensuring events. Number of events: {Number}. Number of chunks: {Chunks}", events.Count(), chunks.Count());
            var generators = chunks
                .Select<IEnumerable<EventCreate>, Func<Task>>(
                chunk => async () => {
                    await EnsureChunk(resource, chunk, failOnError, token);
                });

            int taskNum = 0;
            await generators.RunThrottled(
                throttleSize,
                (_) => {
                    if (chunks.Count() > 1)
                        _logger.LogDebug("{MethodName} completed {NumDone}/{TotalNum} tasks",
                            nameof(EnsureExistsAsync), ++taskNum, chunks.Count);
                },
                token);
        }

        private static async Task EnsureChunk(
            EventsResource resource,
            IEnumerable<EventCreate> events,
            bool failOnError,
            CancellationToken token)
        {
            var create = events;
            while (!token.IsCancellationRequested && create.Any())
            {
                try
                {
                    IEnumerable<Event> newEvents;
                    using (CdfMetrics.Events.WithLabels("create").NewTimer())
                    {
                        newEvents = await resource.CreateAsync(create, token);
                    }
                    _logger.LogDebug("Created {New} new events in CDF", newEvents.Count());
                    return;
                }
                catch (ResponseException e) when (e.Code == 409 && e.Duplicated.Any())
                {
                    // Remove duplicates - already exists
                    var duplicated = new HashSet<string>(e.Duplicated
                        .Select(d => d.GetValue("externalId", null))
                        .Where(mv => mv != null)
                        .Select(mv => mv.ToString()));
                    create = events.Where(ts => !duplicated.Contains(ts.ExternalId));
                    await Task.Delay(TimeSpan.FromMilliseconds(100), token);
                    _logger.LogDebug("Found {NumDuplicated} duplicates, during the creation of {NumEvents} events",
                        e.Duplicated.Count(), create.Count());
                    continue;
                }
#pragma warning disable CA1031 // Do not catch general exception types
                catch (Exception e)
                {
                    if (failOnError) throw;
                    _logger.LogWarning("CDF create events failed: {Message} - Retrying in 1 second", e.Message);
                }
#pragma warning restore CA1031 // Do not catch general exception types
                await Task.Delay(TimeSpan.FromSeconds(1), token);
            }
        }
    }
}
