using Cognite.Extractor.Common;
using Cognite.Extractor.Logging;
using CogniteSdk;
using CogniteSdk.Resources;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ExtractorUtils.Cognite
{
    public static class EventExtensions
    {
        private static ILogger _logger = LoggingUtils.GetDefault();

        internal static void SetLogger(ILogger logger)
        {
            _logger = logger;
        }

        public static async Task<IEnumerable<Event>> GetOrCreateEventsAsync(
            this EventsResource client,
            IEnumerable<string> externalIds,
            Func<IEnumerable<string>, Task<IEnumerable<EventCreate>>> buildEvents,
            int chunkSize,
            int throttleSize,
            CancellationToken token)
        {
            var result = new List<Event>();
            var chunks = externalIds
                .ChunkBy(chunkSize)
                .ToList();
            _logger.LogDebug("Getting or creating events. Number of external ids: {Number}. Number of chunks: {Chunks}", externalIds.Count(), chunks.Count());
            var generators = chunks
                .Select<IEnumerable<string>, Func<Task>>(
                    chunk => async () => {
                        var existing = await GetOrCreateEventsChunk(client, chunk, buildEvents, 0, token);
                        result.AddRange(existing);
                    });

            int taskNum = 0;
            await generators.RunThrottled(
                throttleSize,
                (_) => {
                    if (chunks.Count > 1)
                        _logger.LogDebug("{MethodName} completed {NumDone}/{TotalNum} tasks",
                            nameof(GetOrCreateEventsAsync), ++taskNum, chunks.Count);
                },
                token);
            return result;
        }

        private static async Task<IEnumerable<Event>> GetOrCreateEventsChunk(
            EventsResource client,
            IEnumerable<string> externalIds,
            Func<IEnumerable<string>, Task<IEnumerable<EventCreate>>> buildEvents,
            int backoff,
            CancellationToken token)
        {
            var found = await client.RetrieveAsync(externalIds.Select(Identity.Create), true, token);
            _logger.LogDebug("Retrieved {Existing} events from CDF", found.Count());

            var existingEvents = found.ToList();
            var missing = externalIds.Except(existingEvents.Select(asset => asset.ExternalId)).ToList();

            if (!missing.Any())
            {
                return existingEvents;
            }

            _logger.LogDebug("Could not fetch {Missing} out of {Found} events. Attempting to create the missing ones", missing.Count, externalIds.Count());
            try
            {
                var newAssets = await client.CreateAsync(await buildEvents(missing), token);
                existingEvents.AddRange(newAssets);
                _logger.LogDebug("Created {New} new assets in CDF", newAssets.Count());
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
            var ensured = await GetOrCreateEventsChunk(client, missing, buildEvents, backoff + 1, token);
            existingEvents.AddRange(ensured);

            return existingEvents;
        }
    }
}
