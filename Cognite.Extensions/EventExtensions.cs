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
        /// If any items fail to be pushed due to missing assetIds, missing dataset, or duplicated externalId
        /// they can be removed before retrying by setting <paramref name="retryMode"/>
        /// </summary>
        /// <param name="resource">Cognite events resource</param>
        /// <param name="externalIds">External Ids</param>
        /// <param name="buildEvents">Async function that builds CogniteSdk EventCreate objects</param>
        /// <param name="chunkSize">Chunk size</param>
        /// <param name="throttleSize">Throttle size</param>
        /// <param name="retryMode">How to handle failed requests</param>
        /// <param name="token">Cancellation token</param>
        /// <returns></returns>
        public static Task<CogniteResult<Event>> GetOrCreateAsync(
            this EventsResource resource,
            IEnumerable<string> externalIds,
            Func<IEnumerable<string>, IEnumerable<EventCreate>> buildEvents,
            int chunkSize,
            int throttleSize,
            RetryMode retryMode,
            CancellationToken token)
        {
            Task<IEnumerable<EventCreate>> asyncBuildEvents(IEnumerable<string> ids)
            {
                return Task.FromResult(buildEvents(ids));
            }
            return resource.GetOrCreateAsync(externalIds, asyncBuildEvents, chunkSize, throttleSize, retryMode, token);
        }
        /// <summary>
        /// Get or create the events with the provided <paramref name="externalIds"/> exist in CDF.
        /// If one or more do not exist, use the <paramref name="buildEvents"/> function to construct
        /// the missing event objects and upload them to CDF using the chunking of items and throttling
        /// passed as parameters.
        /// If any items fail to be pushed due to missing assetIds, missing dataset, or duplicated externalId
        /// they can be removed before retrying by setting <paramref name="retryMode"/>
        /// </summary>
        /// <param name="resource">Cognite events resource</param>
        /// <param name="externalIds">External Ids</param>
        /// <param name="buildEvents">Async function that builds CogniteSdk EventCreate objects</param>
        /// <param name="chunkSize">Chunk size</param>
        /// <param name="throttleSize">Throttle size</param>
        /// <param name="retryMode">How to handle failed requests</param>
        /// <param name="token">Cancellation token</param>
        /// <returns></returns>
        public static async Task<CogniteResult<Event>> GetOrCreateAsync(
            this EventsResource resource,
            IEnumerable<string> externalIds,
            Func<IEnumerable<string>, Task<IEnumerable<EventCreate>>> buildEvents,
            int chunkSize,
            int throttleSize,
            RetryMode retryMode,
            CancellationToken token)
        {
            var chunks = externalIds
                .ChunkBy(chunkSize)
                .ToList();
            if (!chunks.Any()) return new CogniteResult<Event>(null, null);

            var results = new CogniteResult<Event>[chunks.Count];

            _logger.LogDebug("Getting or creating events. Number of external ids: {Number}. Number of chunks: {Chunks}", externalIds.Count(), chunks.Count());
            var generators = chunks
                .Select<IEnumerable<string>, Func<Task>>(
                    (chunk, idx) => async () => {
                        var result = await GetOrCreateEventsChunk(resource, chunk, buildEvents, 0, retryMode, token);
                        results[idx] = result;
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

            return CogniteResult<Event>.Merge(results);
        }

        /// <summary>
        /// Ensures that all events in <paramref name="events"/> exist in CDF.
        /// Tries to create the events and returns when all are created or removed
        /// due to a handled error.
        /// If any items fail to be pushed due to missing assetIds, missing dataset, or duplicated externalId
        /// they can be removed before retrying by setting <paramref name="retryMode"/>
        /// </summary>
        /// <param name="resource">Cognite events resource</param>
        /// <param name="events">List of CogniteSdk EventCreate objects</param>
        /// <param name="chunkSize">Chunk size</param>
        /// <param name="throttleSize">Throttle size</param>
        /// <param name="retryMode">How to do retries. Keeping duplicates is not valid for
        /// this method.</param>
        /// <param name="token">Cancellation token</param>
        public static async Task<CogniteResult> EnsureExistsAsync(
            this EventsResource resource,
            IEnumerable<EventCreate> events,
            int chunkSize,
            int throttleSize,
            RetryMode retryMode,
            CancellationToken token)
        {
            CogniteError prePushError;
            (events, prePushError) = Sanitation.CleanEventRequest(events);

            var chunks = events
                .ChunkBy(chunkSize)
                .ToList();

            _logger.LogDebug("Ensuring events. Number of events: {Number}. Number of chunks: {Chunks}", events.Count(), chunks.Count());

            int size = chunks.Count + (prePushError != null ? 1 : 0);
            var results = new CogniteResult[size];
            if (prePushError != null)
            {
                results[size - 1] = new CogniteResult(new[] { prePushError });
                if (size == 1) return results[size - 1];
            }
            if (!results.Any()) return new CogniteResult(null);

            var generators = chunks
                .Select<IEnumerable<EventCreate>, Func<Task>>(
                (chunk, idx) => async () => {
                    var result = await CreateEventsHandleErrors(resource, events, retryMode, token);
                    results[idx] = result;
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

            return CogniteResult.Merge(results);
        }

        private static async Task<CogniteResult<Event>> GetOrCreateEventsChunk(
            EventsResource resource,
            IEnumerable<string> externalIds,
            Func<IEnumerable<string>, Task<IEnumerable<EventCreate>>> buildEvents,
            int backoff,
            RetryMode retryMode,
            CancellationToken token)
        {
            IEnumerable<Event> found;
            using (CdfMetrics.Events.WithLabels("retrieve").NewTimer())
            {
                found = await resource.RetrieveAsync(externalIds.Select(Identity.Create), true, token);
            }
            _logger.LogDebug("Retrieved {Existing} events from CDF", found.Count());

            var missing = externalIds.Except(found.Select(evt => evt.ExternalId)).ToList();

            if (!missing.Any())
            {
                return new CogniteResult<Event>(null, found);
            }

            _logger.LogDebug("Could not fetch {Missing} out of {Found} events. Attempting to create the missing ones", missing.Count, externalIds.Count());
            var toCreate = await buildEvents(missing);

            CogniteError prePushError;
            (toCreate, prePushError) = Sanitation.CleanEventRequest(toCreate);

            var result = await CreateEventsHandleErrors(resource, toCreate, retryMode, token);
            result.Results = result.Results == null ? found : result.Results.Concat(found);

            if (prePushError != null)
            {
                result.Errors = result.Errors == null ? new[] { prePushError } : result.Errors.Append(prePushError);
            }

            if (!result.Errors?.Any() ?? false || ((int)retryMode & 1) != 0) return result;

            var duplicateErrors = result.Errors.Where(err =>
                err.Resource == ResourceType.ExternalId
                && err.Type == ErrorType.ItemExists)
                .ToList();

            var duplicatedIds = new HashSet<string>();
            if (duplicateErrors.Any())
            {
                foreach (var error in duplicateErrors)
                {
                    if (!error.Values?.Any() ?? false) continue;
                    foreach (var idt in error.Values) duplicatedIds.Add(idt.ExternalId);
                }
            }

            if (!duplicatedIds.Any()) return result;
            _logger.LogDebug("Found {cnt} duplicated events, retrying", duplicatedIds.Count);

            await Task.Delay(TimeSpan.FromSeconds(0.1 * Math.Pow(2, backoff)));
            var nextResult = await GetOrCreateEventsChunk(resource, duplicatedIds, buildEvents, backoff + 1, retryMode, token);
            result = result.Merge(nextResult);

            return result;
        }

        private static async Task<CogniteResult<Event>> CreateEventsHandleErrors(
            EventsResource events,
            IEnumerable<EventCreate> toCreate,
            RetryMode retryMode,
            CancellationToken token)
        {
            var errors = new List<CogniteError>();
            while (toCreate != null && toCreate.Any() && !token.IsCancellationRequested)
            {
                try
                {
                    IEnumerable<Event> newEvents;
                    using (CdfMetrics.Events.WithLabels("create").NewTimer())
                    {
                        newEvents = await events.CreateAsync(toCreate, token);
                    }

                    _logger.LogDebug("Created {New} new events in CDF", newEvents.Count());
                    return new CogniteResult<Event>(errors, newEvents);
                }
                catch (Exception ex)
                {
                    _logger.LogDebug("Failed to create {cnt} events: {msg}",
                        toCreate.Count(), ex.Message);
                    var error = ResultHandlers.ParseException(ex, RequestType.CreateEvents);
                    errors.Add(error);
                    if (error.Type == ErrorType.FatalFailure && ((int)retryMode & 4) != 0)
                    {
                        await Task.Delay(1000);
                    }
                    else if (((int)retryMode & 2) == 0) break;
                    else
                    {
                        toCreate = ResultHandlers.CleanFromError(error, toCreate);
                    }
                }
            }
            return new CogniteResult<Event>(errors, null);
        }
    }
}
