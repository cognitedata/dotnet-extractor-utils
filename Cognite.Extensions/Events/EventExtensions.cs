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
        /// <param name="sanitationMode">The type of sanitation to apply to events before creating</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>A <see cref="CogniteResult{TResult, TError}"/> containing errors that occured and a list of the created and found events</returns>
        public static Task<CogniteResult<Event, EventCreate>> GetOrCreateAsync(
            this EventsResource resource,
            IEnumerable<string> externalIds,
            Func<IEnumerable<string>, IEnumerable<EventCreate>> buildEvents,
            int chunkSize,
            int throttleSize,
            RetryMode retryMode,
            SanitationMode sanitationMode,
            CancellationToken token)
        {
            Task<IEnumerable<EventCreate>> asyncBuildEvents(IEnumerable<string> ids)
            {
                return Task.FromResult(buildEvents(ids));
            }
            return resource.GetOrCreateAsync(externalIds, asyncBuildEvents, chunkSize, throttleSize, retryMode, sanitationMode, token);
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
        /// <param name="sanitationMode">The type of sanitation to apply to events before creating</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>A <see cref="CogniteResult{TResult, TError}"/> containing errors that occured and a list of the created and found events</returns>
        public static async Task<CogniteResult<Event, EventCreate>> GetOrCreateAsync(
            this EventsResource resource,
            IEnumerable<string> externalIds,
            Func<IEnumerable<string>, Task<IEnumerable<EventCreate>>> buildEvents,
            int chunkSize,
            int throttleSize,
            RetryMode retryMode,
            SanitationMode sanitationMode,
            CancellationToken token)
        {
            var chunks = externalIds
                .ChunkBy(chunkSize)
                .ToList();
            if (!chunks.Any()) return new CogniteResult<Event, EventCreate>(null, null);

            var results = new CogniteResult<Event, EventCreate>[chunks.Count];

            _logger.LogDebug("Getting or creating events. Number of external ids: {Number}. Number of chunks: {Chunks}", externalIds.Count(), chunks.Count);
            var generators = chunks
                .Select<IEnumerable<string>, Func<Task>>(
                    (chunk, idx) => async () => {
                        var result = await GetOrCreateEventsChunk(resource, chunk, buildEvents, 0, retryMode, sanitationMode, token).ConfigureAwait(false);
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
                token).ConfigureAwait(false);

            return CogniteResult<Event, EventCreate>.Merge(results);
        }

        /// <summary>
        /// Ensures that all events in <paramref name="events"/> exist in CDF.
        /// Tries to create the events and returns when all are created or removed
        /// due to a handled error.
        /// If any items fail to be pushed due to missing assetIds, missing dataset, or duplicated externalId
        /// they can be removed before retrying by setting <paramref name="retryMode"/>
        /// Events will be returned in the same order as given in <paramref name="events"/>.
        /// </summary>
        /// <param name="resource">Cognite events resource</param>
        /// <param name="events">List of CogniteSdk EventCreate objects</param>
        /// <param name="chunkSize">Chunk size</param>
        /// <param name="throttleSize">Throttle size</param>
        /// <param name="retryMode">How to do retries. Keeping duplicates is not valid for
        /// this method.</param>
        /// <param name="sanitationMode">The type of sanitation to apply to events before creating</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>A <see cref="CogniteResult{TResult, TError}"/> containing errors that occured and a list of the created events</returns>
        public static async Task<CogniteResult<Event, EventCreate>> EnsureExistsAsync(
            this EventsResource resource,
            IEnumerable<EventCreate> events,
            int chunkSize,
            int throttleSize,
            RetryMode retryMode,
            SanitationMode sanitationMode,
            CancellationToken token)
        {
            IEnumerable<CogniteError<EventCreate>> errors;
            (events, errors) = Sanitation.CleanEventRequest(events, sanitationMode);

            var chunks = events
                .ChunkBy(chunkSize)
                .ToList();

            _logger.LogDebug("Ensuring events. Number of events: {Number}. Number of chunks: {Chunks}", events.Count(), chunks.Count);

            int size = chunks.Count + (errors.Any() ? 1 : 0);
            var results = new CogniteResult<Event, EventCreate>[size];
            if (errors.Any())
            {
                results[size - 1] = new CogniteResult<Event, EventCreate>(errors, null);
                if (size == 1) return results[size - 1];
            }
            if (!results.Any()) return new CogniteResult<Event, EventCreate>(null, null);

            var generators = chunks
                .Select<IEnumerable<EventCreate>, Func<Task>>(
                (chunk, idx) => async () => {
                    var result = await CreateEventsHandleErrors(resource, chunk, retryMode, token).ConfigureAwait(false);
                    results[idx] = result;
                });

            int taskNum = 0;
            await generators.RunThrottled(
                throttleSize,
                (_) => {
                    if (chunks.Count > 1)
                        _logger.LogDebug("{MethodName} completed {NumDone}/{TotalNum} tasks",
                            nameof(EnsureExistsAsync), ++taskNum, chunks.Count);
                },
                token).ConfigureAwait(false);

            return CogniteResult<Event, EventCreate>.Merge(results);
        }

        private static async Task<CogniteResult<Event, EventCreate>> GetOrCreateEventsChunk(
            EventsResource resource,
            IEnumerable<string> externalIds,
            Func<IEnumerable<string>, Task<IEnumerable<EventCreate>>> buildEvents,
            int backoff,
            RetryMode retryMode,
            SanitationMode sanitationMode,
            CancellationToken token)
        {
            IEnumerable<Event> found;
            using (CdfMetrics.Events.WithLabels("retrieve").NewTimer())
            {
                found = await resource.RetrieveAsync(externalIds.Select(Identity.Create), true, token).ConfigureAwait(false);
            }
            _logger.LogDebug("Retrieved {Existing} events from CDF", found.Count());

            var missing = externalIds.Except(found.Select(evt => evt.ExternalId)).ToList();

            if (!missing.Any())
            {
                return new CogniteResult<Event, EventCreate>(null, found);
            }

            _logger.LogDebug("Could not fetch {Missing} out of {Found} events. Attempting to create the missing ones", missing.Count, externalIds.Count());
            var toCreate = await buildEvents(missing).ConfigureAwait(false);

            IEnumerable<CogniteError<EventCreate>> errors;
            (toCreate, errors) = Sanitation.CleanEventRequest(toCreate, sanitationMode);

            var result = await CreateEventsHandleErrors(resource, toCreate, retryMode, token).ConfigureAwait(false);
            result.Results = result.Results == null ? found : result.Results.Concat(found);

            if (errors.Any())
            {
                result.Errors = result.Errors == null ? errors : result.Errors.Concat(errors);
            }

            if (!result.Errors?.Any() ?? false
                || retryMode != RetryMode.OnErrorKeepDuplicates
                && retryMode != RetryMode.OnFatalKeepDuplicates) return result;

            var duplicateErrors = result.Errors.Where(err =>
                err.Resource == ResourceType.ExternalId
                && err.Type == ErrorType.ItemExists)
                .ToList();

            var duplicatedIds = new HashSet<string>();
            if (duplicateErrors.Any())
            {
                foreach (var error in duplicateErrors)
                {
                    if (error.Values == null || !error.Values.Any()) continue;
                    foreach (var idt in error.Values) duplicatedIds.Add(idt.ExternalId);
                }
            }

            if (!duplicatedIds.Any()) return result;
            _logger.LogDebug("Found {cnt} duplicated events, retrying", duplicatedIds.Count);

            await Task
                .Delay(TimeSpan.FromSeconds(0.1 * Math.Pow(2, backoff)), token)
                .ConfigureAwait(false);
            var nextResult = await GetOrCreateEventsChunk(resource, duplicatedIds, buildEvents, backoff + 1, retryMode, sanitationMode, token)
                .ConfigureAwait(false);
            result = result.Merge(nextResult);

            return result;
        }

        private static async Task<CogniteResult<Event, EventCreate>> CreateEventsHandleErrors(
            EventsResource events,
            IEnumerable<EventCreate> toCreate,
            RetryMode retryMode,
            CancellationToken token)
        {
            var errors = new List<CogniteError<EventCreate>>();
            while (toCreate != null && toCreate.Any() && !token.IsCancellationRequested)
            {
                try
                {
                    IEnumerable<Event> newEvents;
                    using (CdfMetrics.Events.WithLabels("create").NewTimer())
                    {
                        newEvents = await events.CreateAsync(toCreate, token).ConfigureAwait(false);
                    }

                    _logger.LogDebug("Created {New} new events in CDF", newEvents.Count());
                    return new CogniteResult<Event, EventCreate>(errors, newEvents);
                }
                catch (Exception ex)
                {
                    _logger.LogDebug("Failed to create {cnt} events: {msg}",
                        toCreate.Count(), ex.Message);
                    var error = ResultHandlers.ParseException<EventCreate>(ex, RequestType.CreateEvents);
                    errors.Add(error);
                    if (error.Type == ErrorType.FatalFailure
                        && (retryMode == RetryMode.OnFatal
                            || retryMode == RetryMode.OnFatalKeepDuplicates))
                    {
                        await Task.Delay(1000, token).ConfigureAwait(false);
                    }
                    else if (retryMode == RetryMode.None) break;
                    else
                    {
                        toCreate = ResultHandlers.CleanFromError(error, toCreate);
                    }
                }
            }
            return new CogniteResult<Event, EventCreate>(errors, null);
        }
    }
}
