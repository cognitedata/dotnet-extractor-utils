using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extractor.Common;
using CogniteSdk;
using CogniteSdk.DataModels;
using CogniteSdk.DataModels.Core;
using CogniteSdk.Resources;
using CogniteSdk.Resources.DataModels;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Prometheus;

namespace Cognite.Extensions.DataModels.CogniteExtractorExtensions
{
    /// <summary>
    /// Extension utility methods for <see cref="Client"/>
    /// </summary>
    public static class ExtractorTimeSeriesExtensions
    {
        private static ILogger _logger = new NullLogger<Client>();

        internal static void SetLogger(ILogger logger)
        {
            _logger = logger;
        }

        /// <summary>
        /// Get or create the time series with the provided <paramref name="instanceIds"/> if they exist in CDF.
        /// If one or more do not exist, use the <paramref name="buildTimeSeries"/> function to construct
        /// the missing time series objects and upload them to CDF using the chunking of items and throttling
        /// passed as parameters
        /// If any items fail to be created due to missing asset, duplicated externalId, duplicated
        /// legacy name, or missing dataSetId, they can be removed before retrying by setting <paramref name="retryMode"/>
        /// </summary>
        /// <param name="timeSeries">Cognite timeseries resource</param>
        /// <param name="instanceIds">Instance Ids</param>
        /// <param name="buildTimeSeries">Function that builds CogniteSdk SourcedNodeWrite objects</param>
        /// <param name="chunkSize">Chunk size</param>
        /// <param name="throttleSize">Throttle size</param>
        /// <param name="retryMode">How to handle failed requests</param>
        /// <param name="sanitationMode">The type of sanitation to apply to timeseries before creating</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>A <see cref="CogniteResult{TResult, TError}"/> containing errors that occured and a list of the created and found timeseries</returns>
        public static Task<CogniteResult<SourcedNode<T>, SourcedNodeWrite<T>>> GetOrCreateTimeSeriesAsync<T>(
            this CoreTimeSeriesResource<T> timeSeries,
            IEnumerable<InstanceIdentifier> instanceIds,
            Func<IEnumerable<InstanceIdentifier>, IEnumerable<SourcedNodeWrite<T>>> buildTimeSeries,
            int chunkSize,
            int throttleSize,
            RetryMode retryMode,
            SanitationMode sanitationMode,
            CancellationToken token) where T : CogniteExtractorTimeSeries
        {
            Task<IEnumerable<SourcedNodeWrite<T>>> asyncBuildTimeSeries(IEnumerable<InstanceIdentifier> ids)
            {
                return Task.FromResult(buildTimeSeries(ids));
            }
            return timeSeries.GetOrCreateTimeSeriesAsync<T>(instanceIds, asyncBuildTimeSeries,
                chunkSize, throttleSize, retryMode, sanitationMode, token);
        }

        /// <summary>
        /// Get or create the time series with the provided <paramref name="instanceIds"/> if they exist in CDF.
        /// If one or more do not exist, use the <paramref name="buildTimeSeries"/> function to construct
        /// the missing time series objects and upload them to CDF using the chunking of items and throttling
        /// passed as parameters
        /// If any items fail to be created due to missing asset, duplicated externalId, duplicated
        /// legacy name, or missing dataSetId, they can be removed before retrying by setting <paramref name="retryMode"/>
        /// </summary>
        /// <param name="timeSeries">Cognite client</param>
        /// <param name="instanceIds">External Ids</param>
        /// <param name="buildTimeSeries">Async function that builds CogniteSdk SourcedNodeWrite objects</param>
        /// <param name="chunkSize">Chunk size</param>
        /// <param name="throttleSize">Throttle size</param>
        /// <param name="retryMode">How to handle failed requests</param>
        /// <param name="sanitationMode">The type of sanitation to apply to timeseries before creating</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>A <see cref="CogniteResult{TResult, TError}"/> containing errors that occured and a list of the created and found timeseries</returns>
        public static async Task<CogniteResult<SourcedNode<T>, SourcedNodeWrite<T>>> GetOrCreateTimeSeriesAsync<T>(
            this CoreTimeSeriesResource<T> timeSeries,
            IEnumerable<InstanceIdentifier> instanceIds,
            Func<IEnumerable<InstanceIdentifier>, Task<IEnumerable<SourcedNodeWrite<T>>>> buildTimeSeries,
            int chunkSize,
            int throttleSize,
            RetryMode retryMode,
            SanitationMode sanitationMode,
            CancellationToken token) where T : CogniteExtractorTimeSeries
        {
            var chunks = instanceIds
                .ChunkBy(chunkSize)
                .ToList();
            if (!chunks.Any()) return new CogniteResult<SourcedNode<T>, SourcedNodeWrite<T>>(null, null);

            var results = new CogniteResult<SourcedNode<T>, SourcedNodeWrite<T>>[chunks.Count];

            _logger.LogDebug("Getting or creating time series. Number of external ids: {Number}. Number of chunks: {Chunks}", instanceIds.Count(), chunks.Count);
            var generators = chunks
                .Select<IEnumerable<InstanceIdentifier>, Func<Task>>(
                    (chunk, idx) => async () =>
                    {
                        var result = await GetOrCreateTimeSeriesChunk(timeSeries, chunk,
                            buildTimeSeries, 0, retryMode, sanitationMode, token).ConfigureAwait(false);
                        results[idx] = result;
                    });

            int taskNum = 0;
            await generators.RunThrottled(
                throttleSize,
                (_) =>
                {
                    if (chunks.Count > 1)
                        _logger.LogDebug("{MethodName} completed {NumDone}/{TotalNum} tasks",
                            nameof(GetOrCreateTimeSeriesAsync), ++taskNum, chunks.Count);
                },
                token).ConfigureAwait(false);

            return CogniteResult<SourcedNode<T>, SourcedNodeWrite<T>>.Merge(results);
        }

        /// <summary>
        /// Ensures that all time series in <paramref name="timeSeriesToEnsure"/> exists in CDF.
        /// Tries to create the time series and returns when all are created or have been removed
        /// due to issues with the request.
        /// If any items fail to be created due to missing asset, duplicated externalId, duplicated
        /// legacy name, or missing dataSetId, they can be removed before retrying by setting <paramref name="retryMode"/>
        /// Timeseries will be returned in the same order as given in <paramref name="timeSeriesToEnsure"/>
        /// </summary>
        /// <param name="timeSeries">Cognite client</param>
        /// <param name="timeSeriesToEnsure">List of CogniteSdk SourcedNodeWrite objects</param>
        /// <param name="chunkSize">Chunk size</param>
        /// <param name="throttleSize">Throttle size</param>
        /// <param name="retryMode">How to do retries. Keeping duplicates is not valid for
        /// this method.</param>
        /// <param name="sanitationMode">The type of sanitation to apply to timeseries before creating</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>A <see cref="CogniteResult{TResult, TError}"/> containing errors that occured and a list of the created timeseries</returns>
        public static async Task<CogniteResult<SourcedNode<T>, SourcedNodeWrite<T>>> EnsureTimeSeriesExistsAsync<T>(
            this CoreTimeSeriesResource<T> timeSeries,
            IEnumerable<SourcedNodeWrite<T>> timeSeriesToEnsure,
            int chunkSize,
            int throttleSize,
            RetryMode retryMode,
            SanitationMode sanitationMode,
            CancellationToken token) where T : CogniteTimeSeriesBase
        {
            IEnumerable<CogniteError<SourcedNodeWrite<T>>> errors;
            (timeSeriesToEnsure, errors) = CoreTSSanitation.CleanTimeSeriesRequest(timeSeriesToEnsure, sanitationMode);

            var chunks = timeSeriesToEnsure
                .ChunkBy(chunkSize)
                .ToList();

            int size = chunks.Count + (errors.Any() ? 1 : 0);
            var results = new CogniteResult<SourcedNode<T>, SourcedNodeWrite<T>>[size];

            if (errors.Any())
            {
                results[size - 1] = new CogniteResult<SourcedNode<T>, SourcedNodeWrite<T>>(errors, null);
                if (size == 1) return results[size - 1];
            }
            if (size == 0) return new CogniteResult<SourcedNode<T>, SourcedNodeWrite<T>>(null, null);

            _logger.LogDebug("Ensuring time series. Number of time series: {Number}. Number of chunks: {Chunks}", timeSeriesToEnsure.Count(), chunks.Count);
            var generators = chunks
                .Select<IEnumerable<SourcedNodeWrite<T>>, Func<Task>>(
                (chunk, idx) => async () =>
                {
                    var result = await CreateTimeSeriesHandleErrors(timeSeries, chunk, retryMode, token).ConfigureAwait(false);
                    results[idx] = result;
                });

            int taskNum = 0;
            await generators.RunThrottled(
                throttleSize,
                (_) =>
                {
                    if (chunks.Count > 1)
                        _logger.LogDebug("{MethodName} completed {NumDone}/{TotalNum} tasks",
                            nameof(EnsureTimeSeriesExistsAsync), ++taskNum, chunks.Count);
                },
                token).ConfigureAwait(false);

            return CogniteResult<SourcedNode<T>, SourcedNodeWrite<T>>.Merge(results);
        }

        /// <summary>
        /// Get the time series with the provided <paramref name="ids"/>. Ignore any
        /// unknown ids
        /// </summary>
        /// <param name="timeSeries">A CogniteSdk TimeSeries resource</param>
        /// <param name="ids">List of <see cref="Identity"/> objects</param>
        /// <param name="chunkSize">Chunk size</param>
        /// <param name="throttleSize">Throttle size</param>
        /// <param name="token">Cancellation token</param>
        /// <returns></returns>
        public static async Task<IEnumerable<SourcedNode<T>>> GetTimeSeriesByIdsIgnoreErrors<T>(
            this CoreTimeSeriesResource<T> timeSeries,
            IEnumerable<Identity> ids,
            int chunkSize,
            int throttleSize,
            CancellationToken token) where T : CogniteTimeSeriesBase
        {
            var result = new List<SourcedNode<T>>();
            object mutex = new object();

            var chunks = ids
                .ChunkBy(chunkSize)
                .ToList();

            var generators = chunks
                .Select(
                (Func<IEnumerable<Identity>, Func<Task>>)(                chunk => async () =>
                {
                    IEnumerable<SourcedInstance<T>> found;
                    using (CdfMetrics.CoreTimeSeries.WithLabels("retrieve").NewTimer())
                    {
                        found = await timeSeries.RetrieveAsync(chunk.Select(x => new InstanceIdentifierWithType(InstanceType.node, x.InstanceId)), token).ConfigureAwait(false);
                    }
                    lock (mutex)
                    {
                        result.AddRange(found.Select(x => new SourcedNode<T>(x)));
                    }
                }));
            int numTasks = 0;
            await generators.RunThrottled(throttleSize, (_) =>
                _logger.LogDebug("{MethodName} completed {NumDone}/{TotalNum} tasks", nameof(GetTimeSeriesByIdsIgnoreErrors), ++numTasks, chunks.Count),
                token).ConfigureAwait(false);
            return result;
        }

        private static async Task<CogniteResult<SourcedNode<T>, SourcedNodeWrite<T>>> GetOrCreateTimeSeriesChunk<T>(
            CoreTimeSeriesResource<T> client,
            IEnumerable<InstanceIdentifier> instanceIds,
            Func<IEnumerable<InstanceIdentifier>, Task<IEnumerable<SourcedNodeWrite<T>>>> buildTimeSeries,
            int backoff,
            RetryMode retryMode,
            SanitationMode sanitationMode,
            CancellationToken token) where T : CogniteExtractorTimeSeries
        {
            IEnumerable<SourcedInstance<T>> found;
            using (CdfMetrics.CoreTimeSeries.WithLabels("retrieve").NewTimer())
            {
                var idts = instanceIds;
                try
                {
                    found = await client.RetrieveAsync(idts.Select(x => new InstanceIdentifierWithType(InstanceType.node, x)), token).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    var err = ResultHandlers.ParseSimpleError<SourcedNodeWrite<T>>(ex, idts?.Select(x => Identity.Create(x)), null);
                    return new CogniteResult<SourcedNode<T>, SourcedNodeWrite<T>>(new[] { err }, null);
                }
            }
            _logger.LogDebug("Retrieved {Existing} times series from CDF", found.Count());

            var missing = instanceIds.Except(found.Select(ts => new InstanceIdentifier(ts.Space, ts.ExternalId))).ToList();

            if (missing.Count == 0)
            {
                return new CogniteResult<SourcedNode<T>, SourcedNodeWrite<T>>(null, found.Select(x => new SourcedNode<T>(x)));
            }

            _logger.LogDebug("Could not fetch {Missing} out of {Found} time series. Attempting to create the missing ones", missing.Count, instanceIds.Count());
            var toCreate = await buildTimeSeries(missing).ConfigureAwait(false);

            IEnumerable<CogniteError<SourcedNodeWrite<T>>> errors;
            (toCreate, errors) = CoreTSSanitation.CleanTimeSeriesRequest(toCreate, sanitationMode);

            var result = await CreateTimeSeriesHandleErrors<T>(client, toCreate, retryMode, token).ConfigureAwait(false);
            result.Results = (result.Results == null ? found : result.Results.Concat(found)).Select(x => new SourcedNode<T>(x));

            if (errors.Any())
            {
                result.Errors = result.Errors == null ? errors : result.Errors.Concat(errors);
            }

            if (!result.Errors?.Any() ?? false
                || retryMode != RetryMode.OnErrorKeepDuplicates
                && retryMode != RetryMode.OnFatalKeepDuplicates) return result;

            var duplicateErrors = (result.Errors ?? Enumerable.Empty<CogniteError>()).Where(err =>
                err.Resource == ResourceType.ExternalId
                && err.Type == ErrorType.ItemExists)
                .ToList();

            var duplicatedIds = new HashSet<InstanceIdentifier>();
            if (duplicateErrors.Any())
            {
                foreach (var error in duplicateErrors)
                {
                    if (error.Values == null || !error.Values.Any()) continue;
                    foreach (var idt in error.Values) duplicatedIds.Add(idt.InstanceId);
                }
            }

            if (!duplicatedIds.Any()) return result;
            _logger.LogDebug("Found {cnt} duplicated timeseries, retrying", duplicatedIds.Count);

            await Task.Delay(TimeSpan.FromSeconds(0.1 * Math.Pow(2, backoff)), token).ConfigureAwait(false);
            var nextResult = await GetOrCreateTimeSeriesChunk(client, duplicatedIds,
                buildTimeSeries, backoff + 1, retryMode, sanitationMode, token)
                .ConfigureAwait(false);
            result = result.Merge(nextResult);

            return result;
        }

        private static async Task<CogniteResult<SourcedNode<T>, SourcedNodeWrite<T>>> CreateTimeSeriesHandleErrors<T>(
            CoreTimeSeriesResource<T> timeseries,
            IEnumerable<SourcedNodeWrite<T>> toCreate,
            RetryMode retryMode,
            CancellationToken token) where T : CogniteTimeSeriesBase
        {
            var errors = new List<CogniteError<SourcedNodeWrite<T>>>();
            while (toCreate != null && toCreate.Any() && !token.IsCancellationRequested)
            {
                try
                {
                    IEnumerable<SlimInstance> newTimeseries;
                    using (CdfMetrics.CoreTimeSeries.WithLabels("create").NewTimer())
                    {
                        newTimeseries = await timeseries.UpsertAsync(toCreate, null, token).ConfigureAwait(false);
                    }

                    _logger.LogDebug("Created {New} new timeseries in CDF", newTimeseries.Count());
                    var toCreateDict = new Dictionary<Identity, T>();
                    foreach (var cr in toCreate)
                    {
                        toCreateDict[new Identity(new InstanceIdentifier(cr.Space, cr.ExternalId))] = cr.Properties;
                    }

                    return new CogniteResult<SourcedNode<T>, SourcedNodeWrite<T>>(errors, newTimeseries.Select(x => new SourcedNode<T>(x, toCreateDict[new Identity(new InstanceIdentifier(x.Space, x.ExternalId))])));
                }
                catch (Exception ex)
                {
                    _logger.LogDebug("Failed to create {cnt} timeseries: {msg}",
                        toCreate.Count(), ex.Message);
                    var error = ResultHandlers.ParseException<SourcedNodeWrite<T>>(ex, RequestType.CreateTimeSeries);
                    if (error.Type == ErrorType.FatalFailure
                        && (retryMode == RetryMode.OnFatal
                            || retryMode == RetryMode.OnFatalKeepDuplicates))
                    {
                        await Task.Delay(1000, token).ConfigureAwait(false);
                    }
                    else if (retryMode == RetryMode.None)
                    {
                        errors.Add(error);
                        break;
                    }
                    else
                    {
                        errors.Add(error);
                        toCreate = ResultHandlers.CleanFromError(error, toCreate);
                    }
                }
            }
            return new CogniteResult<SourcedNode<T>, SourcedNodeWrite<T>>(errors, null);
        }

        /// <summary>
        /// Upsert time series.
        /// If any items fail to be created due to duplicated instance ids, they can be removed before retrying by setting <paramref name="retryMode"/>
        /// Timeseries will be returned in the same order as given in <paramref name="items"/>
        /// </summary>
        /// <param name="resource">CogniteSdk time series resource</param>
        /// <param name="items">List of timeseries updates</param>
        /// <param name="chunkSize">Maximum number of timeseries per request</param>
        /// <param name="throttleSize">Maximum number of parallel requests</param>
        /// <param name="retryMode">How to handle retries</param>
        /// <param name="sanitationMode">What kind of pre-request sanitation to perform</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>A <see cref="CogniteResult{TResult, TError}"/> containing errors that occured and a list of the updated timeseries</returns>
        public static async Task<CogniteResult<SlimInstance, SourcedNodeWrite<T>>> UpsertAsync<T>(
            this CoreTimeSeriesResource<T> resource,
            IEnumerable<SourcedNodeWrite<T>> items,
            int chunkSize,
            int throttleSize,
            RetryMode retryMode,
            SanitationMode sanitationMode,
            CancellationToken token) where T : CogniteTimeSeriesBase
        {
            IEnumerable<CogniteError<SourcedNodeWrite<T>>> errors;
            (items, errors) = CoreTSSanitation.CleanTimeSeriesRequest<T>(items, sanitationMode);

            var chunks = items
                .ChunkBy(chunkSize)
                .ToList();

            int size = chunks.Count + (errors.Any() ? 1 : 0);
            var results = new CogniteResult<SlimInstance, SourcedNodeWrite<T>>[size];

            if (errors.Any())
            {
                results[size - 1] = new CogniteResult<SlimInstance, SourcedNodeWrite<T>>(errors, null);
                if (size == 1) return results[size - 1];
            }
            if (size == 0) return new CogniteResult<SlimInstance, SourcedNodeWrite<T>>(null, null);

            _logger.LogDebug("Updating time series. Number of time series: {Number}. Number of chunks: {Chunks}", items.Count(), chunks.Count);
            var generators = chunks
                .Select<IEnumerable<SourcedNodeWrite<T>>, Func<Task>>(
                (chunk, idx) => async () =>
                {
                    var result = await UpsertTimeSeriesHandleErrors(resource, chunk, retryMode, token).ConfigureAwait(false);
                    results[idx] = result;
                });

            int taskNum = 0;
            await generators.RunThrottled(
                throttleSize,
                (_) =>
                {
                    if (chunks.Count > 1)
                        _logger.LogDebug("{MethodName} completed {NumDone}/{TotalNum} tasks",
                            nameof(UpsertAsync), ++taskNum, chunks.Count);
                },
                token).ConfigureAwait(false);

            return CogniteResult<SlimInstance, SourcedNodeWrite<T>>.Merge(results);
        }

        private static async Task<CogniteResult<SlimInstance, SourcedNodeWrite<T>>> UpsertTimeSeriesHandleErrors<T>(
            CoreTimeSeriesResource<T> timeseries,
            IEnumerable<SourcedNodeWrite<T>> items,
            RetryMode retryMode,
            CancellationToken token) where T : CogniteTimeSeriesBase
        {
            var errors = new List<CogniteError<SourcedNodeWrite<T>>>();
            while (items != null && items.Any() && !token.IsCancellationRequested)
            {
                try
                {
                    var toUpdate = new List<SourcedNodeWrite<CogniteExtractorTimeSeries>>();

                    IEnumerable<SlimInstance> updated;
                    using (CdfMetrics.CoreTimeSeries.WithLabels("update").NewTimer())
                    {
                        updated = await timeseries.UpsertAsync(items, null, token).ConfigureAwait(false);
                    }

                    _logger.LogDebug("Updated {Count} timeseries in CDF", updated.Count());
                    return new CogniteResult<SlimInstance, SourcedNodeWrite<T>>(errors, updated);
                }
                catch (Exception ex)
                {
                    _logger.LogDebug("Failed to create {Count} timeseries: {Message}",
                        items.Count(), ex.Message);
                    var error = ResultHandlers.ParseException<SourcedNodeWrite<T>>(ex, RequestType.UpdateTimeSeries);
                    if (error.Type == ErrorType.FatalFailure
                        && (retryMode == RetryMode.OnFatal
                            || retryMode == RetryMode.OnFatalKeepDuplicates))
                    {
                        await Task.Delay(1000, token).ConfigureAwait(false);
                    }
                    else if (retryMode == RetryMode.None)
                    {
                        errors.Add(error);
                        break;
                    }
                    else
                    {
                        errors.Add(error);
                        items = ResultHandlers.CleanFromError(error, items);
                    }
                }
            }

            return new CogniteResult<SlimInstance, SourcedNodeWrite<T>>(errors, null);
        }
    }
}