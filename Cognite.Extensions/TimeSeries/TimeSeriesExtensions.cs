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
    /// Extension utility methods for <see cref="Client"/>
    /// </summary>
    public static class TimeSeriesExtensions
    {
        private static ILogger _logger = new NullLogger<Client>();

        internal static void SetLogger(ILogger logger)
        {
            _logger = logger;
        }

        /// <summary>
        /// Get or create the time series with the provided <paramref name="externalIds"/> exist in CDF.
        /// If one or more do not exist, use the <paramref name="buildTimeSeries"/> function to construct
        /// the missing time series objects and upload them to CDF using the chunking of items and throttling
        /// passed as parameters
        /// If any items fail to be created due to missing asset, duplicated externalId, duplicated
        /// legacy name, or missing dataSetId, they can be removed before retrying by setting <paramref name="retryMode"/>
        /// </summary>
        /// <param name="timeSeries">Cognite timeseries resource</param>
        /// <param name="externalIds">External Ids</param>
        /// <param name="buildTimeSeries">Function that builds CogniteSdk TimeSeriesCreate objects</param>
        /// <param name="chunkSize">Chunk size</param>
        /// <param name="throttleSize">Throttle size</param>
        /// <param name="retryMode">How to handle failed requests</param>
        /// <param name="sanitationMode">The type of sanitation to apply to timeseries before creating</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>A <see cref="CogniteResult{TResult, TError}"/> containing errors that occured and a list of the created and found timeseries</returns>
        public static Task<CogniteResult<TimeSeries, TimeSeriesCreate>> GetOrCreateTimeSeriesAsync(
            this TimeSeriesResource timeSeries,
            IEnumerable<string> externalIds,
            Func<IEnumerable<string>, IEnumerable<TimeSeriesCreate>> buildTimeSeries,
            int chunkSize,
            int throttleSize,
            RetryMode retryMode,
            SanitationMode sanitationMode,
            CancellationToken token)
        {
            Task<IEnumerable<TimeSeriesCreate>> asyncBuildTimeSeries(IEnumerable<string> ids)
            {
                return Task.FromResult(buildTimeSeries(ids));
            }
            return timeSeries.GetOrCreateTimeSeriesAsync(externalIds, asyncBuildTimeSeries,
                chunkSize, throttleSize, retryMode, sanitationMode, token);
        }

        /// <summary>
        /// Get or create the time series with the provided <paramref name="externalIds"/> exist in CDF.
        /// If one or more do not exist, use the <paramref name="buildTimeSeries"/> function to construct
        /// the missing time series objects and upload them to CDF using the chunking of items and throttling
        /// passed as parameters
        /// If any items fail to be created due to missing asset, duplicated externalId, duplicated
        /// legacy name, or missing dataSetId, they can be removed before retrying by setting <paramref name="retryMode"/>
        /// </summary>
        /// <param name="timeSeries">Cognite client</param>
        /// <param name="externalIds">External Ids</param>
        /// <param name="buildTimeSeries">Async function that builds CogniteSdk TimeSeriesCreate objects</param>
        /// <param name="chunkSize">Chunk size</param>
        /// <param name="throttleSize">Throttle size</param>
        /// <param name="retryMode">How to handle failed requests</param>
        /// <param name="sanitationMode">The type of sanitation to apply to timeseries before creating</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>A <see cref="CogniteResult{TResult, TError}"/> containing errors that occured and a list of the created and found timeseries</returns>
        public static async Task<CogniteResult<TimeSeries, TimeSeriesCreate>> GetOrCreateTimeSeriesAsync(
            this TimeSeriesResource timeSeries,
            IEnumerable<string> externalIds,
            Func<IEnumerable<string>, Task<IEnumerable<TimeSeriesCreate>>> buildTimeSeries,
            int chunkSize,
            int throttleSize,
            RetryMode retryMode,
            SanitationMode sanitationMode,
            CancellationToken token)
        {
            var chunks = externalIds
                .ChunkBy(chunkSize)
                .ToList();
            if (!chunks.Any()) return new CogniteResult<TimeSeries, TimeSeriesCreate>(null, null);

            var results = new CogniteResult<TimeSeries, TimeSeriesCreate>[chunks.Count];

            _logger.LogDebug("Getting or creating time series. Number of external ids: {Number}. Number of chunks: {Chunks}", externalIds.Count(), chunks.Count);
            var generators = chunks
                .Select<IEnumerable<string>, Func<Task>>(
                    (chunk, idx) => async () => {
                        var result = await GetOrCreateTimeSeriesChunk(timeSeries, chunk,
                            buildTimeSeries, 0, retryMode, sanitationMode, token).ConfigureAwait(false);
                        results[idx] = result;
                    });

            int taskNum = 0;
            await generators.RunThrottled(
                throttleSize,
                (_) => {
                    if (chunks.Count > 1)
                        _logger.LogDebug("{MethodName} completed {NumDone}/{TotalNum} tasks",
                            nameof(GetOrCreateTimeSeriesAsync), ++taskNum, chunks.Count);
                },
                token).ConfigureAwait(false);

            return CogniteResult<TimeSeries, TimeSeriesCreate>.Merge(results);
        }
        /// <summary>
        /// Ensures that all time series in <paramref name="timeSeriesToEnsure"/> exist in CDF.
        /// Tries to create the time series and returns when all are created or have been removed
        /// due to issues with the request.
        /// If any items fail to be created due to missing asset, duplicated externalId, duplicated
        /// legacy name, or missing dataSetId, they can be removed before retrying by setting <paramref name="retryMode"/>
        /// Timeseries will be returned in the same order as given in <paramref name="timeSeriesToEnsure"/>
        /// </summary>
        /// <param name="timeseries">Cognite client</param>
        /// <param name="timeSeriesToEnsure">List of CogniteSdk TimeSeriesCreate objects</param>
        /// <param name="chunkSize">Chunk size</param>
        /// <param name="throttleSize">Throttle size</param>
        /// <param name="retryMode">How to do retries. Keeping duplicates is not valid for
        /// this method.</param>
        /// <param name="sanitationMode">The type of sanitation to apply to timeseries before creating</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>A <see cref="CogniteResult{TResult, TError}"/> containing errors that occured and a list of the created timeseries</returns>
        public static async Task<CogniteResult<TimeSeries, TimeSeriesCreate>> EnsureTimeSeriesExistsAsync(
            this TimeSeriesResource timeseries,
            IEnumerable<TimeSeriesCreate> timeSeriesToEnsure,
            int chunkSize,
            int throttleSize,
            RetryMode retryMode,
            SanitationMode sanitationMode,
            CancellationToken token)
        {
            IEnumerable<CogniteError<TimeSeriesCreate>> errors;
            (timeSeriesToEnsure, errors) = Sanitation.CleanTimeSeriesRequest(timeSeriesToEnsure, sanitationMode);

            var chunks = timeSeriesToEnsure
                .ChunkBy(chunkSize)
                .ToList();

            int size = chunks.Count + (errors.Any() ? 1 : 0);
            var results = new CogniteResult<TimeSeries, TimeSeriesCreate>[size];

            if (errors.Any())
            {
                results[size - 1] = new CogniteResult<TimeSeries, TimeSeriesCreate>(errors, null);
                if (size == 1) return results[size - 1];
            }
            if (size == 0) return new CogniteResult<TimeSeries, TimeSeriesCreate>(null, null);

            _logger.LogDebug("Ensuring time series. Number of time series: {Number}. Number of chunks: {Chunks}", timeSeriesToEnsure.Count(), chunks.Count);
            var generators = chunks
                .Select<IEnumerable<TimeSeriesCreate>, Func<Task>>(
                (chunk, idx) => async () => {
                    var result = await CreateTimeSeriesHandleErrors(timeseries, chunk, retryMode, token).ConfigureAwait(false);
                    results[idx] = result;
                });

            int taskNum = 0;
            await generators.RunThrottled(
                throttleSize,
                (_) => {
                    if (chunks.Count > 1)
                        _logger.LogDebug("{MethodName} completed {NumDone}/{TotalNum} tasks",
                            nameof(EnsureTimeSeriesExistsAsync), ++taskNum, chunks.Count);
                },
                token).ConfigureAwait(false);

            return CogniteResult<TimeSeries, TimeSeriesCreate>.Merge(results);
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
        public static async Task<IEnumerable<TimeSeries>> GetTimeSeriesByIdsIgnoreErrors(
            this TimeSeriesResource timeSeries,
            IEnumerable<Identity> ids,
            int chunkSize,
            int throttleSize,
            CancellationToken token)
        {
            var result = new List<TimeSeries>();
            object mutex = new object();

            var chunks = ids
                .ChunkBy(chunkSize)
                .ToList();

            var generators = chunks
                .Select<IEnumerable<Identity>, Func<Task>>(
                chunk => async () => {
                    IEnumerable<TimeSeries> found;
                    using (CdfMetrics.TimeSeries.WithLabels("retrieve").NewTimer())
                    {
                        found = await timeSeries.RetrieveAsync(chunk, true, token).ConfigureAwait(false);
                    }
                    lock (mutex)
                    {
                        result.AddRange(found);
                    }
                });
            int numTasks = 0;
            await generators.RunThrottled(throttleSize, (_) =>
                _logger.LogDebug("{MethodName} completed {NumDone}/{TotalNum} tasks", nameof(GetTimeSeriesByIdsIgnoreErrors), ++numTasks, chunks.Count),
                token).ConfigureAwait(false);
            return result;
        }

        private static async Task<CogniteResult<TimeSeries, TimeSeriesCreate>> GetOrCreateTimeSeriesChunk(
            TimeSeriesResource client,
            IEnumerable<string> externalIds,
            Func<IEnumerable<string>, Task<IEnumerable<TimeSeriesCreate>>> buildTimeSeries,
            int backoff,
            RetryMode retryMode,
            SanitationMode sanitationMode,
            CancellationToken token)
        {
            IEnumerable<TimeSeries> found;
            using (CdfMetrics.TimeSeries.WithLabels("retrieve").NewTimer())
            {
                found = await client.RetrieveAsync(externalIds.Select(id => new Identity(id)), true, token).ConfigureAwait(false);
            }
            _logger.LogDebug("Retrieved {Existing} times series from CDF", found.Count());

            var missing = externalIds.Except(found.Select(ts => ts.ExternalId)).ToList();

            if (!missing.Any())
            {
                return new CogniteResult<TimeSeries, TimeSeriesCreate>(null, found);
            }

            _logger.LogDebug("Could not fetch {Missing} out of {Found} time series. Attempting to create the missing ones", missing.Count, externalIds.Count());
            var toCreate = await buildTimeSeries(missing).ConfigureAwait(false);

            IEnumerable<CogniteError<TimeSeriesCreate>> errors;
            (toCreate, errors) = Sanitation.CleanTimeSeriesRequest(toCreate, sanitationMode);

            var result = await CreateTimeSeriesHandleErrors(client, toCreate, retryMode, token).ConfigureAwait(false);
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
            _logger.LogDebug("Found {cnt} duplicated timeseries, retrying", duplicatedIds.Count);

            await Task.Delay(TimeSpan.FromSeconds(0.1 * Math.Pow(2, backoff)), token).ConfigureAwait(false);
            var nextResult = await GetOrCreateTimeSeriesChunk(client, duplicatedIds,
                buildTimeSeries, backoff + 1, retryMode, sanitationMode, token)
                .ConfigureAwait(false);
            result = result.Merge(nextResult);

            return result;
        }

        private static async Task<CogniteResult<TimeSeries, TimeSeriesCreate>> CreateTimeSeriesHandleErrors(
            TimeSeriesResource timeseries,
            IEnumerable<TimeSeriesCreate> toCreate,
            RetryMode retryMode,
            CancellationToken token)
        {
            var errors = new List<CogniteError<TimeSeriesCreate>>();
            while (toCreate != null && toCreate.Any() && !token.IsCancellationRequested)
            {
                try
                {
                    IEnumerable<TimeSeries> newTimeseries;
                    using (CdfMetrics.TimeSeries.WithLabels("create").NewTimer())
                    {
                        newTimeseries = await timeseries.CreateAsync(toCreate, token).ConfigureAwait(false);
                    }

                    _logger.LogDebug("Created {New} new timeseries in CDF", newTimeseries.Count());
                    return new CogniteResult<TimeSeries, TimeSeriesCreate>(errors, newTimeseries);
                }
                catch (Exception ex)
                {
                    _logger.LogDebug("Failed to create {cnt} timeseries: {msg}",
                        toCreate.Count(), ex.Message);
                    var error = ResultHandlers.ParseException<TimeSeriesCreate>(ex, RequestType.CreateTimeSeries);
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
            return new CogniteResult<TimeSeries, TimeSeriesCreate>(errors, null);
        }

        /// <summary>
        /// Update time series.
        /// If any items fail to be created due to missing asset, duplicated externalId, missing id,
        /// or missing dataSetId, they can be removed before retrying by setting <paramref name="retryMode"/>
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
        public static async Task<CogniteResult<TimeSeries, TimeSeriesUpdateItem>> UpdateAsync(
            this TimeSeriesResource resource,
            IEnumerable<TimeSeriesUpdateItem> items,
            int chunkSize,
            int throttleSize,
            RetryMode retryMode,
            SanitationMode sanitationMode,
            CancellationToken token)
        {
            IEnumerable<CogniteError<TimeSeriesUpdateItem>> errors;
            (items, errors) = Sanitation.CleanTimeSeriesUpdateRequest(items, sanitationMode);

            var chunks = items
                .ChunkBy(chunkSize)
                .ToList();

            int size = chunks.Count + (errors.Any() ? 1 : 0);
            var results = new CogniteResult<TimeSeries, TimeSeriesUpdateItem>[size];

            if (errors.Any())
            {
                results[size - 1] = new CogniteResult<TimeSeries, TimeSeriesUpdateItem>(errors, null);
                if (size == 1) return results[size - 1];
            }
            if (size == 0) return new CogniteResult<TimeSeries, TimeSeriesUpdateItem>(null, null);

            _logger.LogDebug("Updating time series. Number of time series: {Number}. Number of chunks: {Chunks}", items.Count(), chunks.Count);
            var generators = chunks
                .Select<IEnumerable<TimeSeriesUpdateItem>, Func<Task>>(
                (chunk, idx) => async () => {
                    var result = await UpdateTimeSeriesHandleErrors(resource, chunk, retryMode, token).ConfigureAwait(false);
                    results[idx] = result;
                });

            int taskNum = 0;
            await generators.RunThrottled(
                throttleSize,
                (_) => {
                    if (chunks.Count > 1)
                        _logger.LogDebug("{MethodName} completed {NumDone}/{TotalNum} tasks",
                            nameof(UpdateAsync), ++taskNum, chunks.Count);
                },
                token).ConfigureAwait(false);

            return CogniteResult<TimeSeries, TimeSeriesUpdateItem>.Merge(results);
        }


        private static async Task<CogniteResult<TimeSeries, TimeSeriesUpdateItem>> UpdateTimeSeriesHandleErrors(
            TimeSeriesResource timeseries,
            IEnumerable<TimeSeriesUpdateItem> items,
            RetryMode retryMode,
            CancellationToken token)
        {
            var errors = new List<CogniteError<TimeSeriesUpdateItem>>();
            while (items != null && items.Any() && !token.IsCancellationRequested)
            {
                try
                {
                    IEnumerable<TimeSeries> updated;
                    using (CdfMetrics.TimeSeries.WithLabels("update").NewTimer())
                    {
                        updated = await timeseries.UpdateAsync(items, token).ConfigureAwait(false);
                    }

                    _logger.LogDebug("Updated {Count} timeseries in CDF", updated.Count());
                    return new CogniteResult<TimeSeries, TimeSeriesUpdateItem>(errors, updated);
                }
                catch (Exception ex)
                {
                    _logger.LogDebug("Failed to create {Count} timeseries: {Message}",
                        items.Count(), ex.Message);
                    var error = ResultHandlers.ParseException<TimeSeriesUpdateItem>(ex, RequestType.UpdateTimeSeries);
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
                        items = ResultHandlers.CleanFromError(error, items);
                    }
                }
            }

            return new CogniteResult<TimeSeries, TimeSeriesUpdateItem>(errors, null);
        }

        /// <summary>
        /// Insert or update a list of timeseries, handling errors that come up during both insert and update.
        /// Only timeseries that differ from timeseries in CDF are updated.
        /// 
        /// All given timeseries must have an external id, so it is not in practice possible to use this to change
        /// the externalId of timeseries.
        /// 
        /// Timeseries are returned in the same order as given.
        /// </summary>
        /// <param name="tss">TimeSeries resource</param>
        /// <param name="upserts">Assets to upsert</param>
        /// <param name="chunkSize">Number of asset creates, retrieves or updates per request</param>
        /// <param name="throttleSize">Maximum number of parallel requests</param>
        /// <param name="retryMode">How to handle retries on errors</param>
        /// <param name="sanitationMode">How to sanitize creates and updates</param>
        /// <param name="options">How to update existing assets</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>Result with failed creates/updates and list of assets</returns>
        /// <exception cref="ArgumentException">All upserted assets must have external id</exception>
        public static async Task<CogniteResult<TimeSeries, TimeSeriesCreate>> UpsertAsync(
            this TimeSeriesResource tss,
            IEnumerable<TimeSeriesCreate> upserts,
            int chunkSize,
            int throttleSize,
            RetryMode retryMode,
            SanitationMode sanitationMode,
            UpsertParams? options,
            CancellationToken token)
        {
            if (!upserts.All(a => a.ExternalId != null)) throw new ArgumentException("All inserts must have externalId");

            var assetDict = upserts.ToDictionary(ts => ts.ExternalId);

            var createResult = await tss.GetOrCreateTimeSeriesAsync(
                assetDict.Keys,
                keys => keys.Select(key => assetDict[key]),
                chunkSize, throttleSize,
                retryMode,
                sanitationMode,
                token).ConfigureAwait(false);

            if (createResult.Errors?.Any() ?? false)
            {
                var badTimeSeries = new HashSet<TimeSeriesCreate>(createResult.Errors.Where(e => e.Skipped != null).SelectMany(e => e.Skipped));
                assetDict = assetDict.Where(kvp => !badTimeSeries.Contains(kvp.Value)).ToDictionary(kvp => kvp.Key, kvp => kvp.Value);
            }

            if (!assetDict.Any() || createResult.Results == null || !createResult.Results.Any()) return createResult;

            var resultDict = createResult.Results.ToDictionary(ts => ts.ExternalId);
            var updates = assetDict.Values
                .Select(ts => (ts.ToUpdate(resultDict[ts.ExternalId], options)!, ts))
                .Where(upd => upd.Item1 != null)
                .ToDictionary(upd => upd.Item1.Id!.Value);

            var updateResult = await tss.UpdateAsync(
                updates.Values.Select(pair => pair.Item1),
                chunkSize,
                throttleSize,
                retryMode,
                sanitationMode,
                token).ConfigureAwait(false);

            var merged = updateResult.Replace(upd => updates[upd.Id!.Value].ts).Merge(createResult);

            var resultTimeSeries = createResult.Results;

            if (updateResult.Results != null && updateResult.Results.Any())
            {
                var updated = new HashSet<long>(updateResult.Results.Select(ts => ts.Id));
                var finalResultDict = resultTimeSeries
                    .Where(ts => !updated.Contains(ts.Id))
                    .Union(updateResult.Results)
                    .ToDictionary(ts => ts.ExternalId);

                // To maintain the order as given we have to do this mapping if updates have been made.
                resultTimeSeries = upserts
                    .Where(ts => finalResultDict.ContainsKey(ts.ExternalId))
                    .Select(ts => finalResultDict[ts.ExternalId])
                    .ToList();
                merged.Results = resultTimeSeries;
            }

            return merged;
        }
    }
}
