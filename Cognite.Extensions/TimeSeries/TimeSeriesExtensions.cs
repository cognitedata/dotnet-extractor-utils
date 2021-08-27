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
        /// <returns>A <see cref="CogniteResult"/> containing errors that occured and a list of the created and found timeseries</returns>
        public static Task<CogniteResult<TimeSeries>> GetOrCreateTimeSeriesAsync(
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
        /// <returns>A <see cref="CogniteResult"/> containing errors that occured and a list of the created and found timeseries</returns>
        public static async Task<CogniteResult<TimeSeries>> GetOrCreateTimeSeriesAsync(
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
            if (!chunks.Any()) return new CogniteResult<TimeSeries>(null, null);

            var results = new CogniteResult<TimeSeries>[chunks.Count];

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

            return CogniteResult<TimeSeries>.Merge(results);
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
        /// <returns>A <see cref="CogniteResult"/> containing errors that occured and a list of the created timeseries</returns>
        public static async Task<CogniteResult<TimeSeries>> EnsureTimeSeriesExistsAsync(
            this TimeSeriesResource timeseries,
            IEnumerable<TimeSeriesCreate> timeSeriesToEnsure,
            int chunkSize,
            int throttleSize,
            RetryMode retryMode,
            SanitationMode sanitationMode,
            CancellationToken token)
        {
            IEnumerable<CogniteError> errors;
            (timeSeriesToEnsure, errors) = Sanitation.CleanTimeSeriesRequest(timeSeriesToEnsure, sanitationMode);

            var chunks = timeSeriesToEnsure
                .ChunkBy(chunkSize)
                .ToList();

            int size = chunks.Count + (errors.Any() ? 1 : 0);
            var results = new CogniteResult<TimeSeries>[size];

            if (errors.Any())
            {
                results[size - 1] = new CogniteResult<TimeSeries>(errors, null);
                if (size == 1) return results[size - 1];
            }
            if (size == 0) return new CogniteResult<TimeSeries>(null, null);

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

            return CogniteResult<TimeSeries>.Merge(results);
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

        private static async Task<CogniteResult<TimeSeries>> GetOrCreateTimeSeriesChunk(
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
                return new CogniteResult<TimeSeries>(null, found);
            }

            _logger.LogDebug("Could not fetch {Missing} out of {Found} time series. Attempting to create the missing ones", missing.Count, externalIds.Count());
            var toCreate = await buildTimeSeries(missing).ConfigureAwait(false);

            IEnumerable<CogniteError> errors;
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
                    if (!error.Values?.Any() ?? false) continue;
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

        private static async Task<CogniteResult<TimeSeries>> CreateTimeSeriesHandleErrors(
            TimeSeriesResource timeseries,
            IEnumerable<TimeSeriesCreate> toCreate,
            RetryMode retryMode,
            CancellationToken token)
        {
            var errors = new List<CogniteError>();
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
                    return new CogniteResult<TimeSeries>(errors, newTimeseries);
                }
                catch (Exception ex)
                {
                    _logger.LogDebug("Failed to create {cnt} timeseries: {msg}",
                        toCreate.Count(), ex.Message);
                    var error = ResultHandlers.ParseException(ex, RequestType.CreateTimeSeries);
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
            return new CogniteResult<TimeSeries>(errors, null);
        }
    }
}
