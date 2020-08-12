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
        /// </summary>
        /// <param name="timeSeries">Cognite timeseries resource</param>
        /// <param name="externalIds">External Ids</param>
        /// <param name="buildTimeSeries">Function that builds CogniteSdk TimeSeriesCreate objects</param>
        /// <param name="chunkSize">Chunk size</param>
        /// <param name="throttleSize">Throttle size</param>
        /// <param name="token">Cancellation token</param>
        /// <returns></returns>
        public static Task<CogniteResult<TimeSeries>> GetOrCreateTimeSeriesAsync(
            this TimeSeriesResource timeSeries,
            IEnumerable<string> externalIds,
            Func<IEnumerable<string>, IEnumerable<TimeSeriesCreate>> buildTimeSeries,
            int chunkSize,
            int throttleSize,
            CancellationToken token)
        {
            Task<IEnumerable<TimeSeriesCreate>> asyncBuildTimeSeries(IEnumerable<string> ids)
            {
                return Task.FromResult(buildTimeSeries(ids));
            }
            return timeSeries.GetOrCreateTimeSeriesAsync(externalIds, asyncBuildTimeSeries, chunkSize, throttleSize, token);
        }

        /// <summary>
        /// Get or create the time series with the provided <paramref name="externalIds"/> exist in CDF.
        /// If one or more do not exist, use the <paramref name="buildTimeSeries"/> function to construct
        /// the missing time series objects and upload them to CDF using the chunking of items and throttling
        /// passed as parameters
        /// </summary>
        /// <param name="timeSeries">Cognite client</param>
        /// <param name="externalIds">External Ids</param>
        /// <param name="buildTimeSeries">Async function that builds CogniteSdk TimeSeriesCreate objects</param>
        /// <param name="chunkSize">Chunk size</param>
        /// <param name="throttleSize">Throttle size</param>
        /// <param name="token">Cancellation token</param>
        /// <returns></returns>
        public static async Task<CogniteResult<TimeSeries>> GetOrCreateTimeSeriesAsync(
            this TimeSeriesResource timeSeries,
            IEnumerable<string> externalIds,
            Func<IEnumerable<string>, Task<IEnumerable<TimeSeriesCreate>>> buildTimeSeries,
            int chunkSize,
            int throttleSize,
            CancellationToken token)
        {
            var results = new List<CogniteResult<TimeSeries>>();
            object mutex = new object();

            var chunks = externalIds
                .ChunkBy(chunkSize)
                .ToList();

            _logger.LogDebug("Getting or creating time series. Number of external ids: {Number}. Number of chunks: {Chunks}", externalIds.Count(), chunks.Count());
            var generators = chunks
                .Select<IEnumerable<string>, Func<Task>>(
                    chunk => async () => {
                        var result = await GetOrCreateTimeSeriesChunk(timeSeries, chunk, buildTimeSeries, 0, token);
                        lock (mutex) results.Add(result);
                    });

            int taskNum = 0;
            await generators.RunThrottled(
                throttleSize,
                (_) => {
                    if (chunks.Count > 1)
                        _logger.LogDebug("{MethodName} completed {NumDone}/{TotalNum} tasks",
                            nameof(GetOrCreateTimeSeriesAsync), ++taskNum, chunks.Count);
                },
                token);
            if (!results.Any()) return new CogniteResult<TimeSeries>(null, null);
            return results.Aggregate((seed, res) => seed.Merge(res));
        }
        /// <summary>
        /// Ensures that all time series in <paramref name="timeSeriesToEnsure"/> exist in CDF.
        /// Tries to create the time series and returns when all are created or reported as 
        /// duplicates (already exist in CDF)
        /// </summary>
        /// <param name="timeseries">Cognite client</param>
        /// <param name="timeSeriesToEnsure">List of CogniteSdk TimeSeriesCreate objects</param>
        /// <param name="chunkSize">Chunk size</param>
        /// <param name="throttleSize">Throttle size</param>
        /// <param name="failOnError">If true, return if a fatal error occurs</param>
        /// <param name="token">Cancellation token</param>
        public static async Task<CogniteResult> EnsureTimeSeriesExistsAsync(
            this TimeSeriesResource timeseries,
            IEnumerable<TimeSeriesCreate> timeSeriesToEnsure,
            int chunkSize,
            int throttleSize,
            bool failOnError,
            CancellationToken token)
        {
            CogniteError idError, nameError;
            (timeSeriesToEnsure, idError, nameError) = Sanitation.CleanTimeSeriesRequest(timeSeriesToEnsure);

            var chunks = timeSeriesToEnsure
                .ChunkBy(chunkSize)
                .ToList();

            var results = new List<CogniteResult>();
            object mutex = new object();

            if (idError != null || nameError != null)
            {
                var errors = new List<CogniteError>();
                if (idError != null) errors.Add(idError);
                if (nameError != null) errors.Add(nameError);
                results.Add(new CogniteResult(errors));
            }

            _logger.LogDebug("Ensuring time series. Number of time series: {Number}. Number of chunks: {Chunks}", timeSeriesToEnsure.Count(), chunks.Count());
            var generators = chunks
                .Select<IEnumerable<TimeSeriesCreate>, Func<Task>>(
                chunk => async () => {
                    var result = await CreateTimeSeriesHandleErrors(timeseries, timeSeriesToEnsure, failOnError, token);
                    lock (mutex) results.Add(result);
                });

            int taskNum = 0;
            await generators.RunThrottled(
                throttleSize,
                (_) => {
                    if (chunks.Count() > 1)
                        _logger.LogDebug("{MethodName} completed {NumDone}/{TotalNum} tasks",
                            nameof(EnsureTimeSeriesExistsAsync), ++taskNum, chunks.Count);
                },
                token);
            if (!results.Any()) return new CogniteResult(null);
            return results.Aggregate((seed, res) => seed.Merge(res));
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
                        found = await timeSeries.RetrieveAsync(chunk, true, token);
                    }
                    lock (mutex)
                    {
                        result.AddRange(found);
                    }
                });
            int numTasks = 0;
            await generators.RunThrottled(throttleSize, (_) =>
                _logger.LogDebug("{MethodName} completed {NumDone}/{TotalNum} tasks", nameof(GetTimeSeriesByIdsIgnoreErrors), ++numTasks, chunks.Count),
                token);
            return result;
        }

        private static async Task<CogniteResult<TimeSeries>> GetOrCreateTimeSeriesChunk(
            TimeSeriesResource client,
            IEnumerable<string> externalIds,
            Func<IEnumerable<string>, Task<IEnumerable<TimeSeriesCreate>>> buildTimeSeries,
            int backoff,
            CancellationToken token)
        {
            IEnumerable<TimeSeries> found;
            using (CdfMetrics.TimeSeries.WithLabels("retrieve").NewTimer())
            {
                found = await client.RetrieveAsync(externalIds.Select(id => new Identity(id)), true, token);
            }
            _logger.LogDebug("Retrieved {Existing} times series from CDF", found.Count());

            var missing = externalIds.Except(found.Select(ts => ts.ExternalId)).ToList();

            if (!missing.Any())
            {
                return new CogniteResult<TimeSeries>(null, found);
            }

            _logger.LogDebug("Could not fetch {Missing} out of {Found} time series. Attempting to create the missing ones", missing.Count, externalIds.Count());
            var toCreate = await buildTimeSeries(missing);

            CogniteError idError, nameError;
            (toCreate, idError, nameError) = Sanitation.CleanTimeSeriesRequest(toCreate);

            var result = await CreateTimeSeriesHandleErrors(client, toCreate, true, token);
            result.Results = result.Results == null ? found : result.Results.Concat(found);

            if (idError != null || nameError != null)
            {
                var errors = new List<CogniteError>();
                if (result.Errors != null) errors.AddRange(result.Errors);
                if (idError != null) errors.Add(idError);
                if (nameError != null) errors.Add(nameError);
                result.Errors = errors;
            }

            if (!result.Errors?.Any() ?? false) return result;

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

            await Task.Delay(TimeSpan.FromSeconds(0.1 * Math.Pow(2, backoff)));
            var nextResult = await GetOrCreateTimeSeriesChunk(client, duplicatedIds, buildTimeSeries, backoff + 1, token);
            result = result.Merge(nextResult);

            return result;
        }

        private static async Task<CogniteResult<TimeSeries>> CreateTimeSeriesHandleErrors(
            TimeSeriesResource timeseries,
            IEnumerable<TimeSeriesCreate> toCreate,
            bool failOnError,
            CancellationToken token)
        {
            var errors = new List<CogniteError>();
            while (toCreate != null && toCreate.Any() && !token.IsCancellationRequested)
            {
                try
                {
                    IEnumerable<TimeSeries> newTimeseries;
                    using (CdfMetrics.TimeSeries.WithLabels("create"))
                    {
                        newTimeseries = await timeseries.CreateAsync(toCreate, token);
                    }

                    _logger.LogDebug("Created {New} new timeseries in CDF", newTimeseries.Count());
                    return new CogniteResult<TimeSeries>(errors, newTimeseries);
                }
                catch (Exception ex)
                {
                    _logger.LogDebug("Failed to create {cnt} timeseries: {msg}",
                        toCreate.Count(), ex.Message);
                    var error = ResultHandlers.ParseException(ex, RequestType.CreateTimeSeries);
                    toCreate = ResultHandlers.CleanFromError(error, toCreate, failOnError);
                    if (error.Type == ErrorType.FatalFailure && !failOnError)
                    {
                        await Task.Delay(1000);
                    }
                    errors.Add(error);
                }
            }
            return new CogniteResult<TimeSeries>(errors, null);
        }
    }
}
