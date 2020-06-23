using Cognite.Extractor.Common;
using Cognite.Extractor.Logging;
using CogniteSdk;
using CogniteSdk.Resources;
using Com.Cognite.V1.Timeseries.Proto;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.Extractor.Utils
{
    /// <summary>
    /// Extension utility methods for <see cref="Client"/>
    /// </summary>
    public static class TimeSeriesExtensions
    {
        private static ILogger _logger = LoggingUtils.GetDefault();

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
        /// <param name="buildTimeSeries">Function that builds <see cref="TimeSeriesCreate"/> objects</param>
        /// <param name="chunkSize">Chunk size</param>
        /// <param name="throttleSize">Throttle size</param>
        /// <param name="token">Cancellation token</param>
        /// <returns></returns>
        public static Task<IEnumerable<TimeSeries>> GetOrCreateTimeSeriesAsync(
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
        /// <param name="buildTimeSeries">Async function that builds <see cref="TimeSeriesCreate"/> objects</param>
        /// <param name="chunkSize">Chunk size</param>
        /// <param name="throttleSize">Throttle size</param>
        /// <param name="token">Cancellation token</param>
        /// <returns></returns>
        public static async Task<IEnumerable<TimeSeries>> GetOrCreateTimeSeriesAsync(
            this TimeSeriesResource timeSeries,
            IEnumerable<string> externalIds,
            Func<IEnumerable<string>, Task<IEnumerable<TimeSeriesCreate>>> buildTimeSeries,
            int chunkSize,
            int throttleSize,
            CancellationToken token)
        {
            var result = new List<TimeSeries>();
            object mutex = new object();

            var chunks = externalIds
                .ChunkBy(chunkSize)
                .ToList();
            _logger.LogDebug("Getting or creating time series. Number of external ids: {Number}. Number of chunks: {Chunks}", externalIds.Count(), chunks.Count());
            var generators = chunks
                .Select<IEnumerable<string>, Func<Task>>(
                    chunk => async () => {
                        var existing = await GetOrCreateTimeSeriesChunk(timeSeries, chunk, buildTimeSeries, 0, token);
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
                            nameof(GetOrCreateTimeSeriesAsync), ++taskNum, chunks.Count);
                },
                token);
            return result;
        }
        /// <summary>
        /// Ensures that all time series in <paramref name="timeSeriesToEnsure"/> exist in CDF.
        /// Tries to create the time series and returns when all are created or reported as 
        /// duplicates (already exist in CDF)
        /// </summary>
        /// <param name="timeSeries">Cognite client</param>
        /// <param name="timeSeriesToEnsure">List of <see cref="TimeSeriesCreate"/> objects</param>
        /// <param name="chunkSize">Chunk size</param>
        /// <param name="throttleSize">Throttle size</param>
        /// <param name="token">Cancellation token</param>
        public static async Task EnsureTimeSeriesExistsAsync(
            this TimeSeriesResource timeSeries,
            IEnumerable<TimeSeriesCreate> timeSeriesToEnsure,
            int chunkSize,
            int throttleSize,
            CancellationToken token)
        {
            var chunks = timeSeriesToEnsure
                .ChunkBy(chunkSize);
            _logger.LogDebug("Ensuring time series. Number of time series: {Number}. Number of chunks: {Chunks}", timeSeriesToEnsure.Count(), chunks.Count());
            var generators = chunks
                .Select<IEnumerable<TimeSeriesCreate>, Func<Task>>(
                chunk => async () => {
                    await EnsureTimeSeriesChunk(timeSeries, chunk, token);
                });

            int taskNum = 0;
            await generators.RunThrottled(
                throttleSize,
                (_) => {
                    if (chunks.Count() > 1)
                        _logger.LogDebug("{MethodName} completed {NumDone}/{TotalNum} tasks",
                            nameof(GetOrCreateTimeSeriesAsync), ++taskNum, chunks.Count());
                },
                token);
        }

        /// <summary>
        /// Get the time series with the provided <paramref name="ids"/>. Ignore any
        /// unknown ids
        /// </summary>
        /// <param name="timeSeries">A <see cref="Client.TimeSeries"/> client</param>
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
                .ChunkBy(chunkSize);
            var generators = chunks
                .Select<IEnumerable<Identity>, Func<Task>>(
                chunk => async () => {
                    var found = await timeSeries.RetrieveAsync(chunk, true, token);
                    lock (mutex)
                    {
                        result.AddRange(found);
                    }
                });
            await generators.RunThrottled(throttleSize, token);
            return result;
        }

        private static async Task<IEnumerable<TimeSeries>> GetOrCreateTimeSeriesChunk(
            TimeSeriesResource client,
            IEnumerable<string> externalIds,
            Func<IEnumerable<string>, Task<IEnumerable<TimeSeriesCreate>>> buildTimeSeries,
            int backoff,
            CancellationToken token)
        {
            var found = await client.RetrieveAsync(externalIds.Select(id => new Identity(id)), true, token);
            _logger.LogDebug("Retrieved {Existing} times series from CDF", found.Count());

            var existingTimeSeries = found.ToList();
            var missing = externalIds.Except(existingTimeSeries.Select(ts => ts.ExternalId)).ToList();

            if (!missing.Any())
            {
                return existingTimeSeries;
            }

            _logger.LogDebug("Could not fetch {Missing} out of {Found} time series. Attempting to create the missing ones", missing.Count, externalIds.Count());
            try
            {
                var newTs = await client.CreateAsync(await buildTimeSeries(missing), token);
                existingTimeSeries.AddRange(newTs);
                _logger.LogDebug("Created {New} new time series in CDF", newTs.Count());
                return existingTimeSeries;
            }
            catch (ResponseException e) when (e.Code == 409 && e.Duplicated.Any())
            {
                if (backoff > 10) // ~3.5 min total backoff time
                {
                    throw;
                }
                _logger.LogDebug("Found {NumDuplicated} duplicates, during the creation of {NumTimeSeries} time series", e.Duplicated.Count(), missing.Count);
            }

            await Task.Delay(TimeSpan.FromSeconds(0.1 * Math.Pow(2, backoff)));
            var ensured = await GetOrCreateTimeSeriesChunk(client, missing, buildTimeSeries, backoff + 1, token);
            existingTimeSeries.AddRange(ensured);

            return existingTimeSeries;
        }

        private static async Task EnsureTimeSeriesChunk(
            this TimeSeriesResource timeSeries,
            IEnumerable<TimeSeriesCreate> timeSeriesToEnsure,
            CancellationToken token)
        {
            var create = timeSeriesToEnsure;
            while (!token.IsCancellationRequested && create.Any())
            {
                try
                {
                    var newTs = await timeSeries.CreateAsync(create, token);
                    _logger.LogDebug("Created {New} new time series in CDF", newTs.Count());
                    return;
                }
                catch (ResponseException e) when (e.Code == 409 && e.Duplicated.Any())
                {
                    // Remove duplicates - already exists
                    // also a case for legacyName...
                    var duplicated = new HashSet<string>(e.Duplicated
                        .Select(d => d.GetValue("externalId", null))
                        .Where(mv => mv != null)
                        .Select(mv => mv.ToString()));
                    create = timeSeriesToEnsure.Where(ts => !duplicated.Contains(ts.ExternalId));
                    await Task.Delay(TimeSpan.FromMilliseconds(100), token);
                    _logger.LogDebug("Found {NumDuplicated} duplicates, during the creation of {NumTimeSeries} time series",
                        e.Duplicated.Count(), create.Count());
                    continue;
                }
#pragma warning disable CA1031 // Do not catch general exception types
                catch (Exception e)
                {
                    _logger.LogWarning("CDF create timeseries failed: {Message} - Retrying in 1 second", e.Message);
                }
#pragma warning restore CA1031 // Do not catch general exception types
                await Task.Delay(TimeSpan.FromSeconds(1), token);
            }
        }
    }
}
