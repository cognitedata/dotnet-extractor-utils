using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using CogniteSdk;
using CogniteSdk.Resources;
using Com.Cognite.V1.Timeseries.Proto;
using Microsoft.Extensions.Logging;
using Polly.Timeout;
using Prometheus;
using Cognite.Extractor.Common;
using Cognite.Extractor.Logging;

namespace Cognite.Extractor.Utils
{
    /// <summary>
    /// Extension utility methods for <see cref="Client"/>
    /// </summary>
    public static class CogniteClientExtensions
    {
        private static ILogger _logger = LoggingUtils.GetDefault();

        private static readonly Counter _numberDataPoints = Prometheus.Metrics.CreateCounter("extractor_utils_cdf_datapoints", null);
        internal static void SetLogger(ILogger logger) {
            _logger = logger;
        }

        /// <summary>
        /// Verifies that the <paramref name="client"/> configured according to <paramref name="config"/>
        /// can access Cognite Data Fusion
        /// </summary>
        /// <param name="client">Cognite SDK client</param>
        /// <param name="config">Configuration object</param>
        /// <param name="token">Cancellation token</param>
        /// <exception cref="CogniteUtilsException">Thrown when credentials are invalid
        /// or the client cannot be used to access CDF resources</exception>
        public async static Task TestCogniteConfig(this Client client, CogniteConfig config, CancellationToken token)
        {
            if (config == null) {
                throw new CogniteUtilsException("Cognite configuration missing");
            }
            
            if (config?.Project?.TrimToNull() == null)
            {
                throw new CogniteUtilsException("CDF project is not configured");
            }

            var loginStatus = await client.Login.StatusAsync(token);
            if (!loginStatus.LoggedIn)
            {
                throw new CogniteUtilsException("CDF credentials are invalid");
            }
            if (!loginStatus.Project.Equals(config.Project))
            {
                throw new CogniteUtilsException($"CDF credentials are not associated with project {config.Project}");
            }
        }

        /// <summary>
        /// Get or create the time series with the provided <paramref name="externalIds"/> exist in CDF.
        /// If one or more do not exist, use the <paramref name="buildTimeSeries"/> function to construct
        /// the missing time series objects and upload them to CDF using the chunking of items and throttling
        /// passed as parameters
        /// </summary>
        /// <param name="client">Cognite client</param>
        /// <param name="externalIds">External Ids</param>
        /// <param name="buildTimeSeries">Function that builds <see cref="TimeSeriesCreate"/> objects</param>
        /// <param name="chunkSize">Chunk size</param>
        /// <param name="throttleSize">Throttle size</param>
        /// <param name="token">Cancellation token</param>
        /// <returns></returns>
        public static async Task<IEnumerable<TimeSeries>> GetOrCreateTimeSeriesAsync(
            this Client client,
            IEnumerable<string> externalIds,
            Func<IEnumerable<string>, IEnumerable<TimeSeriesCreate>> buildTimeSeries,
            int chunkSize,
            int throttleSize,
            CancellationToken token)
        {
            var result = new List<TimeSeries>();
            var chunks = externalIds
                .ChunkBy(chunkSize);
            _logger.LogDebug("Getting or creating time series. Number of external ids: {Number}. Number of chunks: {Chunks}", externalIds.Count(), chunks.Count());
            var generators = chunks
                .Select<IEnumerable<string>, Func<Task>>(
                    chunk => async () => {
                        var existing = await GetOrCreateTimeSeriesChunk(client, chunk, buildTimeSeries, 0, token);
                        result.AddRange(existing);
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
            return result;
        }
        
        /// <summary>
        /// Insert the provided data points into CDF. The data points are chunked
        /// according to <paramref name="keyChunkSize"/> and <paramref name="valueChunkSize"/>.
        /// The data points are trimmed according to the <see href="https://docs.cognite.com/api/v1/#operation/postMultiTimeSeriesDatapoints">CDF limits</see>.
        /// The <paramref name="points"/> dictionary keys are time series identities (Id or ExternalId) and the values are numeric or string data points
        /// </summary>
        /// <param name="client">Cognite client</param>
        /// <param name="points">Data points</param>
        /// <param name="keyChunkSize">Dictionary key chunk size</param>
        /// <param name="valueChunkSize">Dictionary value chunk size</param>
        /// <param name="throttleSize">Throttle size</param>
        /// <param name="token">Cancellation token</param>
        public static async Task InsertDataPointsAsync(
            this Client client,
            IDictionary<Identity, IEnumerable<Datapoint>> points,
            int keyChunkSize,
            int valueChunkSize,
            int throttleSize,
            CancellationToken token)
        {
            var trimmedDict = GetTrimmedDataPoints(points);
            var chunks = trimmedDict
                .Select(p => (p.Key, p.Value))
                .ChunkBy(valueChunkSize, keyChunkSize);

            var generators = chunks
                .Select<IEnumerable<(Identity id, IEnumerable<Datapoint> dataPoints)>, Func<Task>>(
                    chunk => async () => await InsertDataPointsChunk(client, chunk, token));
            
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
        /// Tries to insert the data points into CDF. If any time series are not
        /// found, or if the time series is of wrong type (Inserting numeric data
        /// into a string time series), the errors are ignored and the missing/mismatched 
        /// ids are returned
        /// </summary>
        /// <param name="client">Cognite client</param>
        /// <param name="points">Data points</param>
        /// <param name="keyChunkSize">Dictionary key chunk size</param>
        /// <param name="valueChunkSize">Dictionary value chunk size</param>
        /// <param name="throttleSize">Throttle size</param>
        /// <param name="token">Cancellation token</param>
        /// <returns></returns>
        public static async Task<InsertError> InsertDataPointsIgnoreErrorsAsync(
            this Client client,
            IDictionary<Identity, IEnumerable<Datapoint>> points,
            int keyChunkSize,
            int valueChunkSize,
            int throttleSize,
            CancellationToken token)
        {
            var trimmedDict = GetTrimmedDataPoints(points);
            var chunks = trimmedDict
                .Select(p => (p.Key, p.Value))
                .ChunkBy(valueChunkSize, keyChunkSize);

            var errors = new List<InsertError>();
            var generators = chunks
                .Select<IEnumerable<(Identity id, IEnumerable<Datapoint> dataPoints)>, Func<Task>>(
                    chunk => async () => { 
                        var error = await InsertDataPointsIgnoreErrorsChunk(client, chunk, token);
                        errors.Add(error);
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
            InsertError errorsFound = new InsertError(new Identity[]{}, new Identity[]{});
            foreach (var err in errors)
            {   
                errorsFound = errorsFound.UnionWith(err);
            }
            return errorsFound;
        }

        private static Dictionary<Identity, IEnumerable<Datapoint>> GetTrimmedDataPoints(IDictionary<Identity, IEnumerable<Datapoint>> points)
        {
            var comparer = new IdentityComparer();
            Dictionary<Identity, IEnumerable<Datapoint>> trimmedDict = new Dictionary<Identity, IEnumerable<Datapoint>>(comparer);
            foreach (var key in points.Keys)
            {
                var validDps = points[key].TrimValues();
                if (validDps.Any())
                {
                    if (trimmedDict.ContainsKey(key))
                    {
                        var existing = trimmedDict[key].ToList();
                        existing.AddRange(validDps);
                        trimmedDict[key] = existing;
                    }
                    else {
                        trimmedDict.Add(key, validDps);
                    }
                }
            }

            return trimmedDict;
        }

        private static async Task InsertDataPointsChunk(
            this Client client,
            IEnumerable<(Identity id, IEnumerable<Datapoint> dataPoints)> points,
            CancellationToken token)
        {
            var request = new DataPointInsertionRequest();
            var dataPointCount = 0;
            foreach (var entry in points)
            {
                var item = new DataPointInsertionItem();
                if (entry.id.Id.HasValue)
                {
                    item.Id = entry.id.Id.Value;
                }
                else
                {
                    item.ExternalId = entry.id.ExternalId.ToString();
                }
                if (!entry.dataPoints.Any())
                {
                    continue;
                }
                var stringPoints = entry.dataPoints
                    .Where(dp => dp.StringValue != null)
                    .Select(dp => new StringDatapoint
                        {
                            Timestamp = dp.Timestamp,
                            Value = dp.StringValue
                        });
                var numericPoints = entry.dataPoints
                    .Where(dp => dp.NumericValue.HasValue)
                    .Select(dp => new NumericDatapoint
                        {
                            Timestamp = dp.Timestamp,
                            Value = dp.NumericValue.Value
                        });
                if (stringPoints.Any())
                {
                    var stringData = new StringDatapoints();
                    stringData.Datapoints.AddRange(stringPoints);
                    if (stringData.Datapoints.Count > 0)
                    {
                        item.StringDatapoints = stringData;
                        request.Items.Add(item);
                        dataPointCount += stringData.Datapoints.Count;
                    }
                }
                else
                {
                    var doubleData = new NumericDatapoints();
                    doubleData.Datapoints.AddRange(numericPoints);
                    if (doubleData.Datapoints.Count > 0)
                    {
                        item.NumericDatapoints = doubleData;
                        request.Items.Add(item);
                        dataPointCount += doubleData.Datapoints.Count;
                    }
                }
            }
            try
            {
                await client.DataPoints.CreateAsync(request, token);
                _numberDataPoints.Inc(dataPointCount);
            }
            catch (TimeoutRejectedException)
            {
                _logger.LogWarning("Uploading data points to CDF timed out. Consider reducing the chunking sizes in the config file");
                throw;
            }
        }

        private static async Task<InsertError> InsertDataPointsIgnoreErrorsChunk(
            this Client client,
            IEnumerable<(Identity id, IEnumerable<Datapoint> dataPoints)> points,
            CancellationToken token)
        {
            var comparer = new IdentityComparer();
            var missing = new HashSet<Identity>(comparer);
            var mismatched = new HashSet<Identity>(comparer);
            try
            {
                await InsertDataPointsChunk(client, points, token);
            }
            catch (ResponseException e) when (e.Code == 400)
            {
                if (e.Missing != null && e.Missing.Any()) {
                    foreach (var ts in e.Missing)
                    {
                        if (ts.TryGetValue("externalId", out MultiValue exIdValue))
                        {
                            missing.Add(new Identity(exIdValue.ToString()));
                        }
                        else if (ts.TryGetValue("id", out MultiValue idValue))
                        {
                            missing.Add(new Identity(((MultiValue.Long) idValue).Value));
                        }
                    }
                }
                else if (e.Message == "Expected string value for datapoint" || e.Message == "Expected numeric value for datapoint")
                {
                    // The error message does not specify which time series caused the error.
                    // Need to fetch all time series in the chunk and check...
                    var chunking = new ChunkingConfig();
                    var timeseries = await client.TimeSeries.GetByIdsIgnoreErrors(points.Select(p => p.id), chunking.TimeSeries, 1, token);
                    foreach (var entry in points)
                    {
                        var ts = timeseries
                            .Where(t => entry.id.ExternalId == t.ExternalId || entry.id.Id == t.Id)
                            .FirstOrDefault();
                        if (ts != null) {
                            if (ts.IsString && entry.dataPoints.Any(dp => dp.NumericValue.HasValue))
                            {
                                mismatched.Add(entry.id);
                            }
                            else if (!ts.IsString && entry.dataPoints.Any(dp => dp.StringValue != null))
                            {
                                mismatched.Add(entry.id);
                            }
                        }
                    }
                    if (!mismatched.Any())
                    {
                        _logger.LogError("Trying to insert data points of the wrong type, but cannot determine in which time series");
                        throw;
                    }
                }
                else {
                    throw;
                }
                var toInsert = points
                    .Where(p => !missing.Contains(p.id) && !mismatched.Contains(p.id));
                var errors = await InsertDataPointsIgnoreErrorsChunk(client, toInsert, token);
                missing.UnionWith(errors.IdsNotFound);
                mismatched.UnionWith(errors.IdsWithMismatchedData);
            }
            return new InsertError(missing, mismatched);
        }

        /// <summary>
        /// Ensures that all time series in <paramref name="timeSeries"/> exist in CDF.
        /// Tries to create the time series and returns when all are created or reported as 
        /// duplicates (already exist in CDF)
        /// </summary>
        /// <param name="client">Cognite client</param>
        /// <param name="timeSeries">List of <see cref="TimeSeriesCreate"/> objects</param>
        /// <param name="chunkSize">Chunk size</param>
        /// <param name="throttleSize">Throttle size</param>
        /// <param name="token">Cancellation token</param>
        public static async Task EnsureTimeSeriesExistsAsync(
            this Client client,
            IEnumerable<TimeSeriesCreate> timeSeries, 
            int chunkSize, 
            int throttleSize,
            CancellationToken token)
        {
            var chunks = timeSeries
                .ChunkBy(chunkSize);
            _logger.LogDebug("Ensuring time series. Number of time series: {Number}. Number of chunks: {Chunks}", timeSeries.Count(), chunks.Count());
            var generators = chunks
                .Select<IEnumerable<TimeSeriesCreate>, Func<Task>>(
                chunk => async () => {
                    await EnsureTimeSeriesChunk(client, chunk, token);
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


        private static async Task<IEnumerable<TimeSeries>> GetOrCreateTimeSeriesChunk(
            Client client,
            IEnumerable<string> externalIds,
            Func<IEnumerable<string>, IEnumerable<TimeSeriesCreate>> buildTimeSeries,
            int backoff,
            CancellationToken token)
        {
            var missing = new HashSet<string>();
            try
            {
                var existingTs = await client.TimeSeries.RetrieveAsync(externalIds.Select(id => new Identity(id)), token);
                _logger.LogDebug("Retrieved {Existing} times series from CDF", existingTs.Count());
                return existingTs;
            }
            catch (ResponseException e) when (e.Code == 400 && e.Missing.Any()){
                foreach (var ts in e.Missing)
                {
                    if (ts.TryGetValue("externalId", out MultiValue value))
                    {
                        missing.Add(value.ToString());
                    }
                }
                
            }

            _logger.LogDebug("Could not fetch {Missing} out of {Found} time series. Attempting to create the missing ones", missing.Count, externalIds.Count());
            var toGet = new HashSet<string>(externalIds
                .Where(e => !missing.Contains(e)));
            var created = new List<TimeSeries>();

            try
            {
                var newTs = await client.TimeSeries.CreateAsync(buildTimeSeries(missing), token);
                created.AddRange(newTs);
                _logger.LogDebug("Created {New} new time series in CDF", newTs.Count());
            }
            catch (ResponseException e) when (e.Code == 409 && e.Duplicated.Any())
            {
                if (backoff > 10) // ~3.5 min total backoff time
                {
                    throw;
                }
                _logger.LogDebug("Found {NumDuplicated} duplicates, during the creation of {NumTimeSeries} time series", e.Duplicated.Count(), missing.Count);
                toGet.UnionWith(missing);
            }
            if (toGet.Any()) {
                await Task.Delay(TimeSpan.FromSeconds(0.1 * Math.Pow(2, backoff)));
                var ensured = await GetOrCreateTimeSeriesChunk(client, toGet, buildTimeSeries, backoff + 1, token);
                created.AddRange(ensured);
            }
            return created;
        }

        private static async Task EnsureTimeSeriesChunk(
            Client client,
            IEnumerable<TimeSeriesCreate> timeSeries,
            CancellationToken token)
        {
            var create = timeSeries;
            while (!token.IsCancellationRequested && create.Any())
            {
                try
                {
                    var newTs = await client.TimeSeries.CreateAsync(create, token);
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
                    create = timeSeries.Where(ts => !duplicated.Contains(ts.ExternalId));
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

        /// <summary>
        /// Get the time series with the provided <paramref name="ids"/>. Ignore any
        /// unknown ids
        /// </summary>
        /// <param name="tsClient">A <see cref="Client.TimeSeries"/> client</param>
        /// <param name="ids">List of <see cref="Identity"/> objects</param>
        /// <param name="chunkSize">Chunk size</param>
        /// <param name="throttleSize">Throttle size</param>
        /// <param name="token">Cancellation token</param>
        /// <returns></returns>
        public static async Task<IEnumerable<TimeSeries>> GetByIdsIgnoreErrors(
            this TimeSeriesResource tsClient,
            IEnumerable<Identity> ids, 
            int chunkSize, 
            int throttleSize,
            CancellationToken token)
        {
            var result = new List<TimeSeries>();
            var chunks = ids
                .ChunkBy(chunkSize);
            var generators = chunks
                .Select<IEnumerable<Identity>, Func<Task>>(
                chunk => async () => {
                    var found = await GetByIdsIgnoreErrorsChunk(tsClient, chunk, token);
                    result.AddRange(found);
                });
            await generators.RunThrottled(throttleSize, token);
            return result;
        }

        private static async Task<IEnumerable<TimeSeries>> GetByIdsIgnoreErrorsChunk(
            TimeSeriesResource tsClient,
            IEnumerable<Identity> ids,
            CancellationToken token)
        {
            // TODO: Remove once ignoreUnknownIds is available in the SDK
            var comparer = new IdentityComparer();
            var missing = new HashSet<Identity>(comparer);
            try
            {
                var existingTs = await tsClient.RetrieveAsync(ids, token);
                _logger.LogDebug("Retrieved {Existing} times series from CDF", existingTs.Count());
                return existingTs;
            }
            catch (ResponseException e) when (e.Code == 400 && e.Missing.Any()){
                foreach (var ts in e.Missing)
                {
                    if (ts.TryGetValue("externalId", out MultiValue exIdValue))
                    {
                        missing.Add(new Identity(exIdValue.ToString()));
                    }
                    else if (ts.TryGetValue("id", out MultiValue idValue))
                    {
                        missing.Add(new Identity(((MultiValue.Long) idValue).Value));
                    }
                }
                var toGet = ids
                    .Where(id => !missing.Contains(id));
                return await GetByIdsIgnoreErrorsChunk(tsClient, toGet, token);
            }
        }
    }
}