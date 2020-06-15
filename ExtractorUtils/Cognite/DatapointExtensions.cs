using Cognite.Extractor.Common;
using Cognite.Extractor.Logging;
using Cognite.Extractor.Utils;
using CogniteSdk;
using Com.Cognite.V1.Timeseries.Proto;
using Microsoft.Extensions.Logging;
using Polly.Timeout;
using Prometheus;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.Extractor.Utils
{
    /// <summary>
    /// Extension utility methods for <see cref="Client"/>
    /// </summary>
    public static class DatapointExtensions
    {
        private static ILogger _logger = LoggingUtils.GetDefault();
        private const int _maxNumOfVerifyRequests = 10;

        internal static void SetLogger(ILogger logger)
        {
            _logger = logger;
        }

        private static readonly Counter _numberDataPoints = Prometheus.Metrics.CreateCounter(
            "extractor_utils_cdf_datapoints", "Number of data points uploaded to CDF");
        private static readonly Counter _invalidTimeDataPoints = Prometheus.Metrics.CreateCounter(
            "extractor_utils_cdf_invalid_data_points", "Number of skipped data points with timestamps not supported by CDF");
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
                .ChunkBy(valueChunkSize, keyChunkSize)
                .ToList();

            var generators = chunks
                .Select<IEnumerable<(Identity id, IEnumerable<Datapoint> dataPoints)>, Func<Task>>(
                    chunk => async () => await InsertDataPointsChunk(client, chunk, token));

            int taskNum = 0;
            await generators.RunThrottled(
                throttleSize,
                (_) => {
                    if (chunks.Count > 1)
                        _logger.LogDebug("{MethodName} completed {NumDone}/{TotalNum} tasks",
                            nameof(InsertDataPointsAsync), ++taskNum, chunks.Count);
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
                .ChunkBy(valueChunkSize, keyChunkSize)
                .ToList();

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
                    if (chunks.Count > 1)
                        _logger.LogDebug("{MethodName} completed {NumDone}/{TotalNum} tasks",
                            nameof(InsertDataPointsIgnoreErrorsAsync), ++taskNum, chunks.Count);
                },
                token);
            InsertError errorsFound = new InsertError(new Identity[] { }, new Identity[] { });
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
                var trimmedDps = points[key].TrimValues().ToList();
                var validDps = trimmedDps.RemoveOutOfRangeTimestamps().ToList();
                var difference = trimmedDps.Count - validDps.Count;
                if (difference > 0)
                {
                    _invalidTimeDataPoints.Inc(difference);
                    _logger.LogWarning("Time series {Name}: Discarding {Num} data points outside valid CDF timestamp range", key.ToString(), difference);
                }
                if (validDps.Any())
                {
                    if (trimmedDict.ContainsKey(key))
                    {
                        var existing = trimmedDict[key].ToList();
                        existing.AddRange(validDps);
                        trimmedDict[key] = existing;
                    }
                    else
                    {
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
                if (e.Missing != null && e.Missing.Any())
                {
                    CogniteUtils.ExtractMissingFromResponseException(missing, e);
                }
                else if (e.Message == "Expected string value for datapoint" || e.Message == "Expected numeric value for datapoint")
                {
                    // The error message does not specify which time series caused the error.
                    // Need to fetch all time series in the chunk and check...
                    var chunking = new ChunkingConfig();
                    var timeseries = await client.TimeSeries.GetTimeSeriesByIdsIgnoreErrors(points.Select(p => p.id), chunking.TimeSeries, 1, token);
                    foreach (var entry in points)
                    {
                        var ts = timeseries
                            .Where(t => entry.id.ExternalId == t.ExternalId || entry.id.Id == t.Id)
                            .FirstOrDefault();
                        if (ts != null)
                        {
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
                else
                {
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
        /// Deletes ranges of data points in CDF. The <paramref name="ranges"/> parameter contains the first (inclusive)
        /// and last (inclusive) timestamps for the range. After the delete request is sent to CDF, attempt to confirm that
        /// the data points were deleted by querying the time range. Deletes in CDF are eventually consistent, failing to 
        /// confirm the deletion does not mean that the operation failed in CDF
        /// </summary>
        /// <param name="client">Cognite client</param>
        /// <param name="ranges">Ranges to delete</param>
        /// <param name="deleteChunkSize">Chunk size for delete operations</param>
        /// <param name="listChunkSize">Chunk size for list operations</param>
        /// <param name="deleteThrottleSize">Throttle size for delete operations</param>
        /// <param name="listThrottleSize">Throttle size for list operations</param>
        /// <param name="token">Cancelation token</param>
        /// <returns>A <see cref="DeleteError"/> object with any missing ids or ids with unconfirmed deletes</returns>
        public static async Task<DeleteError> DeleteDataPointsIgnoreErrorsAsync(
            this Client client,
            IDictionary<Identity, IEnumerable<TimeRange>> ranges,
            int deleteChunkSize,
            int listChunkSize,
            int deleteThrottleSize,
            int listThrottleSize,
            CancellationToken token)
        {
            var toDelete = new List<IdentityWithRange>();
            foreach(var kvp in ranges)
            {
                _logger.LogTrace("Deleting data points from time series {Name}. Ranges: {Ranges}", 
                    kvp.Key.ToString(), string.Join(", ", kvp.Value.Select(v => v.ToString())));
                toDelete.AddRange(kvp.Value.Select(r =>
                    new IdentityWithRange
                    {
                        ExternalId = kvp.Key.ExternalId,
                        Id = kvp.Key.Id,
                        InclusiveBegin = r.First.ToUnixTimeMilliseconds(),
                        ExclusiveEnd = r.Last.ToUnixTimeMilliseconds() + 1 // exclusive
                    })
                );
            }

            var chunks = toDelete
                .ChunkBy(deleteChunkSize)
                .ToList(); // Maximum number of items in the /timeseries/data/delete endpoint.

            var missing = new HashSet<Identity>(new IdentityComparer());
            var generators = chunks
                .Select<IEnumerable<IdentityWithRange>, Func<Task>>(
                    c => async () =>
                    {
                        var errors = await DeleteDataPointsIgnoreErrorsChunk(client, c, token);
                        missing.UnionWith(errors);
                    });

            var taskNum = 0;
            await generators.RunThrottled(
                deleteThrottleSize,
                (_) => { 
                    if (chunks.Count > 1) 
                        _logger.LogDebug("{MethodName} completed {NumDone}/{TotalNum} tasks", 
                            nameof(DeleteDataPointsIgnoreErrorsAsync), ++taskNum, chunks.Count); 
                },
                token);
            _logger.LogDebug("Deletion completed. Verifying that data points were removed from CDF");

            var query = new List<DataPointsQueryItem>();
            foreach (var kvp in ranges)
            {
                var queries = kvp.Value.Select(r =>
                {
                    return new DataPointsQueryItem()
                    {
                        ExternalId = kvp.Key.ExternalId,
                        Id = kvp.Key.Id,
                        Start = $"{r.First.ToUnixTimeMilliseconds()}",
                        End = $"{r.Last.ToUnixTimeMilliseconds() + 1}",
                        Limit = 1
                    };
                });
                query.AddRange(queries);
            }

            var queryChunks = query
                .ChunkBy(listChunkSize); // Maximum number of items in the /timeseries/data/list endpoint.

            var notVerified = new HashSet<Identity>(new IdentityComparer());
            var verifyGenerators = queryChunks
                .Select<IEnumerable<DataPointsQueryItem>, Func<Task>>(
                    c => async () =>
                    {
                        var errors = await VerifyDataPointsDeletion(client, c, token);
                        notVerified.UnionWith(errors);
                    });

            taskNum = 0;
            await verifyGenerators.RunThrottled(
                listThrottleSize,
                (_) => { _logger.LogDebug("Verifying data points deletion: {Num}/{Total}", ++taskNum, queryChunks.Count()); },
                token);

            _logger.LogDebug("Deletion tasks completed");
            return new DeleteError(missing, notVerified);
        }

        private static async Task<HashSet<Identity>> DeleteDataPointsIgnoreErrorsChunk(
            Client client,
            IEnumerable<IdentityWithRange> chunks,
            CancellationToken token)
        {
            var missing = new HashSet<Identity>(new IdentityComparer());
            var deleteQuery = new DataPointsDelete()
            {
                Items = chunks
            };
            try{
                await client.DataPoints.DeleteAsync(deleteQuery, token);
            }
            catch (ResponseException e) when (e.Code == 400 && e.Missing != null && e.Missing.Any())
            {
                CogniteUtils.ExtractMissingFromResponseException(missing, e);
                var remaining = chunks.Where(i => !missing.Contains(i.Id.HasValue ? new Identity(i.Id.Value) : new Identity(i.ExternalId)));
                var errors = await DeleteDataPointsIgnoreErrorsChunk(client, remaining, token);
                missing.UnionWith(errors);
            }
            return missing;
        }

        private static async Task<HashSet<Identity>> VerifyDataPointsDeletion(
            Client client,
            IEnumerable<DataPointsQueryItem> query, 
            CancellationToken token)
        {
            int count = 1;
            var dataPointsQuery = new DataPointsQuery()
            {
                Items = query
            };

            int tries = 0;
            var notVerified = new HashSet<Identity>(new IdentityComparer());
            while (count > 0 && tries < _maxNumOfVerifyRequests)
            {
                notVerified.Clear();
                var results = await client.DataPoints.ListAsync(dataPointsQuery, token);
                count = 0;
                var queries = dataPointsQuery.Items.ToList();
                var remaining = new List<DataPointsQueryItem>(); 
                for (int i = 0; i < results.Items.Count; ++i)
                {
                    // The query and response have items in the same order
                    var q = queries[i];
                    var itemCount = results.Items[i].NumericDatapoints?.Datapoints.Count ?? 0 + results.Items[i].StringDatapoints?.Datapoints.Count ?? 0;
                    if (itemCount > 0)
                    {
                        var id = q.Id.HasValue ? new Identity(q.Id.Value) : new Identity(q.ExternalId);
                        notVerified.Add(id);
                        remaining.Add(q);
                    }
                    count += itemCount;
                }
                if (count > 0)
                {
                    dataPointsQuery.Items = remaining;
                    tries++;
                    _logger.LogDebug("Could not verify the deletion of data points in {Count}/{Total} time series. Retrying in 500ms", count, query.Count());
                    await Task.Delay(500);
                }
            }
            if (tries == _maxNumOfVerifyRequests && count > 0)
            {
                _logger.LogWarning("Failed to verify the deletion of data points after {NumAttempts} attempts. Ids: {Ids}", 
                    _maxNumOfVerifyRequests, 
                    dataPointsQuery.Items.Select(q => q.Id.HasValue ? q.Id.ToString() : q.ExternalId.ToString()));
            }
            else
            {
                notVerified.Clear();
            }
            return notVerified;
        }
    }
}
