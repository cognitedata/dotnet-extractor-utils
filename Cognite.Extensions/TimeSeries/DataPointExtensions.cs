using Cognite.Extractor.Common;
using CogniteSdk;
using CogniteSdk.Resources;
using Com.Cognite.V1.Timeseries.Proto;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Prometheus;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using TimeRange = Cognite.Extractor.Common.TimeRange;

namespace Cognite.Extensions
{
    /// <summary>
    /// Extensions to datapoints
    /// </summary>
    public static class DataPointExtensions
    {
        private const int _maxNumOfVerifyRequests = 10;
        private static ILogger _logger = new NullLogger<Client>();

        internal static void SetLogger(ILogger logger)
        {
            _logger = logger;
        }

        /// <summary>
        /// Create a protobuf insertion request from dictionary
        /// </summary>
        /// <param name="dps">Datapoints to insert</param>
        /// <returns>Converted request</returns>
        public static DataPointInsertionRequest ToInsertRequest(this IDictionary<Identity, IEnumerable<Datapoint>> dps)
        {
            if (dps == null) throw new ArgumentNullException(nameof(dps));
            var request = new DataPointInsertionRequest();
            var dataPointCount = 0;
            foreach (var kvp in dps)
            {
                var item = new DataPointInsertionItem();
                if (kvp.Key.Id.HasValue)
                {
                    item.Id = kvp.Key.Id.Value;
                }
                else
                {
                    item.ExternalId = kvp.Key.ExternalId.ToString();
                }
                if (!kvp.Value.Any())
                {
                    continue;
                }
                var stringPoints = kvp.Value
                    .Where(dp => dp.StringValue != null)
                    .Select(dp => new StringDatapoint
                    {
                        Timestamp = dp.Timestamp,
                        Value = dp.StringValue
                    });
                var numericPoints = kvp.Value
                    .Where(dp => dp.NumericValue.HasValue)
                    .Select(dp => new NumericDatapoint
                    {
                        Timestamp = dp.Timestamp,
                        Value = dp.NumericValue!.Value
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
            return request;
        }

        /// <summary>
        /// Insert datapoints to timeseries. Insertions are chunked and cleaned according to configuration,
        /// and can optionally handle errors.
        /// </summary>
        /// <param name="client">Cognite client</param>
        /// <param name="points">Datapoints to insert</param>
        /// <param name="keyChunkSize">Maximum number of timeseries per chunk</param>
        /// <param name="valueChunkSize">Maximum number of datapoints per timeseries</param>
        /// <param name="throttleSize">Maximum number of parallel request</param>
        /// <param name="timeseriesChunkSize">Maximum number of timeseries to retrieve per request</param>
        /// <param name="timeseriesThrottleSize">Maximum number of parallel requests to retrieve timeseries</param>
        /// <param name="gzipCountLimit">Number of datapoints total before using gzip compression.</param>
        /// <param name="sanitationMode">How to sanitize datapoints</param>
        /// <param name="retryMode">How to handle retries</param>
        /// <param name="nanReplacement">Optional replacement for NaN double values</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>Result with a list of errors</returns>
        public static async Task<CogniteResult<DataPointInsertError>> InsertAsync(
            Client client,
            IDictionary<Identity, IEnumerable<Datapoint>> points,
            int keyChunkSize,
            int valueChunkSize,
            int throttleSize,
            int timeseriesChunkSize,
            int timeseriesThrottleSize,
            int gzipCountLimit,
            SanitationMode sanitationMode,
            RetryMode retryMode,
            double? nanReplacement,
            CancellationToken token)
        {
            IEnumerable<CogniteError<DataPointInsertError>> errors;
            (points, errors) = Sanitation.CleanDataPointsRequest(points, sanitationMode, nanReplacement);

            var chunks = points
                .Select(p => (p.Key, p.Value))
                .ChunkBy(valueChunkSize, keyChunkSize)
                .Select(chunk => chunk.ToDictionary(pair => pair.Key, pair => pair.Values))
                .ToList();

            int size = chunks.Count + (errors.Any() ? 1 : 0);
            var results = new CogniteResult<DataPointInsertError>[size];

            if (errors.Any())
            {
                results[size - 1] = new CogniteResult<DataPointInsertError>(errors);
                if (size == 1) return results[size - 1];
            }
            if (size == 0) return new CogniteResult<DataPointInsertError>(null);

            _logger.LogDebug("Inserting timeseries datapoints. Number of timeseries: {Number}. Number of chunks: {Chunks}", points.Count, chunks.Count);
            var generators = chunks
                .Select<IDictionary<Identity, IEnumerable<Datapoint>>, Func<Task>>(
                (chunk, idx) => async () => {
                    var result = await
                        InsertDataPointsHandleErrors(client, chunk, timeseriesChunkSize, timeseriesThrottleSize, gzipCountLimit, retryMode, token)
                        .ConfigureAwait(false);
                    results[idx] = result;
                });

            int taskNum = 0;
            await generators.RunThrottled(
                throttleSize,
                (_) => {
                    if (chunks.Count > 1)
                        _logger.LogDebug("{MethodName} completed {NumDone}/{TotalNum} tasks",
                            nameof(InsertAsync), ++taskNum, chunks.Count);
                },
                token).ConfigureAwait(false);

            return CogniteResult<DataPointInsertError>.Merge(results);
        }

        private static async Task<CogniteResult<DataPointInsertError>> InsertDataPointsHandleErrors(
            Client client,
            IDictionary<Identity, IEnumerable<Datapoint>> points,
            int timeseriesChunkSize,
            int timeseriesThrottleSize,
            int gzipCountLimit,
            RetryMode retryMode,
            CancellationToken token)
        {
            var errors = new List<CogniteError<DataPointInsertError>>();
            while (points != null && points.Any() && !token.IsCancellationRequested)
            {
                var request = points.ToInsertRequest();
                try
                {
                    bool useGzip = false;
                    int count = points.Sum(kvp => kvp.Value.Count());
                    if (gzipCountLimit >= 0 && count >= gzipCountLimit)
                    {
                        useGzip = true;
                    }

                    if (useGzip)
                    {
                        using (CdfMetrics.Datapoints.WithLabels("create"))
                        {
                            await client.DataPoints.CreateAsync(request, CompressionLevel.Fastest, token).ConfigureAwait(false);
                        }
                    }
                    else
                    {
                        using (CdfMetrics.Datapoints.WithLabels("create"))
                        {
                            await client.DataPoints.CreateAsync(request, token).ConfigureAwait(false);
                        }
                    }

                    CdfMetrics.NumberDatapoints.Inc(count);

                    _logger.LogDebug("Created {cnt} datapoints for {ts} timeseries in CDF", count, points.Count);
                    return new CogniteResult<DataPointInsertError>(errors);
                }
                catch (Exception ex)
                {
                    _logger.LogDebug("Failed to create datapoints for {seq} timeseries: {msg}", points.Count, ex.Message);
                    var error = ResultHandlers.ParseException<DataPointInsertError>(ex, RequestType.CreateDatapoints);
                    if (error.Complete) errors.Add(error);
                    if (error.Type == ErrorType.FatalFailure
                        && (retryMode == RetryMode.OnFatal
                            || retryMode == RetryMode.OnFatalKeepDuplicates))
                    {
                        await Task.Delay(1000, token).ConfigureAwait(false);
                    }
                    else if (retryMode == RetryMode.None) break;
                    else
                    {
                        if (!error.Complete)
                        {
                            (error, points) = await ResultHandlers
                                .VerifyDatapointsFromCDF(client.TimeSeries, error,
                                    points, timeseriesChunkSize, timeseriesThrottleSize, token)
                                .ConfigureAwait(false);
                            errors.Add(error);
                        }
                        points = ResultHandlers.CleanFromError(error, points);
                    }
                }
            }

            return new CogniteResult<DataPointInsertError>(errors);
        }

        /// <summary>
        /// Deletes ranges of data points in CDF. The <paramref name="ranges"/> parameter contains the first (inclusive)
        /// and last (inclusive) timestamps for the range. After the delete request is sent to CDF, attempt to confirm that
        /// the data points were deleted by querying the time range. Deletes in CDF are eventually consistent, failing to 
        /// confirm the deletion does not mean that the operation failed in CDF
        /// </summary>
        /// <param name="dataPoints">Cognite datapoints resource</param>
        /// <param name="ranges">Ranges to delete</param>
        /// <param name="deleteChunkSize">Chunk size for delete operations</param>
        /// <param name="listChunkSize">Chunk size for list operations</param>
        /// <param name="deleteThrottleSize">Throttle size for delete operations</param>
        /// <param name="listThrottleSize">Throttle size for list operations</param>
        /// <param name="token">Cancelation token</param>
        /// <returns>A <see cref="DeleteError"/> object with any missing ids or ids with unconfirmed deletes</returns>
        public static async Task<DeleteError> DeleteIgnoreErrorsAsync(
            this DataPointsResource dataPoints,
            IDictionary<Identity, IEnumerable<TimeRange>> ranges,
            int deleteChunkSize,
            int listChunkSize,
            int deleteThrottleSize,
            int listThrottleSize,
            CancellationToken token)
        {
            if (ranges == null)
            {
                throw new ArgumentNullException(nameof(ranges));
            }
            var toDelete = new List<IdentityWithRange>();
            foreach (var kvp in ranges)
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

            var missing = new HashSet<Identity>();
            var mutex = new object();

            var generators = chunks
                .Select<IEnumerable<IdentityWithRange>, Func<Task>>(
                    c => async () =>
                    {
                        var errors = await DeleteDataPointsIgnoreErrorsChunk(dataPoints, c, token).ConfigureAwait(false);
                        lock (mutex)
                        {
                            missing.UnionWith(errors);
                        }
                    });

            var taskNum = 0;
            await generators.RunThrottled(
                deleteThrottleSize,
                (_) => {
                    if (chunks.Count > 1)
                        _logger.LogDebug("{MethodName} completed {NumDone}/{TotalNum} tasks",
                            nameof(DeleteIgnoreErrorsAsync), ++taskNum, chunks.Count);
                },
                token).ConfigureAwait(false);
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
                .ChunkBy(listChunkSize)
                .ToList(); // Maximum number of items in the /timeseries/data/list endpoint.

            var notVerified = new HashSet<Identity>();
            var verifyGenerators = queryChunks
                .Select<IEnumerable<DataPointsQueryItem>, Func<Task>>(
                    c => async () =>
                    {
                        var errors = await VerifyDataPointsDeletion(dataPoints, c, token).ConfigureAwait(false);
                        lock (mutex)
                        {
                            notVerified.UnionWith(errors);
                        }
                    });

            taskNum = 0;
            await verifyGenerators.RunThrottled(
                listThrottleSize,
                (_) => { _logger.LogDebug("Verifying data points deletion: {Num}/{Total}", ++taskNum, queryChunks.Count); },
                token).ConfigureAwait(false);

            _logger.LogDebug("Deletion tasks completed");
            return new DeleteError(missing, notVerified);
        }

        private static async Task<HashSet<Identity>> DeleteDataPointsIgnoreErrorsChunk(
            DataPointsResource dataPoints,
            IEnumerable<IdentityWithRange> chunks,
            CancellationToken token)
        {
            var missing = new HashSet<Identity>();
            var deleteQuery = new DataPointsDelete()
            {
                Items = chunks
            };
            try
            {
                using (CdfMetrics.Datapoints.WithLabels("delete").NewTimer())
                {
                    await dataPoints.DeleteAsync(deleteQuery, token).ConfigureAwait(false);
                }
            }
            catch (ResponseException e) when (e.Code == 400 && e.Missing != null && e.Missing.Any())
            {
                CogniteUtils.ExtractMissingFromResponseException(missing, e);
                var remaining = chunks.Where(i => !missing.Contains(i.Id.HasValue ? new Identity(i.Id.Value) : new Identity(i.ExternalId)));
                var errors = await DeleteDataPointsIgnoreErrorsChunk(dataPoints, remaining, token).ConfigureAwait(false);
                missing.UnionWith(errors);
            }
            return missing;
        }

        private static async Task<HashSet<Identity>> VerifyDataPointsDeletion(
            DataPointsResource dataPoints,
            IEnumerable<DataPointsQueryItem> query,
            CancellationToken token)
        {
            int count = 1;
            var dataPointsQuery = new DataPointsQuery()
            {
                Items = query
            };

            int tries = 0;
            var notVerified = new HashSet<Identity>();
            while (count > 0 && tries < _maxNumOfVerifyRequests)
            {
                notVerified.Clear();
                DataPointListResponse results;
                using (CdfMetrics.Datapoints.WithLabels("list").NewTimer())
                {
                    results = await dataPoints.ListAsync(dataPointsQuery, token).ConfigureAwait(false);
                }
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
                    await Task.Delay(500, token).ConfigureAwait(false);
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
        /// <summary>
        /// Get the last timestamp for each time series given in <paramref name="ids"/> before each given timestamp.
        /// Ignores timeseries not in CDF. The return dictionary contains only ids that exist in CDF.
        /// Note that end limits closer to actual endpoints in CDF is considerably faster.
        /// </summary>
        /// <param name="dataPoints">DataPointsResource to use</param>
        /// <param name="ids">ExternalIds and last timestamp. Let last timestamp be DateTime.MaxValue to use default ("now").</param>
        /// <param name="chunkSize">Number of timeseries per request</param>
        /// <param name="throttleSize">Maximum number of parallel requests</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>Dictionary from externalId to last timestamp, only contains existing timeseries</returns>
        public static async Task<IDictionary<Identity, DateTime>> GetLatestTimestamps(
            this DataPointsResource dataPoints,
            IEnumerable<(Identity id, DateTime before)> ids,
            int chunkSize,
            int throttleSize,
            CancellationToken token)
        {
            var ret = new ConcurrentDictionary<Identity, DateTime>();
            var idSet = new HashSet<Identity>(ids.Select(id => id.id));

            var chunks = ids
                .Select((pair) =>
                {
                    var id = pair.id;
                    IdentityWithBefore idt = id.ExternalId == null ? IdentityWithBefore.Create(id.Id!.Value) : IdentityWithBefore.Create(id.ExternalId);
                    if (pair.before != DateTime.MaxValue)
                    {
                        idt.Before = pair.before.ToUnixTimeMilliseconds().ToString();
                    }
                    return idt;
                })
                .ChunkBy(chunkSize)
                .ToList();

            var generators = chunks.Select<IEnumerable<IdentityWithBefore>, Func<Task>>(
                chunk => async () =>
                {
                    IEnumerable<DataPointsItem<DataPoint>> dps;
                    using (CdfMetrics.Datapoints.WithLabels("latest").NewTimer())
                    {
                        dps = await dataPoints.LatestAsync(
                            new DataPointsLatestQuery
                            {
                                IgnoreUnknownIds = true,
                                Items = chunk
                            }, token).ConfigureAwait(false);
                    }

                    foreach (var dp in dps)
                    {
                        if (dp.DataPoints.Any())
                        {
                            Identity id;
                            if (dp.ExternalId != null)
                            {
                                id = new Identity(dp.ExternalId);
                                if (!idSet.Contains(id))
                                {
                                    id = new Identity(dp.Id);
                                }
                            }
                            else
                            {
                                id = new Identity(dp.Id);
                            }
                            ret[id] = CogniteTime.FromUnixTimeMilliseconds(dp.DataPoints.First().Timestamp);
                        }
                    }
                });
            int numTasks = 0;
            await generators
                .RunThrottled(throttleSize, (_) =>
                    _logger.LogDebug("Last timestamp from CDF: {num}/{total}", ++numTasks, chunks.Count), token)
                .ConfigureAwait(false);
            return ret;
        }

        /// <summary>
        /// Get the first timestamp for each time series given in <paramref name="ids"/> after each given timestamp.
        /// Ignores timeseries not in CDF. The return dictionary contains only ids that exist in CDF.
        /// </summary>
        /// <param name="dataPoints">DataPointsResource to use</param>
        /// <param name="ids">ExternalIds and last timestamp. Let last timestamp be Epoch to use default.</param>
        /// <param name="chunkSize">Number of timeseries per request</param>
        /// <param name="throttleSize">Maximum number of parallel requests</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>Dictionary from externalId to first timestamp, only contains existing timeseries</returns>
        public static async Task<IDictionary<Identity, DateTime>> GetEarliestTimestamps(
            this DataPointsResource dataPoints,
            IEnumerable<(Identity id, DateTime after)> ids,
            int chunkSize,
            int throttleSize,
            CancellationToken token)
        {
            var ret = new ConcurrentDictionary<Identity, DateTime>();
            var idSet = new HashSet<Identity>(ids.Select(id => id.id));

            var chunks = ids
                .Select((pair) =>
                {
                    var query = new DataPointsQueryItem();
                    if (pair.id.Id.HasValue)
                    {
                        query.Id = pair.id.Id.Value;
                    }
                    else
                    {
                        query.ExternalId = pair.id.ExternalId;
                    }
                    if (pair.after > CogniteTime.DateTimeEpoch)
                    {
                        query.Start = pair.after.ToUnixTimeMilliseconds().ToString();
                    }
                    return query;
                })
                .ChunkBy(chunkSize)
                .ToList();

            var generators = chunks.Select<IEnumerable<DataPointsQueryItem>, Func<Task>>(
                chunk => async () =>
                {
                    DataPointListResponse dps;
                    using (CdfMetrics.Datapoints.WithLabels("first").NewTimer())
                    {
                        dps = await dataPoints.ListAsync(
                            new DataPointsQuery
                            {
                                IgnoreUnknownIds = true,
                                Items = chunk,
                                Limit = 1
                            }, token).ConfigureAwait(false);
                    }

                    foreach (var dp in dps.Items)
                    {
                        Identity id;
                        if (dp.ExternalId != null)
                        {
                            id = new Identity(dp.ExternalId);
                            if (!idSet.Contains(id))
                            {
                                id = new Identity(dp.Id);
                            }
                        }
                        else
                        {
                            id = new Identity(dp.Id);
                        }
                        if (dp.DatapointTypeCase == DataPointListItem.DatapointTypeOneofCase.NumericDatapoints
                            && dp.NumericDatapoints.Datapoints.Any())
                        {
                            ret[id] = CogniteTime.FromUnixTimeMilliseconds(dp.NumericDatapoints.Datapoints.First().Timestamp);
                        }
                        else if (dp.DatapointTypeCase == DataPointListItem.DatapointTypeOneofCase.StringDatapoints
                            && dp.StringDatapoints.Datapoints.Any())
                        {
                            ret[id] = CogniteTime.FromUnixTimeMilliseconds(dp.StringDatapoints.Datapoints.First().Timestamp);
                        }
                    }
                });
            int numTasks = 0;
            await generators
                .RunThrottled(throttleSize, (_) =>
                    _logger.LogDebug("First timestamp from CDF: {num}/{total}", ++numTasks, chunks.Count), token)
                .ConfigureAwait(false);
            return ret;
        }
        /// <summary>
        /// Fetches the range of datapoints present in CDF. Limited by given ranges for each id.
        /// Note that end limits closer to actual endpoints in CDF is considerably faster.
        /// </summary>
        /// <param name="dataPoints">DataPointsResource to use</param>
        /// <param name="ids">ExternalIds and start/end of region to look for datapoints.
        /// Use TimeRange.Complete for first after epoch, and last before now.</param>
        /// <param name="chunkSizeEarliest">Number of timeseries to read for each earliest request</param>
        /// <param name="chunkSizeLatest">Number of timeseries to read for each latest request</param>
        /// <param name="throttleSize">Max number of parallel requests</param>
        /// <param name="latest">If true, fetch latest timestamps</param>
        /// <param name="earliest">If true, fetch earliest timestamps</param>
        /// <param name="token">Cancellation token</param>
        /// <returns></returns>
        public static async Task<IDictionary<Identity, TimeRange>> GetExtractedRanges(
            this DataPointsResource dataPoints,
            IEnumerable<(Identity id, TimeRange limit)> ids,
            int chunkSizeEarliest,
            int chunkSizeLatest,
            int throttleSize,
            bool latest,
            bool earliest,
            CancellationToken token)
        {
            if (ids == null)
            {
                throw new ArgumentNullException(nameof(ids));
            }
            _logger.LogDebug("Getting extracted ranges for {num} timeseries", ids.Count());

            if (latest && earliest) throttleSize = Math.Max(1, throttleSize / 2);

            var ranges = ids.ToDictionary(pair => pair.id, pair => TimeRange.Empty);
            var tasks = new List<Task<IDictionary<Identity, DateTime>>>();
            if (latest)
            {
                tasks.Add(dataPoints.GetLatestTimestamps(ids.Select(pair => (pair.id, pair.limit?.Last ?? DateTime.MaxValue)),
                    chunkSizeLatest, throttleSize, token));
            }
            if (earliest)
            {
                tasks.Add(dataPoints.GetEarliestTimestamps(ids.Select(pair => (pair.id, pair.limit?.First ?? CogniteTime.DateTimeEpoch)),
                    chunkSizeEarliest, throttleSize, token));
            }
            var results = await Task.WhenAll(tasks).ConfigureAwait(false);
            if (latest)
            {
                var latestResult = results[0];
                foreach (var id in ids)
                {
                    if (latestResult.TryGetValue(id.id, out DateTime ts))
                    {
                        ranges[id.id] = new TimeRange(CogniteTime.DateTimeEpoch, ts);
                    }
                }
            }
            if (earliest)
            {
                var earliestResult = results[latest ? 1 : 0];
                foreach (var id in ids)
                {
                    if (earliestResult.TryGetValue(id.id, out DateTime ts))
                    {
                        ranges[id.id] = new TimeRange(ts, ranges[id.id].Last);
                    }
                }
            }
            return ranges;
        }
    }
}
