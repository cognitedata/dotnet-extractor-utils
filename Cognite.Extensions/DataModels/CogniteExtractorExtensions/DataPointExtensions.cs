using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extractor.Common;
using CogniteSdk.Alpha;
using CogniteSdk.Beta.DataModels;
using CogniteSdk.Beta.DataModels.Core;
using CogniteSdk.Resources;
using Com.Cognite.V1.Timeseries.Proto;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Prometheus;
using TimeRange = Cognite.Extractor.Common.TimeRange;

namespace Cognite.Extensions.DataModels.CogniteExtractorExtensions
{
    /// <summary>
    /// Extensions to datapoints
    /// </summary>
    public static class DataPointExtensionsWithInstanceId
    {
        private const int _maxNumOfVerifyRequests = 10;
        private static ILogger _logger = new NullLogger<CogniteSdk.Client>();

        internal static void SetLogger(ILogger logger)
        {
            _logger = logger;
        }

        /// <summary>
        /// Alpha: Create a protobuf insertion request from dictionary
        /// </summary>
        /// <param name="dps">Datapoints to insert</param>
        /// <returns>Converted request</returns>
        public static DataPointInsertionRequest ToInsertRequest(this IDictionary<IdentityWithInstanceId, IEnumerable<Datapoint>> dps)
        {
            if (dps == null) throw new ArgumentNullException(nameof(dps));
            var request = new DataPointInsertionRequest();
            var dataPointCount = 0;
            foreach (var kvp in dps)
            {
                var item = new DataPointInsertionItem() { InstanceId = new InstanceId() { Space = kvp.Key.InstanceId.Space, ExternalId = kvp.Key.InstanceId.ExternalId } };
                if (!kvp.Value.Any())
                {
                    continue;
                }
                var isString = kvp.Value.First().IsString;
                if (isString)
                {
                    var finalDps = new StringDatapoints();
                    foreach (var dp in kvp.Value)
                    {
                        if (!dp.IsString) continue;
                        finalDps.Datapoints.Add(new StringDatapoint
                        {
                            Timestamp = dp.Timestamp,
                            Value = dp.StringValue ?? "",
                            NullValue = dp.StringValue is null,
                            Status = new Status
                            {
                                Code = (long)dp.Status.Code
                            }
                        });
                    }
                    dataPointCount += finalDps.Datapoints.Count;
                    item.StringDatapoints = finalDps;
                }
                else
                {
                    var finalDps = new NumericDatapoints();
                    foreach (var dp in kvp.Value)
                    {
                        if (dp.IsString) continue;
                        finalDps.Datapoints.Add(new NumericDatapoint
                        {
                            Timestamp = dp.Timestamp,
                            Value = dp.NumericValue ?? 0,
                            NullValue = dp.NumericValue is null,
                            Status = new Status
                            {
                                Code = (long)dp.Status.Code
                            }
                        });
                    }
                    dataPointCount += finalDps.Datapoints.Count;
                    item.NumericDatapoints = finalDps;
                }

                request.Items.Add(item);
            }
            return request;
        }

        /// <summary>
        /// Alpha: Insert datapoints to timeseries. Insertions are chunked and cleaned according to configuration,
        /// and can optionally handle errors. If any timeseries missing from the result and inserted by externalId,
        /// they are created before the points are inserted again.
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
        /// <returns>Results with a list of errors. If TimeSeriesResult is null, no timeseries were attempted created.</returns>
        public static async Task<(CogniteResult<DataPointInsertErrorWithInstanceId> DataPointResult, CogniteResult<SourcedNode<CogniteTimeSeriesBase>, SourcedNodeWrite<CogniteTimeSeriesBase>>? TimeSeriesResult)> InsertAsyncCreateMissing(
            CogniteSdk.Client client,
            IDictionary<IdentityWithInstanceId, IEnumerable<Datapoint>> points,
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
            if (client == null) throw new ArgumentNullException(nameof(client));
            if (points == null) throw new ArgumentNullException(nameof(points));

            var result = await InsertAsync<CogniteTimeSeriesBase>(client, points, keyChunkSize, valueChunkSize, throttleSize,
                timeseriesChunkSize, timeseriesThrottleSize, gzipCountLimit, sanitationMode,
                RetryMode.OnError, nanReplacement, token).ConfigureAwait(false);

            if (result.Errors?.Any(err => err.Type == ErrorType.FatalFailure) ?? false) return (result, null);

            var missingIds = new HashSet<IdentityWithInstanceId>((result.Errors ?? Enumerable.Empty<CogniteError>())
                .Where(err => err.Type == ErrorType.ItemMissing)
                .SelectMany(err => err.Values?.Select(x => (IdentityWithInstanceId)x) ?? Enumerable.Empty<IdentityWithInstanceId>())
                .Where(idt => idt.InstanceId != null));

            if (missingIds.Count == 0) return (result, null);

            _logger.LogInformation("Creating {Count} missing timeseries", missingIds.Count);

            var toCreate = new List<SourcedNodeWrite<CogniteTimeSeriesBase>>();
            foreach (var id in missingIds)
            {
                var dp = points[id].FirstOrDefault();
                if (dp == null) continue;

                bool isString = dp.NumericValue == null;

                toCreate.Add(new SourcedNodeWrite<CogniteTimeSeriesBase>
                {
                    Space = id.InstanceId.Space,
                    ExternalId = id.InstanceId.ExternalId,
                    Properties = new CogniteTimeSeriesBase() { Type = isString ? TimeSeriesType.String : TimeSeriesType.Numeric }
                });
            }

            var tsResult = await client.CoreDataModel.TimeSeries<CogniteTimeSeriesBase>().EnsureTimeSeriesExistsAsync(
                toCreate,
                timeseriesChunkSize,
                timeseriesThrottleSize,
                retryMode,
                sanitationMode,
                token).ConfigureAwait(false);

            if (tsResult.Errors?.Any(err => err.Type != ErrorType.ItemExists) ?? false) return (result, tsResult);

            var pointsToInsert = points.Where(kvp => missingIds.Contains(kvp.Key)).ToDictionary(kvp => kvp.Key, kvp => kvp.Value);

            var result2 = await InsertAsync<CogniteTimeSeriesBase>(client, pointsToInsert, keyChunkSize, valueChunkSize, throttleSize,
                timeseriesChunkSize, timeseriesThrottleSize, gzipCountLimit, sanitationMode,
                RetryMode.OnError, nanReplacement, token).ConfigureAwait(false);

            return (result.Merge(result2), tsResult);
        }


        /// <summary>
        /// Alpha: Insert datapoints to timeseries. Insertions are chunked and cleaned according to configuration,
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
        public static async Task<CogniteResult<DataPointInsertErrorWithInstanceId>> InsertAsync<T>(
            CogniteSdk.Client client,
            IDictionary<IdentityWithInstanceId, IEnumerable<Datapoint>> points,
            int keyChunkSize,
            int valueChunkSize,
            int throttleSize,
            int timeseriesChunkSize,
            int timeseriesThrottleSize,
            int gzipCountLimit,
            SanitationMode sanitationMode,
            RetryMode retryMode,
            double? nanReplacement,
            CancellationToken token) where T : CogniteTimeSeriesBase
        {
            IEnumerable<CogniteError<DataPointInsertErrorWithInstanceId>> errors;
            (points, errors) = CoreTSSanitation.CleanDataPointsRequest(points, sanitationMode, nanReplacement);

            var chunks = points
                .Select(p => (p.Key, p.Value))
                .ChunkBy(valueChunkSize, keyChunkSize)
                .Select(chunk => chunk.ToDictionary(pair => pair.Key, pair => pair.Values))
                .ToList();

            int size = chunks.Count + (errors.Any() ? 1 : 0);
            var results = new CogniteResult<DataPointInsertErrorWithInstanceId>[size];

            if (errors.Any())
            {
                results[size - 1] = new CogniteResult<DataPointInsertErrorWithInstanceId>(errors);
                if (size == 1) return results[size - 1];
            }
            if (size == 0) return new CogniteResult<DataPointInsertErrorWithInstanceId>(null);

            _logger.LogDebug("Inserting timeseries datapoints. Number of timeseries: {Number}. Number of chunks: {Chunks}", points.Count, chunks.Count);
            var generators = chunks
                .Select<IDictionary<IdentityWithInstanceId, IEnumerable<Datapoint>>, Func<Task>>(
                (chunk, idx) => async () =>
                {
                    var result = await
                        InsertDataPointsHandleErrors<T>(client, chunk, timeseriesChunkSize, timeseriesThrottleSize, gzipCountLimit, retryMode, token)
                        .ConfigureAwait(false);
                    results[idx] = result;
                });

            int taskNum = 0;
            await generators.RunThrottled(
                throttleSize,
                (_) =>
                {
                    if (chunks.Count > 1)
                        _logger.LogDebug("{MethodName} completed {NumDone}/{TotalNum} tasks",
                            nameof(InsertAsync), ++taskNum, chunks.Count);
                },
                token).ConfigureAwait(false);

            return CogniteResult<DataPointInsertErrorWithInstanceId>.Merge(results);
        }

        private static async Task<CogniteResult<DataPointInsertErrorWithInstanceId>> InsertDataPointsHandleErrors<T>(
            CogniteSdk.Client client,
            IDictionary<IdentityWithInstanceId, IEnumerable<Datapoint>> points,
            int timeseriesChunkSize,
            int timeseriesThrottleSize,
            int gzipCountLimit,
            RetryMode retryMode,
            CancellationToken token) where T : CogniteTimeSeriesBase
        {
            var errors = new List<CogniteError<DataPointInsertErrorWithInstanceId>>();
            while (points != null && points.Any() && !token.IsCancellationRequested)
            {
                var request = points.ToInsertRequest();
                try
                {
                    bool useGzip = false;
                    int count = request.Items.Sum(r => r.NumericDatapoints?.Datapoints?.Count ?? 0 + r.StringDatapoints?.Datapoints?.Count ?? 0);
                    if (gzipCountLimit >= 0 && count >= gzipCountLimit)
                    {
                        useGzip = true;
                    }

                    if (useGzip)
                    {
                        using (CdfMetrics.Datapoints.WithLabels("create"))
                        {
                            await client.Alpha.DataPoints.CreateAsync(request, CompressionLevel.Fastest, token).ConfigureAwait(false);
                        }
                    }
                    else
                    {
                        using (CdfMetrics.Datapoints.WithLabels("create"))
                        {
                            await client.Alpha.DataPoints.CreateAsync(request, token).ConfigureAwait(false);
                        }
                    }

                    CdfMetrics.NumberDatapoints.Inc(count);

                    _logger.LogDebug("Created {cnt} datapoints for {ts} timeseries in CDF", count, points.Count);
                    return new CogniteResult<DataPointInsertErrorWithInstanceId>(errors);
                }
                catch (Exception ex)
                {
                    _logger.LogDebug("Failed to create datapoints for {seq} timeseries: {msg}", points.Count, ex.Message);
                    var error = ResultHandlers.ParseException<DataPointInsertErrorWithInstanceId>(ex, RequestType.CreateDatapoints);

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
                        if (!error.Complete)
                        {
                            (error, points) = await ResultHandlers
                                .VerifyDatapointsFromCDF(client.CoreDataModel.TimeSeries<T>(), error,
                                    points, timeseriesChunkSize, timeseriesThrottleSize, token)
                                .ConfigureAwait(false);
                            errors.Add(error);
                        }
                        else
                        {
                            errors.Add(error);
                        }
                        points = ResultHandlers.CleanFromError(error, points);
                    }
                }
            }

            return new CogniteResult<DataPointInsertErrorWithInstanceId>(errors);
        }

        /// <summary>
        /// Alpha: Deletes ranges of data points in CDF. The <paramref name="ranges"/> parameter contains the first (inclusive)
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
            this CogniteSdk.Resources.Alpha.DataPointsResource dataPoints,
            IDictionary<IdentityWithInstanceId, IEnumerable<TimeRange>> ranges,
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
                        InstanceId = kvp.Key.InstanceId,
                        Id = kvp.Key.Id,
                        InclusiveBegin = r.First.ToUnixTimeMilliseconds(),
                        ExclusiveEnd = r.Last.ToUnixTimeMilliseconds() + 1 // exclusive
                    })
                );
            }

            var chunks = toDelete
                .ChunkBy(deleteChunkSize)
                .ToList(); // Maximum number of items in the /timeseries/data/delete endpoint.

            var missing = new HashSet<IdentityWithInstanceId>();
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
                (_) =>
                {
                    if (chunks.Count > 1)
                        _logger.LogDebug("{MethodName} completed {NumDone}/{TotalNum} tasks",
                            nameof(DeleteIgnoreErrorsAsync), ++taskNum, chunks.Count);
                },
                token).ConfigureAwait(false);

            return new DeleteError(missing, Enumerable.Empty<IdentityWithInstanceId>());
        }

        private static async Task<HashSet<IdentityWithInstanceId>> DeleteDataPointsIgnoreErrorsChunk(
            CogniteSdk.Resources.Alpha.DataPointsResource dataPoints,
            IEnumerable<IdentityWithRange> chunks,
            CancellationToken token)
        {
            var missing = new HashSet<IdentityWithInstanceId>();
            if (!chunks.Any()) return missing;

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
            catch (CogniteSdk.ResponseException e) when (e.Code == 400 && e.Missing != null && e.Missing.Any())
            {
                CogniteUtils.ExtractMissingFromResponseException(missing, e);
                var remaining = chunks.Where(i => !missing.Contains(new IdentityWithInstanceId(i.InstanceId)));
                var errors = await DeleteDataPointsIgnoreErrorsChunk(dataPoints, remaining, token).ConfigureAwait(false);
                missing.UnionWith(errors);
            }
            return missing;
        }

        /// <summary>
        /// Alpha: Get the last timestamp for each time series given in <paramref name="ids"/> before each given timestamp.
        /// Ignores timeseries not in CDF. The return dictionary contains only ids that exist in CDF.
        /// Note that end limits closer to actual endpoints in CDF is considerably faster.
        /// </summary>
        /// <param name="dataPoints">DataPointsResource to use</param>
        /// <param name="ids">ExternalIds and last timestamp. Let last timestamp be DateTime.MaxValue to use default ("now").</param>
        /// <param name="chunkSize">Number of timeseries per request</param>
        /// <param name="throttleSize">Maximum number of parallel requests</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>Dictionary from externalId to last timestamp, only contains existing timeseries</returns>
        public static async Task<IDictionary<IdentityWithInstanceId, DateTime>> GetLatestTimestamps(
            this CogniteSdk.Resources.Alpha.DataPointsResource dataPoints,
            IEnumerable<(IdentityWithInstanceId id, DateTime before)> ids,
            int chunkSize,
            int throttleSize,
            CancellationToken token)
        {
            var ret = new ConcurrentDictionary<IdentityWithInstanceId, DateTime>();
            var idSet = new HashSet<IdentityWithInstanceId>(ids.Select(id => id.id));

            var chunks = ids
                .Select((pair) =>
                {
                    var id = pair.id;
                    IdentityWithBefore idt = IdentityWithBefore.Create(id.InstanceId);
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
                    IEnumerable<DataPointsItem<CogniteSdk.DataPoint>> dps;
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
                            IdentityWithInstanceId id = new IdentityWithInstanceId(dp.InstanceId);
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
        /// Alpha: Get the first timestamp for each time series given in <paramref name="ids"/> after each given timestamp.
        /// Ignores timeseries not in CDF. The return dictionary contains only ids that exist in CDF.
        /// </summary>
        /// <param name="dataPoints">DataPointsResource to use</param>
        /// <param name="ids">ExternalIds and last timestamp. Let last timestamp be Epoch to use default.</param>
        /// <param name="chunkSize">Number of timeseries per request</param>
        /// <param name="throttleSize">Maximum number of parallel requests</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>Dictionary from externalId to first timestamp, only contains existing timeseries</returns>
        public static async Task<IDictionary<IdentityWithInstanceId, DateTime>> GetEarliestTimestamps(
            this CogniteSdk.Resources.Alpha.DataPointsResource dataPoints,
            IEnumerable<(IdentityWithInstanceId id, DateTime after)> ids,
            int chunkSize,
            int throttleSize,
            CancellationToken token)
        {
            var ret = new ConcurrentDictionary<IdentityWithInstanceId, DateTime>();
            var idSet = new HashSet<IdentityWithInstanceId>(ids.Select(id => id.id));

            var chunks = ids
                .Select((pair) =>
                {
                    var query = new DataPointsQueryItem();
                    query.InstanceId = pair.id.InstanceId;

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
                        IdentityWithInstanceId id = new IdentityWithInstanceId(new InstanceIdentifier(dp.InstanceId.Space, dp.InstanceId.ExternalId));

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
        /// Alpha: Fetches the range of datapoints present in CDF. Limited by given ranges for each id.
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
        public static async Task<IDictionary<IdentityWithInstanceId, TimeRange>> GetExtractedRanges(
            this CogniteSdk.Resources.Alpha.DataPointsResource dataPoints,
            IEnumerable<(IdentityWithInstanceId id, TimeRange limit)> ids,
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
            var tasks = new List<Task<IDictionary<IdentityWithInstanceId, DateTime>>>();
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
