using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extractor.Common;
using CogniteSdk;
using CogniteSdk.DataModels;
using CogniteSdk.DataModels.Core;
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
        /// Insert datapoints to timeseries. Insertions are chunked and cleaned according to configuration,
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
        public static async Task<(CogniteResult<DataPointInsertError> DataPointResult, CogniteResult<SourcedNode<CogniteTimeSeriesBase>, SourcedNodeWrite<CogniteTimeSeriesBase>>? TimeSeriesResult)> InsertAsyncCreateMissing(
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
            if (client == null) throw new ArgumentNullException(nameof(client));
            if (points == null) throw new ArgumentNullException(nameof(points));

            var result = await InsertAsync(client, points, keyChunkSize, valueChunkSize, throttleSize,
                timeseriesChunkSize, timeseriesThrottleSize, gzipCountLimit, sanitationMode,
                RetryMode.OnError, nanReplacement, token).ConfigureAwait(false);

            if (result.Errors?.Any(err => err.Type == ErrorType.FatalFailure) ?? false) return (result, null);

            var missingIds = new HashSet<Identity>((result.Errors ?? Enumerable.Empty<CogniteError>())
                .Where(err => err.Type == ErrorType.ItemMissing)
                .SelectMany(err => err.Values ?? Enumerable.Empty<Identity>())
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

            var result2 = await InsertAsync(client, pointsToInsert, keyChunkSize, valueChunkSize, throttleSize,
                timeseriesChunkSize, timeseriesThrottleSize, gzipCountLimit, sanitationMode,
                RetryMode.OnError, nanReplacement, token).ConfigureAwait(false);

            return (result.Merge(result2), tsResult);
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
                (chunk, idx) => async () =>
                {
                    var result = await
                        InsertDataPointsHandleErrors(client, chunk, timeseriesChunkSize, timeseriesThrottleSize, gzipCountLimit, retryMode, token)
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
                    int count = request.Items.Sum(r => r.NumericDatapoints?.Datapoints?.Count ?? 0 + r.StringDatapoints?.Datapoints?.Count ?? 0);
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
                                .VerifyDatapointsFromCDF(client.CoreDataModel.TimeSeries<CogniteTimeSeriesBase>(), error,
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

            return new CogniteResult<DataPointInsertError>(errors);
        }
    }
}
