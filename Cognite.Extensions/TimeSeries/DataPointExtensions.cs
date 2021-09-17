using Cognite.Extractor.Common;
using CogniteSdk;
using Com.Cognite.V1.Timeseries.Proto;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.Extensions
{
    /// <summary>
    /// Extensions to datapoints
    /// </summary>
    public static class DataPointExtensions
    {
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
        /// <param name="sanitationMode">How to sanitize datapoints</param>
        /// <param name="retryMode">How to handle retries</param>
        /// <param name="nonFiniteReplacement">Optional replacement for NaN or Infinite double values</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>Result with a list of errors</returns>
        public static async Task<CogniteResult> InsertDataPointsAsync(
            Client client,
            IDictionary<Identity, IEnumerable<Datapoint>> points,
            int keyChunkSize,
            int valueChunkSize,
            int throttleSize,
            int timeseriesChunkSize,
            int timeseriesThrottleSize,
            SanitationMode sanitationMode,
            RetryMode retryMode,
            double? nonFiniteReplacement,
            CancellationToken token)
        {
            IEnumerable<CogniteError> errors;
            (points, errors) = Sanitation.CleanDataPointsRequest(points, sanitationMode, nonFiniteReplacement);

            var chunks = points
                .Select(p => (p.Key, p.Value))
                .ChunkBy(valueChunkSize, keyChunkSize)
                .Select(chunk => chunk.ToDictionary(pair => pair.Key, pair => pair.Values))
                .ToList();

            int size = chunks.Count + (errors.Any() ? 1 : 0);
            var results = new CogniteResult[size];

            if (errors.Any())
            {
                results[size - 1] = new CogniteResult(errors);
                if (size == 1) return results[size - 1];
            }
            if (size == 0) return new CogniteResult(null);

            _logger.LogDebug("Inserting timeseries datapoints. Number of timeseries: {Number}. Number of chunks: {Chunks}", points.Count, chunks.Count);
            var generators = chunks
                .Select<IDictionary<Identity, IEnumerable<Datapoint>>, Func<Task>>(
                (chunk, idx) => async () => {
                    var result = await
                        InsertDataPointsHandleErrors(client, points, timeseriesChunkSize, timeseriesThrottleSize, retryMode, token)
                        .ConfigureAwait(false);
                    results[idx] = result;
                });

            int taskNum = 0;
            await generators.RunThrottled(
                throttleSize,
                (_) => {
                    if (chunks.Count > 1)
                        _logger.LogDebug("{MethodName} completed {NumDone}/{TotalNum} tasks",
                            nameof(InsertDataPointsAsync), ++taskNum, chunks.Count);
                },
                token).ConfigureAwait(false);

            return CogniteResult.Merge(results);
        }

        private static async Task<CogniteResult> InsertDataPointsHandleErrors(
            Client client,
            IDictionary<Identity, IEnumerable<Datapoint>> points,
            int timeseriesChunkSize,
            int timeseriesThrottleSize,
            RetryMode retryMode,
            CancellationToken token)
        {
            var errors = new List<CogniteError>();
            while (points != null && points.Any() && !token.IsCancellationRequested)
            {
                var request = points.ToInsertRequest();
                try
                {
                    using (CdfMetrics.Datapoints.WithLabels("create"))
                    {
                        await client.DataPoints.CreateAsync(request, token).ConfigureAwait(false);
                    }

                    _logger.LogDebug("Created {rows} datapoints for {seq} timeseries in CDF", points.Sum(ts => ts.Value.Count()), points.Count);
                    return new CogniteResult(errors);
                }
                catch (Exception ex)
                {
                    _logger.LogDebug("Failed to create datapoints for {seq} timeseries", points.Count);
                    var error = ResultHandlers.ParseException(ex, RequestType.CreateDatapoints);
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
                        }
                        points = ResultHandlers.CleanFromError(error, points);

                    }
                }
            }

            return new CogniteResult(errors);
        }
    }
}
