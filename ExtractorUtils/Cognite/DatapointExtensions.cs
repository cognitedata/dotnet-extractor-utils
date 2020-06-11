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
    }
}
