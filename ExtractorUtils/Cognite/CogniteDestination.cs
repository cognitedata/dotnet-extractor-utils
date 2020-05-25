using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using CogniteSdk;
using Microsoft.Extensions.Logging;

namespace Cognite.Extractor.Utils
{
    /// <summary>
    /// Class with utility methods supporting extraction of data into CDF.
    /// These methods complement the ones offered by the <see cref="Client"/> and use a
    /// <see cref="CogniteConfig"/> object to determine chunking of data and throttling of
    /// requests against the client
    /// </summary>
    public class CogniteDestination
    {
        private Client _client;
        private ILogger<CogniteDestination> _logger;
        private CogniteConfig _config;

        /// <summary>
        /// Initializes the Cognite destination with the provided parameters
        /// </summary>
        /// <param name="client"><see cref="Client"/> object</param>
        /// <param name="logger">Logger</param>
        /// <param name="config">Configuration object</param>
        public CogniteDestination(Client client, ILogger<CogniteDestination> logger, CogniteConfig config)
        {
            _client = client;
            _logger = logger;
            _config = config;
            CogniteClientExtensions.SetLogger(_logger);
        }

        /// <summary>
        /// Verifies that the currently configured Cognite client can access Cognite Data Fusion
        /// </summary>
        /// <param name="token">Cancellation token</param>
        /// <returns></returns>
        public async Task TestCogniteConfig(CancellationToken token)
        {
            await _client.TestCogniteConfig(_config, token);
        }

        /// <summary>
        /// Ensures the the time series with the provided <paramref name="externalIds"/> exist in CDF.
        /// If one or more do not exist, use the <paramref name="buildTimeSeries"/> function to construct
        /// the missing time series objects and upload them to CDF.
        /// This method uses the <see cref="CogniteConfig"/> object to determine chunking of items and throttling
        /// against CDF 
        /// </summary>
        /// <param name="externalIds">External Ids</param>
        /// <param name="buildTimeSeries">Function that builds <see cref="TimeSeriesCreate"/> objects</param>
        /// <param name="token">Cancellation token</param>
        /// <returns></returns>
        public async Task<IEnumerable<TimeSeries>> GetOrCreateTimeSeriesAsync(
            IEnumerable<string> externalIds,
            Func<IEnumerable<string>, IEnumerable<TimeSeriesCreate>> buildTimeSeries,
            CancellationToken token)
        {
            _logger.LogInformation("Getting or creating {Number} time series in CDF", externalIds.Count());
            return await _client.GetOrCreateTimeSeriesAsync(
                externalIds,
                buildTimeSeries,
                _config.CdfChunking.TimeSeries,
                _config.CdfThrottling.TimeSeries,
                token);
        }

        /// <summary>
        /// Ensures that all time series in <paramref name="timeSeries"/> exist in CDF.
        /// Tries to create the time series and returns when all are created or reported as 
        /// duplicates (already exist in CDF)
        /// </summary>
        /// <param name="timeSeries">List of <see cref="TimeSeriesCreate"/> objects</param>
        /// <param name="token">Cancellation token</param>
        /// <returns></returns>
        public async Task EnsureTimeSeriesExistsAsync(
            IEnumerable<TimeSeriesCreate> timeSeries, 
            CancellationToken token)
        {
            _logger.LogInformation("Ensuring that {Number} time series exist in CDF", timeSeries.Count());
            await _client.EnsureTimeSeriesExistsAsync(
                timeSeries,
                _config.CdfChunking.TimeSeries,
                _config.CdfThrottling.TimeSeries,
                token);
        }

        /// <summary>
        /// Insert the provided data points into CDF. The data points are chunked
        /// according to <see cref="CogniteConfig.CdfChunking"/> and trimmed according to the <see href="https://docs.cognite.com/api/v1/#operation/postMultiTimeSeriesDatapoints">CDF limits</see>.
        /// The <paramref name="points"/> dictionary keys are time series identities (Id or ExternalId) and the values are numeric or string data points
        /// </summary>
        /// <param name="points">Data points</param>
        /// <param name="token">Cancellation token</param>
        public async Task InsertDataPointsAsync(
            IDictionary<Identity, IEnumerable<Datapoint>> points,
            CancellationToken token)
        {
            _logger.LogDebug("Uploading {Number} data points to CDF for {NumberTs} time series", 
                points.Values.Select(dp => dp.Count()).Sum(),
                points.Keys.Count);
            await _client.InsertDataPointsAsync(
                points,
                _config.CdfChunking.DataPointTimeSeries,
                _config.CdfChunking.DataPoints,
                _config.CdfThrottling.DataPoints,
                token);
        }

        /// <summary>
        /// Tries to insert the data points into CDF. If any time series are not
        /// found, or if the time series is of wrong type (Inserting numeric data
        /// into a string time series), the errors are ignored and the missing/mismatched 
        /// ids are returned
        /// </summary>
        /// <param name="points">Data points</param>
        /// <param name="token">Cancellation token</param>
        /// <returns></returns>
        public async Task<InsertError> InsertDataPointsIgnoreErrorsAsync(
            IDictionary<Identity, IEnumerable<Datapoint>> points,
            CancellationToken token)
        {
            _logger.LogDebug("Uploading {Number} data points to CDF for {NumberTs} time series", 
                points.Values.Select(dp => dp.Count()).Sum(),
                points.Keys.Count);
            var errors = await _client.InsertDataPointsIgnoreErrorsAsync(
                points,
                _config.CdfChunking.DataPointTimeSeries,
                _config.CdfChunking.DataPoints,
                _config.CdfThrottling.DataPoints,
                token);
            _logger.LogDebug("Found {NumMissing} missing ids and {NumMismatched} mismatched time series", 
                errors.IdsNotFound.Count(), errors.IdsWithMismatchedData.Count());
            return errors;
        }

        public async Task InsertRawRowsAsync<T>(
            string database, 
            string table, 
            IDictionary<string, T> columns, 
            CancellationToken token)
        {
            _logger.LogDebug("Uploading {Number} rows to CDF Raw. Database: {Db}. Table: {Table}", 
                columns.Count,
                database,
                table);
            await _client.Raw.InsertRowsAsync(
                database,
                table,
                columns,
                _config.CdfChunking.RawRows,
                _config.CdfThrottling.Raw,
                token);
        }

        public IRawUploadQueue<T> CreateUploadQueue<T>(string db, string table, TimeSpan interval, int maxQueueSize = 0)
        {
            return new RawUploadQueue<T>(db, table, this, interval, maxQueueSize, _logger);
        }

    }
}