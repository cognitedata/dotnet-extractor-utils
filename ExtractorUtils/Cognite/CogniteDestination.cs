using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Common;
using Cognite.Extractor.Common;
using CogniteSdk;
using Microsoft.Extensions.Logging;
using TimeRange = Cognite.Extractor.Common.TimeRange;

namespace Cognite.Extractor.Utils
{
    /// <summary>
    /// Class with utility methods supporting extraction of data into CDF.
    /// These methods complement the ones offered by the <see cref="Client"/> and use a
    /// <see cref="CogniteConfig"/> object to determine chunking of data and throttling of
    /// requests against the client
    /// </summary>
    public class CogniteDestination : IRawDestination
    {
        private readonly Client _client;
        private readonly ILogger<CogniteDestination> _logger;
        private readonly CogniteConfig _config;

        /// <summary>
        /// The configured Cognite client used by this destination. Can be used to
        /// access the full Cognite API
        /// </summary>
        public Client CogniteClient => _client;

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
            AssetExtensions.SetLogger(_logger);
            DatapointExtensions.SetLogger(_logger);
            TimeSeriesExtensions.SetLogger(_logger);
            RawExtensions.SetLogger(_logger);
            EventExtensions.SetLogger(_logger);
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

        #region timeseries
        /// <summary>
        /// Ensures the the time series with the provided <paramref name="externalIds"/> exist in CDF.
        /// If one or more do not exist, use the <paramref name="buildTimeSeries"/> function to construct
        /// the missing time series objects and upload them to CDF.
        /// This method uses the <see cref="CogniteConfig"/> object to determine chunking of items and throttling
        /// against CDF 
        /// </summary>
        /// <param name="externalIds">External Ids</param>
        /// <param name="buildTimeSeries">Function that builds CogniteSdk TimeSeriesCreate objects</param>
        /// <param name="token">Cancellation token</param>
        /// <returns></returns>
        public async Task<IEnumerable<TimeSeries>> GetOrCreateTimeSeriesAsync(
            IEnumerable<string> externalIds,
            Func<IEnumerable<string>, IEnumerable<TimeSeriesCreate>> buildTimeSeries,
            CancellationToken token)
        {
            _logger.LogInformation("Getting or creating {Number} time series in CDF", externalIds.Count());
            return await _client.TimeSeries.GetOrCreateTimeSeriesAsync(
                externalIds,
                buildTimeSeries,
                _config.CdfChunking.TimeSeries,
                _config.CdfThrottling.TimeSeries,
                token);
        }
        /// <summary>
        /// Ensures the the time series with the provided <paramref name="externalIds"/> exist in CDF.
        /// If one or more do not exist, use the <paramref name="buildTimeSeries"/> function to construct
        /// the missing time series objects and upload them to CDF.
        /// This method uses the <see cref="CogniteConfig"/> object to determine chunking of items and throttling
        /// against CDF 
        /// </summary>
        /// <param name="externalIds">External Ids</param>
        /// <param name="buildTimeSeries">Async function that builds CogniteSdk TimeSeriesCreate objects</param>
        /// <param name="token">Cancellation token</param>
        /// <returns></returns>
        public async Task<IEnumerable<TimeSeries>> GetOrCreateTimeSeriesAsync(
            IEnumerable<string> externalIds,
            Func<IEnumerable<string>, Task<IEnumerable<TimeSeriesCreate>>> buildTimeSeries,
            CancellationToken token)
        {
            _logger.LogInformation("Getting or creating {Number} time series in CDF", externalIds.Count());
            return await _client.TimeSeries.GetOrCreateTimeSeriesAsync(
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
        /// <param name="timeSeries">List of CogniteSdk TimeSeriesCreate objects</param>
        /// <param name="token">Cancellation token</param>
        /// <returns></returns>
        public async Task EnsureTimeSeriesExistsAsync(
            IEnumerable<TimeSeriesCreate> timeSeries, 
            CancellationToken token)
        {
            _logger.LogInformation("Ensuring that {Number} time series exist in CDF", timeSeries.Count());
            await _client.TimeSeries.EnsureTimeSeriesExistsAsync(
                timeSeries,
                _config.CdfChunking.TimeSeries,
                _config.CdfThrottling.TimeSeries,
                token);
        }
        #endregion

        #region assets
        /// <summary>
        /// Ensures the the assets with the provided <paramref name="externalIds"/> exist in CDF.
        /// If one or more do not exist, use the <paramref name="buildAssets"/> function to construct
        /// the missing asset objects and upload them to CDF.
        /// This method uses the <see cref="CogniteConfig"/> object to determine chunking of items and throttling
        /// against CDF 
        /// </summary>
        /// <param name="externalIds">External Ids</param>
        /// <param name="buildAssets">Function that builds CogniteSdk AssetCreate objects</param>
        /// <param name="token">Cancellation token</param>
        /// <returns></returns>
        public async Task<IEnumerable<Asset>> GetOrCreateAssetsAsync(
            IEnumerable<string> externalIds,
            Func<IEnumerable<string>, IEnumerable<AssetCreate>> buildAssets,
            CancellationToken token)
        {
            _logger.LogInformation("Getting or creating {Number} assets in CDF", externalIds.Count());
            return await _client.Assets.GetOrCreateAsync(
                externalIds,
                buildAssets,
                _config.CdfChunking.Assets,
                _config.CdfThrottling.Assets,
                token);
        }
        /// <summary>
        /// Ensures the the assets with the provided <paramref name="externalIds"/> exist in CDF.
        /// If one or more do not exist, use the <paramref name="buildAssets"/> function to construct
        /// the missing asset objects and upload them to CDF.
        /// This method uses the <see cref="CogniteConfig"/> object to determine chunking of items and throttling
        /// against CDF 
        /// </summary>
        /// <param name="externalIds">External Ids</param>
        /// <param name="buildAssets">Async function that builds CogniteSdk AssetCreate objects</param>
        /// <param name="token">Cancellation token</param>
        /// <returns></returns>
        public async Task<IEnumerable<Asset>> GetOrCreateAssetsAsync(
            IEnumerable<string> externalIds,
            Func<IEnumerable<string>, Task<IEnumerable<AssetCreate>>> buildAssets,
            CancellationToken token)
        {
            _logger.LogInformation("Getting or creating {Number} assets in CDF", externalIds.Count());
            return await _client.Assets.GetOrCreateAsync(
                externalIds,
                buildAssets,
                _config.CdfChunking.Assets,
                _config.CdfThrottling.Assets,
                token);
        }

        /// <summary>
        /// Ensures that all assets in <paramref name="assets"/> exist in CDF.
        /// Tries to create the assets and returns when all are created or reported as 
        /// duplicates (already exist in CDF)
        /// </summary>
        /// <param name="assets">List of CogniteSdk AssetCreate objects</param>
        /// <param name="token">Cancellation token</param>
        /// <returns></returns>
        public async Task EnsureAssetsExistsAsync(
            IEnumerable<AssetCreate> assets,
            CancellationToken token)
        {
            _logger.LogInformation("Ensuring that {Number} assets exist in CDF", assets.Count());
            await _client.Assets.EnsureExistsAsync(
                assets,
                _config.CdfChunking.Assets,
                _config.CdfThrottling.Assets,
                token);
        }
        #endregion

        #region datapoints
        /// <summary>
        /// Insert the provided data points into CDF. The data points are chunked
        /// according to <see cref="CogniteConfig.CdfChunking"/> and trimmed according to the
        /// <see href="https://docs.cognite.com/api/v1/#operation/postMultiTimeSeriesDatapoints">CDF limits</see>.
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
            await _client.DataPoints.InsertAsync(
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
            if (errors.IdsNotFound.Any() || errors.IdsWithMismatchedData.Any()) {
                    _logger.LogDebug("Found {NumMissing} missing ids and {NumMismatched} mismatched time series", 
                errors.IdsNotFound.Count(), errors.IdsWithMismatchedData.Count());
			}
            return errors;
        }

        /// <summary>
        /// Deletes ranges of data points in CDF. The <paramref name="ranges"/> parameter contains the first (inclusive)
        /// and last (inclusive) timestamps for the range. After the delete request is sent to CDF, attempt to confirm that
        /// the data points were deleted by querying the time range. Deletes in CDF are eventually consistent, failing to 
        /// confirm the deletion does not mean that the operation failed in CDF
        /// </summary>
        /// <param name="ranges">Ranges to delete</param>
        /// <param name="token">Cancelation token</param>
        /// <returns>A <see cref="DeleteError"/> object with any missing ids or ids with unconfirmed deletes</returns>
        public async Task<DeleteError> DeleteDataPointsIgnoreErrorsAsync(
            IDictionary<Identity, IEnumerable<Common.TimeRange>> ranges,
            CancellationToken token)
        {
            _logger.LogDebug("Deleting data points in CDF for {NumberTs} time series", 
                ranges.Keys.Count);
            var errors = await _client.DataPoints.DeleteIgnoreErrorsAsync(
                ranges,
                _config.CdfChunking.DataPointDelete,
                _config.CdfChunking.DataPointList,
                _config.CdfThrottling.DataPoints,
                _config.CdfThrottling.DataPoints,
                token);
            _logger.LogDebug("During deletion, {NumMissing} ids where not found and {NumNotConfirmed} range deletions could not be confirmed", 
                errors.IdsNotFound.Count(), errors.IdsDeleteNotConfirmed.Count());
            return errors;
        }
        #endregion

        #region raw
        /// <summary>
        /// Insert the provided <paramref name="rows"/> into CDF Raw. The rows are a dictionary of 
        /// keys and DTOs (data type objects). The DTOs  of type <typeparamref name="T"/> are serialized to JSON 
        /// before they are sent to Raw. If the <paramref name="database"/> or <paramref name="table"/> do not
        /// exist, they are created
        /// </summary>
        /// <param name="database">Raw database name</param>
        /// <param name="table">Raw table name</param>
        /// <param name="rows">Rows of keys and columns</param>
        /// <param name="options">Optional JSON options parameter, to be used when converting dto to JsonElement</param>
        /// <param name="token">Cancellation token</param>
        /// <typeparam name="T">DTO type</typeparam>
        /// <returns>Task</returns>
        public async Task InsertRawRowsAsync<T>(
            string database,
            string table,
            IDictionary<string, T> rows,
            JsonSerializerOptions options,
            CancellationToken token)
        {
            _logger.LogDebug("Uploading {Number} rows to CDF Raw. Database: {Db}. Table: {Table}",
                rows.Count,
                database,
                table);
            await _client.Raw.InsertRowsAsync(
                database,
                table,
                rows,
                _config.CdfChunking.RawRows,
                _config.CdfThrottling.Raw,
                token,
                options);
        }

        /// <summary>
        /// Insert the provided <paramref name="rows"/> into CDF Raw. The rows are a dictionary of 
        /// keys and DTOs (data type objects). The DTOs  of type <typeparamref name="T"/> are serialized to JSON 
        /// before they are sent to Raw. If the <paramref name="database"/> or <paramref name="table"/> do not
        /// exist, they are created
        /// </summary>
        /// <param name="database">Raw database name</param>
        /// <param name="table">Raw table name</param>
        /// <param name="rows">Rows of keys and columns</param>
        /// <param name="token">Cancellation token</param>
        /// <typeparam name="T">DTO type</typeparam>
        /// <returns>Task</returns>
        public async Task InsertRawRowsAsync<T>(
            string database, 
            string table, 
            IDictionary<string, T> rows, 
            CancellationToken token)
        {
            _logger.LogDebug("Uploading {Number} rows to CDF Raw. Database: {Db}. Table: {Table}", 
                rows.Count,
                database,
                table);
            await _client.Raw.InsertRowsAsync(
                database,
                table,
                rows,
                _config.CdfChunking.RawRows,
                _config.CdfThrottling.Raw,
                token);
        }

        /// <summary>
        /// Creates a Raw upload queue. It can be used to queue DTOs (data type objects) of type <typeparamref name="T"/>
        /// before sending them to CDF Raw. The items are dequeued and uploaded every <paramref name="interval"/>. If <paramref name="maxQueueSize"/> is
        /// greater than zero, the queue will have a maximum size, and items are also uploaded as soon as the maximum size is reached.
        /// To start the upload loop, use the <see cref="IRawUploadQueue{T}.Start(CancellationToken)"/> method. To stop it, dispose of the queue or
        /// cancel the token
        /// </summary>
        /// <param name="database">Raw database name</param>
        /// <param name="table">Raw table name</param>
        /// <param name="interval">Upload interval</param>
        /// <param name="maxQueueSize">Maximum queue size</param>
        /// <typeparam name="T">Type of the DTO</typeparam>
        /// <returns>An upload queue object</returns>
        public IRawUploadQueue<T> CreateRawUploadQueue<T>(string database, string table, TimeSpan interval, int maxQueueSize = 0)
        {
            return new RawUploadQueue<T>(database, table, this, interval, maxQueueSize, _logger);
        }

        /// <summary>
        /// Returns all rows from the given database and table
        /// </summary>
        /// <param name="dbName">Database to read from</param>
        /// <param name="tableName">Table to read from</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>All rows</returns>
        public Task<IDictionary<string, IDictionary<string, JsonElement>>> GetRowsAsync(
            string dbName,
            string tableName,
            CancellationToken token)
        {
            _logger.LogDebug("Fetching all rows from database {db}, table {table}", dbName, tableName);
            return _client.Raw.GetRowsAsync(dbName, tableName, _config.CdfChunking.RawRows, token);
        }

        /// <summary>
        /// Delete the given rows from raw database
        /// </summary>
        /// <param name="dbName">Database to delete from</param>
        /// <param name="tableName">Table to delete from</param>
        /// <param name="rowKeys">Keys for rows to delete</param>
        /// <param name="token">Cancellation token</param>
        /// <returns></returns>
        public Task DeleteRowsAsync(string dbName, string tableName, IEnumerable<string> rowKeys, CancellationToken token)
        {
            _logger.LogDebug("Deleting {count} rows from database {db}, table {table}", rowKeys.Count(), dbName, tableName);
            return _client.Raw.DeleteRowsAsync(dbName, tableName, rowKeys, _config.CdfChunking.RawRows, _config.CdfThrottling.Raw, token);
        }
        #endregion

        #region ranges

        /// <summary>
        /// Fetches the range of datapoints present in CDF. Limited by given ranges for each id.
        /// Note that end limits closer to actual endpoints in CDF is considerably faster.
        /// </summary>
        /// <param name="ids">ExternalIds and start/end of region to look for datapoints.
        /// Use TimeRange.Complete for first after epoch, and last before now.</param>
        /// <param name="token">Cancellation token</param>
        /// <param name="earliest">If true, fetch earliest timestamps, default true</param>
        /// <param name="latest">If true, fetch latest timestamps, default true</param>
        /// <returns></returns>
        public Task<IDictionary<Identity, TimeRange>> GetExtractedRanges(
            IEnumerable<Identity> ids,
            CancellationToken token,
            bool earliest = true,
            bool latest = true
            )
        {
            return _client.DataPoints.GetExtractedRanges(
                ids.Select(id => (id, TimeRange.Complete)).ToList(),
                _config.CdfChunking.DataPointList,
                _config.CdfChunking.DataPointLatest,
                _config.CdfThrottling.Ranges,
                latest,
                earliest,
                token);
        }


        /// <summary>
        /// Fetches the range of datapoints present in CDF. Limited by given ranges for each id.
        /// Note that end limits closer to actual endpoints in CDF is considerably faster.
        /// </summary>
        /// <param name="ids">ExternalIds and start/end of region to look for datapoints.
        /// Use TimeRange.Complete for first after epoch, and last before now.</param>
        /// <param name="token">Cancellation token</param>
        /// <param name="earliest">If true, fetch earliest timestamps, default true</param>
        /// <param name="latest">If true, fetch latest timestamps, default true</param>
        /// <returns></returns>
        public Task<IDictionary<Identity, TimeRange>> GetExtractedRanges(
            IEnumerable<(Identity id, TimeRange limit)> ids,
            CancellationToken token,
            bool earliest = true,
            bool latest = true
            )
        {
            return _client.DataPoints.GetExtractedRanges(
                ids,
                _config.CdfChunking.DataPointList,
                _config.CdfChunking.DataPointLatest,
                _config.CdfThrottling.Ranges,
                latest,
                earliest,
                token);
        }


        #endregion

        #region events
        /// <summary>
        /// Ensures the the events with the provided <paramref name="externalIds"/> exist in CDF.
        /// If one or more do not exist, use the <paramref name="buildEvents"/> function to construct
        /// the missing event objects and upload them to CDF.
        /// This method uses the <see cref="CogniteConfig"/> object to determine chunking of items and throttling
        /// against CDF 
        /// </summary>
        /// <param name="externalIds">External Ids</param>
        /// <param name="buildEvents">Function that builds CogniteSdk EventCreate objects</param>
        /// <param name="token">Cancellation token</param>
        /// <returns></returns>
        public async Task<IEnumerable<Event>> GetOrCreateEventsAsync(
            IEnumerable<string> externalIds,
            Func<IEnumerable<string>, IEnumerable<EventCreate>> buildEvents,
            CancellationToken token)
        {
            _logger.LogInformation("Getting or creating {Number} events in CDF", externalIds.Count());
            return await _client.Events.GetOrCreateAsync(
                externalIds,
                buildEvents,
                _config.CdfChunking.Events,
                _config.CdfThrottling.Events,
                token);
        }
        /// <summary>
        /// Ensures the the events with the provided <paramref name="externalIds"/> exist in CDF.
        /// If one or more do not exist, use the <paramref name="buildEvents"/> function to construct
        /// the missing event objects and upload them to CDF.
        /// This method uses the <see cref="CogniteConfig"/> object to determine chunking of items and throttling
        /// against CDF 
        /// </summary>
        /// <param name="externalIds">External Ids</param>
        /// <param name="buildEvents">Async function that builds CogniteSdk EventCreate objects</param>
        /// <param name="token">Cancellation token</param>
        /// <returns></returns>
        public async Task<IEnumerable<Event>> GetOrCreateEventsAsync(
            IEnumerable<string> externalIds,
            Func<IEnumerable<string>, Task<IEnumerable<EventCreate>>> buildEvents,
            CancellationToken token)
        {
            _logger.LogInformation("Getting or creating {Number} events in CDF", externalIds.Count());
            return await _client.Events.GetOrCreateAsync(
                externalIds,
                buildEvents,
                _config.CdfChunking.Events,
                _config.CdfThrottling.Events,
                token);
        }

        /// <summary>
        /// Ensures that all events in <paramref name="events"/> exist in CDF.
        /// Tries to create the events and returns when all are created or reported as 
        /// duplicates (already exist in CDF)
        /// </summary>
        /// <param name="events">List of CogniteSdk EventCreate objects</param>
        /// <param name="failOnError">Throw if an error other than duplicate events in CDF occurs.</param>
        /// <param name="token">Cancellation token</param>
        /// <returns></returns>
        public async Task EnsureEventsExistsAsync(
            IEnumerable<EventCreate> events,
            bool failOnError,
            CancellationToken token)
        {
            _logger.LogInformation("Ensuring that {Number} events exist in CDF", events.Count());
            await _client.Events.EnsureExistsAsync(
                events,
                _config.CdfChunking.Events,
                _config.CdfThrottling.Events,
                failOnError,
                token);
        }
        #endregion
    }
}