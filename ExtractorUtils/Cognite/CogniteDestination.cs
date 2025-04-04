using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extensions;
using Cognite.Extensions.DataModels.CogniteExtractorExtensions;
using Cognite.Extractor.Common;
using Cognite.Extractor.StateStorage;
using Cognite.Extractor.Utils.Unstable.Configuration;
using CogniteSdk;
using CogniteSdk.Beta;
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
        private readonly ChunkingConfig _chunking;
        private readonly ThrottlingConfig _throttling;
        private readonly string _project;
        private readonly double? _nanReplacement;

        /// <summary>
        /// Configuration for throttling.
        /// </summary>
        protected ThrottlingConfig Throttling => _throttling;
        /// <summary>
        /// Configuration for chunking.
        /// </summary>
        protected ChunkingConfig Chunking => _chunking;
        /// <summary>
        /// Configuration for replacing NaN with a number.
        /// </summary>
        protected double? NanReplacement => _nanReplacement;

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
            if (config == null) throw new ArgumentNullException(nameof(config));
            _client = client;
            _logger = logger;
            _chunking = config.CdfChunking;
            _throttling = config.CdfThrottling;
            _project = config.Project ?? throw new ConfigurationException("Missing project");
            _nanReplacement = config.NanReplacement;
        }

        /// <summary>
        /// Initializes the Cognite destination with the provided parameters
        /// </summary>
        /// <param name="client"><see cref="Client"/> object</param>
        /// <param name="logger">Logger</param>
        /// <param name="config">Configuration object</param>
        /// <param name="project">Configured project</param>
        public CogniteDestination(Client client, ILogger<CogniteDestination> logger, BaseCogniteConfig config, string project)
        {
            if (config == null) throw new ArgumentNullException(nameof(config));
            _client = client;
            _logger = logger;
            _chunking = config.CdfChunking;
            _throttling = config.CdfThrottling;
            _project = project ?? throw new ConfigurationException("Missing project");
            _nanReplacement = config.NanReplacement;
        }

        /// <summary>
        /// Verifies that the currently configured Cognite client can access Cognite Data Fusion
        /// </summary>
        /// <param name="token">Cancellation token</param>
        /// <exception cref="CogniteUtilsException">Thrown when credentials are invalid</exception>
        public async Task TestCogniteConfig(CancellationToken token)
        {
            await _client.TestCogniteConfig(_project, token).ConfigureAwait(false);
        }

        #region timeseries
        /// <summary>
        /// Ensures the the time series with the provided <paramref name="externalIds"/> exist in CDF.
        /// If one or more do not exist, use the <paramref name="buildTimeSeries"/> function to construct
        /// the missing time series objects and upload them to CDF.
        /// This method uses the <see cref="CogniteConfig"/> object to determine chunking of items and throttling
        /// against CDF
        /// If any items fail to be created due to missing asset, duplicated externalId, duplicated
        /// legacy name, or missing dataSetId, they can be removed before retrying by setting
        /// <paramref name="retryMode"/>
        /// </summary>
        /// <param name="externalIds">External Ids</param>
        /// <param name="buildTimeSeries">Function that builds CogniteSdk TimeSeriesCreate objects</param>
        /// <param name="retryMode">How to handle failed requests</param>
        /// <param name="sanitationMode">The type of sanitation to apply to timeseries before creating</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>A <see cref="CogniteResult{TResult, TError}"/> containing errors that occurred and a list of the created and found timeseries</returns>
        public async Task<CogniteResult<TimeSeries, TimeSeriesCreate>> GetOrCreateTimeSeriesAsync(
            IEnumerable<string> externalIds,
            Func<IEnumerable<string>, IEnumerable<TimeSeriesCreate>> buildTimeSeries,
            RetryMode retryMode,
            SanitationMode sanitationMode,
            CancellationToken token)
        {
            _logger.LogInformation("Getting or creating {Number} time series in CDF", externalIds.Count());
            return await _client.TimeSeries.GetOrCreateTimeSeriesAsync(
                externalIds,
                buildTimeSeries,
                _chunking.TimeSeries,
                _throttling.TimeSeries,
                retryMode,
                sanitationMode,
                token).ConfigureAwait(false);
        }
        /// <summary>
        /// Ensures the the time series with the provided <paramref name="externalIds"/> exist in CDF.
        /// If one or more do not exist, use the <paramref name="buildTimeSeries"/> function to construct
        /// the missing time series objects and upload them to CDF.
        /// This method uses the <see cref="CogniteConfig"/> object to determine chunking of items and throttling
        /// against CDF
        /// By default, if any items fail to be created due to missing asset, duplicated externalId, duplicated
        /// legacy name, or missing dataSetId, they can be removed before retrying by setting
        /// <paramref name="retryMode"/>
        /// </summary>
        /// <param name="externalIds">External Ids</param>
        /// <param name="buildTimeSeries">Async function that builds CogniteSdk TimeSeriesCreate objects</param>
        /// <param name="retryMode">How to handle failed requests</param>
        /// <param name="sanitationMode">The type of sanitation to apply to timeseries before creating</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>A <see cref="CogniteResult{TResult, TError}"/> containing errors that occured and a list of the created and found timeseries</returns>
        public async Task<CogniteResult<TimeSeries, TimeSeriesCreate>> GetOrCreateTimeSeriesAsync(
            IEnumerable<string> externalIds,
            Func<IEnumerable<string>, Task<IEnumerable<TimeSeriesCreate>>> buildTimeSeries,
            RetryMode retryMode,
            SanitationMode sanitationMode,
            CancellationToken token)
        {
            _logger.LogInformation("Getting or creating {Number} time series in CDF", externalIds.Count());
            return await _client.TimeSeries.GetOrCreateTimeSeriesAsync(
                externalIds,
                buildTimeSeries,
                _chunking.TimeSeries,
                _throttling.TimeSeries,
                retryMode,
                sanitationMode,
                token).ConfigureAwait(false);
        }

        /// <summary>
        /// Ensures that all time series in <paramref name="timeSeries"/> exist in CDF.
        /// Tries to create the time series and returns when all are created or have been removed
        /// due to issues with the request.
        /// By default, if any items fail to be created due to missing asset, duplicated externalId, duplicated
        /// legacy name, or missing dataSetId, they can be removed before retrying by setting
        /// <paramref name="retryMode"/>
        /// Timeseries will be returned in the same order as given in <paramref name="timeSeries"/>
        /// </summary>
        /// <param name="timeSeries">List of CogniteSdk TimeSeriesCreate objects</param>
        /// <param name="retryMode">How to do retries. Keeping duplicates is not valid for
        /// this method.</param>
        /// <param name="sanitationMode">The type of sanitation to apply to timeseries before creating</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>A <see cref="CogniteResult{TResult, TError}"/> containing errors that occured and a list of the created timeseries</returns>
        public async Task<CogniteResult<TimeSeries, TimeSeriesCreate>> EnsureTimeSeriesExistsAsync(
            IEnumerable<TimeSeriesCreate> timeSeries,
            RetryMode retryMode,
            SanitationMode sanitationMode,
            CancellationToken token)
        {
            _logger.LogInformation("Ensuring that {Number} time series exist in CDF", timeSeries.Count());
            return await _client.TimeSeries.EnsureTimeSeriesExistsAsync(
                timeSeries,
                _chunking.TimeSeries,
                _throttling.TimeSeries,
                retryMode,
                sanitationMode,
                token).ConfigureAwait(false);
        }

        /// <summary>
        /// Update timeseries in <paramref name="updates"/>.
        /// If items fail due to bad asset ids, id, or dataset they can be removed before retrying
        /// by setting <paramref name="retryMode"/>.
        /// TimeSeries will be returned in the same order as given.
        /// </summary>
        /// <param name="updates">List of CogniteSdk TimeSeriesUpdateItem objects</param>
        /// <param name="retryMode">How to do retries. Keeping duplicates is not valid for this method.</param>
        /// <param name="sanitationMode">The type of sanitation to apply to assets before updating</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>A <see cref="CogniteResult{TResult, TError}"/> containing errors that occured and a list of the updated timeseries</returns>
        public async Task<CogniteResult<TimeSeries, TimeSeriesUpdateItem>> UpdateTimeSeriesAsync(
            IEnumerable<TimeSeriesUpdateItem> updates,
            RetryMode retryMode,
            SanitationMode sanitationMode,
            CancellationToken token)
        {
            _logger.LogInformation("Updating {Number} timeseries in CDF", updates.Count());
            return await _client.TimeSeries.UpdateAsync(
                updates,
                _chunking.TimeSeries,
                _throttling.TimeSeries,
                retryMode,
                sanitationMode,
                token).ConfigureAwait(false);
        }

        /// <summary>
        /// Insert or update a list of timeseries, handling errors that come up during both insert and update.
        /// Only timeseries that differ from timeseries in CDF are updated.
        /// 
        /// All given timeseries must have an external id, so it is not in practice possible to use this to change
        /// the externalId of timeseries.
        /// 
        /// Timeseries are returned in the same order as given.
        /// </summary>
        /// <param name="upserts">Assets to upsert</param>
        /// <param name="retryMode">How to handle retries on errors</param>
        /// <param name="sanitationMode">How to sanitize creates and updates</param>
        /// <param name="options">How to update existing assets</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>Result with failed creates/updates and list of assets</returns>
        /// <exception cref="ArgumentException">All upserted assets must have external id</exception>
        public async Task<CogniteResult<TimeSeries, TimeSeriesCreate>> UpsertTimeSeriesAsync(
            IEnumerable<TimeSeriesCreate> upserts,
            RetryMode retryMode,
            SanitationMode sanitationMode,
            UpsertParams? options,
            CancellationToken token)
        {
            _logger.LogInformation("Upserting {Number} timseries in CDF", upserts.Count());
            return await _client.TimeSeries.UpsertAsync(
                upserts,
                _chunking.TimeSeries,
                _throttling.TimeSeries,
                retryMode,
                sanitationMode,
                options,
                token).ConfigureAwait(false);
        }

        #endregion

        #region assets
        /// <summary>
        /// Ensures the the assets with the provided <paramref name="externalIds"/> exist in CDF.
        /// If one or more do not exist, use the <paramref name="buildAssets"/> function to construct
        /// the missing asset objects and upload them to CDF.
        /// This method uses the <see cref="CogniteConfig"/> object to determine chunking of items and throttling
        /// against CDF
        /// If any items fail to be created due to missing parent, duplicated externalId or missing dataset
        /// they can be removed before retrying by setting <paramref name="retryMode"/>
        /// </summary>
        /// <param name="externalIds">External Ids</param>
        /// <param name="buildAssets">Function that builds CogniteSdk AssetCreate objects</param>
        /// <param name="retryMode">How to handle failed requests</param>
        /// <param name="sanitationMode">The type of sanitation to apply to assets before creating</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>A <see cref="CogniteResult{TResult, TError}"/> containing errors that occured and a list of the created and found assets</returns>
        public async Task<CogniteResult<Asset, AssetCreate>> GetOrCreateAssetsAsync(
            IEnumerable<string> externalIds,
            Func<IEnumerable<string>, IEnumerable<AssetCreate>> buildAssets,
            RetryMode retryMode,
            SanitationMode sanitationMode,
            CancellationToken token)
        {
            _logger.LogInformation("Getting or creating {Number} assets in CDF", externalIds.Count());
            return await _client.Assets.GetOrCreateAsync(
                externalIds,
                buildAssets,
                _chunking.Assets,
                _throttling.Assets,
                retryMode,
                sanitationMode,
                token).ConfigureAwait(false);
        }
        /// <summary>
        /// Ensures the the assets with the provided <paramref name="externalIds"/> exist in CDF.
        /// If one or more do not exist, use the <paramref name="buildAssets"/> function to construct
        /// the missing asset objects and upload them to CDF.
        /// This method uses the <see cref="CogniteConfig"/> object to determine chunking of items and throttling
        /// against CDF
        /// If any items fail to be created due to missing parent, duplicated externalId or missing dataset
        /// they can be removed before retrying by setting <paramref name="retryMode"/>
        /// </summary>
        /// <param name="externalIds">External Ids</param>
        /// <param name="buildAssets">Async function that builds CogniteSdk AssetCreate objects</param>
        /// <param name="retryMode">How to handle failed requests</param>
        /// <param name="sanitationMode">The type of sanitation to apply to assets before creating</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>A <see cref="CogniteResult{TResult, TError}"/> containing errors that occured and a list of the created and found assets</returns>
        public async Task<CogniteResult<Asset, AssetCreate>> GetOrCreateAssetsAsync(
            IEnumerable<string> externalIds,
            Func<IEnumerable<string>, Task<IEnumerable<AssetCreate>>> buildAssets,
            RetryMode retryMode,
            SanitationMode sanitationMode,
            CancellationToken token)
        {
            _logger.LogInformation("Getting or creating {Number} assets in CDF", externalIds.Count());
            return await _client.Assets.GetOrCreateAsync(
                externalIds,
                buildAssets,
                _chunking.Assets,
                _throttling.Assets,
                retryMode,
                sanitationMode,
                token).ConfigureAwait(false);
        }

        /// <summary>
        /// Ensures that all assets in <paramref name="assets"/> exist in CDF.
        /// Tries to create the assets and returns when all are created or have been removed
        /// due to issues with the request.
        /// By default, if any items fail to be created due to missing parent, duplicated externalId or missing dataset
        /// they can be removed before retrying by setting <paramref name="retryMode"/>.
        /// Assets will be returned in the same order as given in <paramref name="assets"/>.
        /// </summary>
        /// <param name="assets">List of CogniteSdk AssetCreate objects</param>
        /// <param name="retryMode">How to do retries. Keeping duplicates is not valid for
        /// this method.</param>
        /// <param name="sanitationMode">The type of sanitation to apply to assets before creating</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>A <see cref="CogniteResult{TResult, TError}"/> containing errors that occured and a list of the created assets</returns>
        public async Task<CogniteResult<Asset, AssetCreate>> EnsureAssetsExistsAsync(
            IEnumerable<AssetCreate> assets,
            RetryMode retryMode,
            SanitationMode sanitationMode,
            CancellationToken token)
        {
            _logger.LogInformation("Ensuring that {Number} assets exist in CDF", assets.Count());
            return await _client.Assets.EnsureExistsAsync(
                assets,
                _chunking.Assets,
                _throttling.Assets,
                retryMode,
                sanitationMode,
                token).ConfigureAwait(false);
        }

        /// <summary>
        /// Update assets in <paramref name="updates"/>.
        /// If items fail due to bad parent ids, id, labels or dataset they can be removed before retrying
        /// by setting <paramref name="retryMode"/>.
        /// Assets will be returned in the same order as given.
        /// </summary>
        /// <param name="updates">List of CogniteSdk AssetUpdateItem objects</param>
        /// <param name="retryMode">How to do retries. Keeping duplicates is not valid for this method.</param>
        /// <param name="sanitationMode">The type of sanitation to apply to assets before updating</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>A <see cref="CogniteResult{TResult, TError}"/> containing errors that occured and a list of the updated assets</returns>
        public async Task<CogniteResult<Asset, AssetUpdateItem>> UpdateAssetsAsync(
            IEnumerable<AssetUpdateItem> updates,
            RetryMode retryMode,
            SanitationMode sanitationMode,
            CancellationToken token)
        {
            _logger.LogInformation("Updating {Number} assets in CDF", updates.Count());
            return await _client.Assets.UpdateAsync(
                updates,
                _chunking.Assets,
                _throttling.Assets,
                retryMode,
                sanitationMode,
                token).ConfigureAwait(false);
        }

        /// <summary>
        /// Insert or update a list of assets, handling errors that come up during both insert and update.
        /// Only assets that differ from assets in CDF are updated.
        /// 
        /// All given assets must have an external id, so it is not in practice possible to use this to change
        /// the externalId of assets.
        /// 
        /// Assets are returned in the same order as given.
        /// </summary>
        /// <param name="upserts">Assets to upsert</param>
        /// <param name="retryMode">How to handle retries on errors</param>
        /// <param name="sanitationMode">How to sanitize creates and updates</param>
        /// <param name="options">How to update existing assets</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>Result with failed creates/updates and list of assets</returns>
        /// <exception cref="ArgumentException">All upserted assets must have external id</exception>
        public async Task<CogniteResult<Asset, AssetCreate>> UpsertAssetsAsync(
            IEnumerable<AssetCreate> upserts,
            RetryMode retryMode,
            SanitationMode sanitationMode,
            UpsertParams? options,
            CancellationToken token)
        {
            _logger.LogInformation("Upserting {Number} assets in CDF", upserts.Count());
            return await _client.Assets.UpsertAsync(
                upserts,
                _chunking.Assets,
                _throttling.Assets,
                retryMode,
                sanitationMode,
                options,
                token).ConfigureAwait(false);
        }

        #endregion

        #region datapoints
        /// <summary>
        /// Insert the provided data points into CDF. The data points are chunked
        /// according to <see cref="CogniteConfig.CdfChunking"/> and trimmed according to the
        /// <see href="https://docs.cognite.com/api/v1/#operation/postMultiTimeSeriesDatapoints">CDF limits</see>.
        /// The <paramref name="points"/> dictionary keys are time series identities (Id or ExternalId) and the values are numeric or string data points
        /// 
        /// On error, the offending timeseries/datapoints can optionally be removed.
        /// </summary>
        /// <param name="points">Data points</param>
        /// <param name="sanitationMode"></param>
        /// <param name="retryMode"></param>
        /// <param name="token">Cancellation token</param>
        public async Task<CogniteResult<DataPointInsertError>> InsertDataPointsAsync(
            IDictionary<Identity, IEnumerable<Datapoint>>? points,
            SanitationMode sanitationMode,
            RetryMode retryMode,
            CancellationToken token)
        {
            if (points == null || !points.Any()) return new CogniteResult<DataPointInsertError>(null);

            _logger.LogDebug("Uploading {Number} data points to CDF for {NumberTs} time series",
                points.Values.Select(dp => dp.Count()).Sum(),
                points.Keys.Count);
            return await DataPointExtensions.InsertAsync(
                _client,
                points,
                _chunking.DataPointTimeSeries,
                _chunking.DataPoints,
                _throttling.DataPoints,
                _chunking.TimeSeries,
                _throttling.TimeSeries,
                _chunking.DataPointsGzipLimit,
                sanitationMode,
                retryMode,
                _nanReplacement,
                token).ConfigureAwait(false);
        }

        /// <summary>
        /// Insert datapoints to timeseries. Insertions are chunked and cleaned according to configuration,
        /// and can optionally handle errors. If any timeseries missing from the result and inserted by externalId,
        /// they are created before the points are inserted again.
        /// </summary>
        /// <param name="points">Datapoints to insert</param>
        /// <param name="sanitationMode">How to sanitize datapoints</param>
        /// <param name="retryMode">How to handle retries</param>
        /// <param name="dataSetId">Optional data set id</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>Results with a list of errors. If TimeSeriesResult is null, no timeseries were attempted created.</returns>
        public async Task<(CogniteResult<DataPointInsertError> DataPointResult, CogniteResult<TimeSeries, TimeSeriesCreate>? TimeSeriesResult)> InsertDataPointsCreateMissingAsync(
            IDictionary<Identity, IEnumerable<Datapoint>>? points,
            SanitationMode sanitationMode,
            RetryMode retryMode,
            long? dataSetId,
            CancellationToken token)
        {
            if (points == null || !points.Any()) return (new CogniteResult<DataPointInsertError>(null), null);

            return await DataPointExtensions.InsertAsyncCreateMissing(
                _client,
                points,
                _chunking.DataPointTimeSeries,
                _chunking.DataPoints,
                _throttling.DataPoints,
                _chunking.TimeSeries,
                _throttling.TimeSeries,
                _chunking.DataPointsGzipLimit,
                sanitationMode,
                retryMode,
                _nanReplacement,
                dataSetId,
                token).ConfigureAwait(false);
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
            IDictionary<Identity, IEnumerable<TimeRange>>? ranges,
            CancellationToken token)
        {
            if (ranges == null || !ranges.Any())
            {
                return new DeleteError(new List<Identity>(), new List<Identity>());
            }
            _logger.LogDebug("Deleting data points in CDF for {NumberTs} time series",
                ranges.Keys.Count);
            var errors = await _client.DataPoints.DeleteIgnoreErrorsAsync(
                ranges,
                _chunking.DataPointDelete,
                _chunking.DataPointList,
                _throttling.DataPoints,
                _throttling.DataPoints,
                token).ConfigureAwait(false);
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
            JsonSerializerOptions? options,
            CancellationToken token)
        {
            if (rows == null || !rows.Any())
            {
                return;
            }
            _logger.LogDebug("Uploading {Number} rows to CDF Raw. Database: {Db}. Table: {Table}",
                rows.Count,
                database,
                table);
            await _client.Raw.InsertRowsAsync(
                database,
                table,
                rows,
                _chunking.RawRows,
                _throttling.Raw,
                token,
                options).ConfigureAwait(false);
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
            IDictionary<string, T>? rows,
            CancellationToken token)
        {
            if (rows == null || !rows.Any())
            {
                return;
            }
            _logger.LogDebug("Uploading {Number} rows to CDF Raw. Database: {Db}. Table: {Table}",
                rows.Count,
                database,
                table);
            await _client.Raw.InsertRowsAsync(
                database,
                table,
                rows,
                _chunking.RawRows,
                _throttling.Raw,
                token).ConfigureAwait(false);
        }

        /// <summary>
        /// Creates a Raw upload queue. It can be used to queue DTOs (data type objects) of type <typeparamref name="T"/>
        /// before sending them to CDF Raw. The items are dequeued and uploaded every <paramref name="interval"/>. If <paramref name="maxQueueSize"/> is
        /// greater than zero, the queue will have a maximum size, and items are also uploaded as soon as the maximum size is reached.
        /// To start the upload loop, use the <see cref="IUploadQueue.Start(CancellationToken)"/> method. To stop it, dispose of the queue or
        /// cancel the token
        /// </summary>
        /// <param name="database">Raw database name</param>
        /// <param name="table">Raw table name</param>
        /// <param name="interval">Upload interval</param>
        /// <param name="maxQueueSize">Maximum queue size</param>
        /// <param name="callback">Callback on upload</param>
        /// <typeparam name="T">Type of the DTO</typeparam>
        /// <returns>An upload queue object</returns>
        public IRawUploadQueue<T> CreateRawUploadQueue<T>(string database, string table,
            TimeSpan interval, int maxQueueSize = 0, Func<QueueUploadResult<(string key, T columns)>, Task>? callback = null)
        {
            return new RawUploadQueue<T>(database, table, this, interval, maxQueueSize, _logger, callback);
        }

        /// <summary>
        /// Creates a datapoint upload queue. It is used to queue datapoints before uploading them to timeseries in CDF.
        /// The items are dequeued and uploaded every <paramref name="interval"/>. If <paramref name="maxQueueSize"/> is greater than zero,
        /// the queue will have a maximum size, and items are also uploaded as soon as the maximum size is reached.
        /// If <paramref name="interval"/> is zero or infinite, the queue will never upload unless prompted or <paramref name="maxQueueSize"/> is reached.
        /// To start the upload loop, use the <see cref="BaseUploadQueue{T}.Start(CancellationToken)"/> method. To stop it, dispose of the queue or
        /// cancel the token
        /// </summary>
        /// <param name="interval">Upload interval</param>
        /// <param name="maxQueueSize">Maximum queue size</param>
        /// <param name="callback">Callback on upload</param>
        /// <param name="bufferPath">Path to local binary buffer file. If this is non-null, points are automatically buffered to
        /// a local file if inserting times out or fails with status >= 500</param>
        /// <returns>An upload queue object</returns>
        public TimeSeriesUploadQueue CreateTimeSeriesUploadQueue(TimeSpan interval, int maxQueueSize = 0,
            Func<QueueUploadResult<(Identity id, Datapoint dp)>, Task>? callback = null, string? bufferPath = null)
        {
            return new TimeSeriesUploadQueue(this, interval, maxQueueSize, _logger, callback, bufferPath);
        }

        /// <summary>
        /// Creates an event upload queue. It is used to queue events before uploading them to CDF.
        /// The items are dequeued and uploaded every <paramref name="interval"/>. If <paramref name="maxQueueSize"/> is greater than zero,
        /// the queue will have a maximum size, and items are also uploaded as soon as the maximum size is reached.
        /// If <paramref name="interval"/> is zero or infinite, the queue will never upload unless prompted or <paramref name="maxQueueSize"/> is reached.
        /// To start the upload loop, use the <see cref="BaseUploadQueue{T}.Start(CancellationToken)"/> method. To stop it, dispose of the queue or
        /// cancel the token
        /// </summary>
        /// <param name="interval">Upload interval</param>
        /// <param name="maxQueueSize">Maximum queue size</param>
        /// <param name="callback">Callback on upload</param>
        /// <param name="bufferPath">Path to local binary buffer file. If this is non-null, points are automatically buffered to
        /// a local file if inserting times out or fails with status >= 500</param>
        /// <returns>An upload queue object</returns>
        public EventUploadQueue CreateEventUploadQueue(TimeSpan interval, int maxQueueSize = 0,
            Func<QueueUploadResult<EventCreate>, Task>? callback = null, string? bufferPath = null)
        {
            return new EventUploadQueue(this, interval, maxQueueSize, _logger, callback, bufferPath);
        }

        /// <summary>
        /// Returns all rows from the given database and table
        /// </summary>
        /// <param name="dbName">Database to read from</param>
        /// <param name="tableName">Table to read from</param>
        /// <param name="options">Optional json serializer options</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>All rows</returns>
        public Task<IDictionary<string, IDictionary<string, JsonElement>>> GetRowsAsync(
            string dbName,
            string tableName,
            JsonSerializerOptions? options,
            CancellationToken token)
        {
            _logger.LogDebug("Fetching all rows from database {db}, table {table}", dbName, tableName);
            return _client.Raw.GetRowsAsync<IDictionary<string, JsonElement>>(dbName, tableName, _chunking.RawRows, token, options);
        }

        /// <summary>
        /// Returns all rows from the given database and table
        /// </summary>
        /// <param name="dbName">Database to read from</param>
        /// <param name="tableName">Table to read from</param>
        /// <param name="options">Optional json serializer options</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>All rows</returns>
        public Task<IDictionary<string, T>> GetRowsAsync<T>(
            string dbName,
            string tableName,
            JsonSerializerOptions? options,
            CancellationToken token)
        {
            _logger.LogDebug("Fetching all rows from database {db}, table {table}", dbName, tableName);
            return _client.Raw.GetRowsAsync<T>(dbName, tableName, _chunking.RawRows, token, options);
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
            return _client.Raw.DeleteRowsAsync(dbName, tableName, rowKeys, _chunking.RawRowsDelete, _throttling.Raw, token);
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
                _chunking.DataPointList,
                _chunking.DataPointLatest,
                _throttling.Ranges,
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
                _chunking.DataPointList,
                _chunking.DataPointLatest,
                _throttling.Ranges,
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
        /// If any items fail to be pushed due to missing assetIds, missing dataset, or duplicated externalId
        /// they can be removed before retrying by setting <paramref name="retryMode"/>
        /// </summary>
        /// <param name="externalIds">External Ids</param>
        /// <param name="buildEvents">Function that builds CogniteSdk EventCreate objects</param>
        /// <param name="retryMode">How to handle failed requests</param>
        /// <param name="sanitationMode">The type of sanitation to apply to events before creating</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>A <see cref="CogniteResult{TResult, TError}"/> containing errors that occured and a list of the created and found events</returns>
        public async Task<CogniteResult<Event, EventCreate>> GetOrCreateEventsAsync(
            IEnumerable<string> externalIds,
            Func<IEnumerable<string>, IEnumerable<EventCreate>> buildEvents,
            RetryMode retryMode,
            SanitationMode sanitationMode,
            CancellationToken token)
        {
            _logger.LogInformation("Getting or creating {Number} events in CDF", externalIds.Count());
            return await _client.Events.GetOrCreateAsync(
                externalIds,
                buildEvents,
                _chunking.Events,
                _throttling.Events,
                retryMode,
                sanitationMode,
                token).ConfigureAwait(false);
        }
        /// <summary>
        /// Ensures the the events with the provided <paramref name="externalIds"/> exist in CDF.
        /// If one or more do not exist, use the <paramref name="buildEvents"/> function to construct
        /// the missing event objects and upload them to CDF.
        /// This method uses the <see cref="CogniteConfig"/> object to determine chunking of items and throttling
        /// against CDF
        /// If any items fail to be pushed due to missing assetIds, missing dataset, or duplicated externalId
        /// they can be removed before retrying by setting <paramref name="retryMode"/>
        /// </summary>
        /// <param name="externalIds">External Ids</param>
        /// <param name="buildEvents">Async function that builds CogniteSdk EventCreate objects</param>
        /// <param name="retryMode">How to handle failed requests</param>
        /// <param name="sanitationMode">The type of sanitation to apply to events before creating</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>A <see cref="CogniteResult{TResult, TError}"/> containing errors that occured and a list of the created and found events</returns>
        public async Task<CogniteResult<Event, EventCreate>> GetOrCreateEventsAsync(
            IEnumerable<string> externalIds,
            Func<IEnumerable<string>, Task<IEnumerable<EventCreate>>> buildEvents,
            RetryMode retryMode,
            SanitationMode sanitationMode,
            CancellationToken token)
        {
            _logger.LogInformation("Getting or creating {Number} events in CDF", externalIds.Count());
            return await _client.Events.GetOrCreateAsync(
                externalIds,
                buildEvents,
                _chunking.Events,
                _throttling.Events,
                retryMode,
                sanitationMode,
                token).ConfigureAwait(false);
        }

        /// <summary>
        /// Ensures that all events in <paramref name="events"/> exist in CDF.
        /// Tries to create the events and returns when all are created or removed
        /// due to issues with the request.
        /// If any items fail to be pushed due to missing assetIds, missing dataset, or duplicated externalId
        /// they can be removed before retrying by setting <paramref name="retryMode"/>
        /// Events will be returned in the same order as given in <paramref name="events"/>.
        /// </summary>
        /// <param name="events">List of CogniteSdk EventCreate objects</param>
        /// <param name="retryMode">How to do retries. Keeping duplicates is not valid for
        /// this method.</param>
        /// <param name="sanitationMode">The type of sanitation to apply to events before creating</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>A <see cref="CogniteResult{TResult, TError}"/> containing errors that occured and a list of the created events</returns>
        public async Task<CogniteResult<Event, EventCreate>> EnsureEventsExistsAsync(
            IEnumerable<EventCreate> events,
            RetryMode retryMode,
            SanitationMode sanitationMode,
            CancellationToken token)
        {
            _logger.LogInformation("Ensuring that {Number} events exist in CDF", events.Count());
            return await _client.Events.EnsureExistsAsync(
                events,
                _chunking.Events,
                _throttling.Events,
                retryMode,
                sanitationMode,
                token).ConfigureAwait(false);
        }
        #endregion

        #region sequences
        /// <summary>
        /// Ensures the the sequences with the provided <paramref name="externalIds"/> exist in CDF.
        /// If one or more do not exist, use the <paramref name="buildSequences"/> function to construct
        /// the missing sequence objects and upload them to CDF.
        /// This method uses the <see cref="CogniteConfig"/> object to determine chunking of items and throttling
        /// against CDF
        /// If any items fail to be pushed due to missing assetId, missing dataset, or duplicated externalId
        /// they can be removed before retrying by setting <paramref name="retryMode"/>
        /// </summary>
        /// <param name="externalIds">External Ids</param>
        /// <param name="buildSequences">Function that builds CogniteSdk SequenceCreate objects</param>
        /// <param name="retryMode">How to handle failed requests</param>
        /// <param name="sanitationMode">The type of sanitation to apply to sequences before creating</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>A <see cref="CogniteResult{TResult, TError}"/> containing errors that occured and a list of the created and found sequences</returns>
        public async Task<CogniteResult<Sequence, SequenceCreate>> GetOrCreateSequencesAsync(
            IEnumerable<string> externalIds,
            Func<IEnumerable<string>, IEnumerable<SequenceCreate>> buildSequences,
            RetryMode retryMode,
            SanitationMode sanitationMode,
            CancellationToken token)
        {
            _logger.LogInformation("Getting or creating {Number} sequences in CDF", externalIds.Count());
            return await _client.Sequences.GetOrCreateAsync(
                externalIds,
                buildSequences,
                _chunking.Sequences,
                _throttling.Sequences,
                retryMode,
                sanitationMode,
                token).ConfigureAwait(false);
        }
        /// <summary>
        /// Ensures the the sequences with the provided <paramref name="externalIds"/> exist in CDF.
        /// If one or more do not exist, use the <paramref name="buildSequences"/> function to construct
        /// the missing sequence objects and upload them to CDF.
        /// This method uses the <see cref="CogniteConfig"/> object to determine chunking of items and throttling
        /// against CDF
        /// If any items fail to be pushed due to missing assetId, missing dataset, or duplicated externalId
        /// they can be removed before retrying by setting <paramref name="retryMode"/>
        /// </summary>
        /// <param name="externalIds">External Ids</param>
        /// <param name="buildSequences">Async function that builds CogniteSdk SequenceCreate objects</param>
        /// <param name="retryMode">How to handle failed requests</param>
        /// <param name="sanitationMode">The type of sanitation to apply to sequences before creating</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>A <see cref="CogniteResult{TResult, TError}"/> containing errors that occured and a list of the created and found sequences</returns>
        public async Task<CogniteResult<Sequence, SequenceCreate>> GetOrCreateSequencesAsync(
            IEnumerable<string> externalIds,
            Func<IEnumerable<string>, Task<IEnumerable<SequenceCreate>>> buildSequences,
            RetryMode retryMode,
            SanitationMode sanitationMode,
            CancellationToken token)
        {
            _logger.LogInformation("Getting or creating {Number} sequences in CDF", externalIds.Count());
            return await _client.Sequences.GetOrCreateAsync(
                externalIds,
                buildSequences,
                _chunking.Sequences,
                _throttling.Sequences,
                retryMode,
                sanitationMode,
                token).ConfigureAwait(false);
        }

        /// <summary>
        /// Ensures that all sequences in <paramref name="sequences"/> exist in CDF.
        /// Tries to create the sequences and returns when all are created or removed
        /// due to issues with the request.
        /// If any items fail to be pushed due to missing assetId, missing dataset, or duplicated externalId
        /// they can be removed before retrying by setting <paramref name="retryMode"/>
        /// Sequences will be returned in the same order as given in <paramref name="sequences"/>.
        /// </summary>
        /// <param name="sequences">List of CogniteSdk SequenceCreate objects</param>
        /// <param name="retryMode">How to do retries. Keeping duplicates is not valid for
        /// this method.</param>
        /// <param name="sanitationMode">The type of sanitation to apply to sequences before creating</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>A <see cref="CogniteResult{TResult, TError}"/> containing errors that occured and a list of the created sequences</returns>
        public async Task<CogniteResult<Sequence, SequenceCreate>> EnsureSequencesExistsAsync(
            IEnumerable<SequenceCreate> sequences,
            RetryMode retryMode,
            SanitationMode sanitationMode,
            CancellationToken token)
        {
            _logger.LogInformation("Ensuring that {Number} events exist in CDF", sequences.Count());
            return await _client.Sequences.EnsureExistsAsync(
                sequences,
                _chunking.Sequences,
                _throttling.Sequences,
                retryMode,
                sanitationMode,
                token).ConfigureAwait(false);
        }

        /// <summary>
        /// Insert the given list of rows into CDF.
        /// Both individual rows and full sequences can be removed due to mismatched datatypes,
        /// duplicated externalIds, or similar, by setting <paramref name="retryMode"/>
        /// and <paramref name="sanitationMode"/>.
        /// </summary>
        /// <param name="sequences">Sequences with rows to insert</param>
        /// <param name="retryMode">How to handle retries. Keeping duplicates is not valid for this method.</param>
        /// <param name="sanitationMode">The type of sanitation to apply to sequences before creating.
        /// Errors that are normally handled by sanitation will not be handled if received from CDF.</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>A <see cref="CogniteResult{TError}"/> containing errors that occured during insertion</returns>
        public async Task<CogniteResult<SequenceRowError>> InsertSequenceRowsAsync(
            IEnumerable<SequenceDataCreate> sequences,
            RetryMode retryMode,
            SanitationMode sanitationMode,
            CancellationToken token)
        {
            _logger.LogDebug("Inserting {Rows} rows for {Seq} sequences into CDF",
                sequences.Sum(seq => seq.Rows?.Count() ?? 0), sequences.Count());
            return await _client.Sequences.InsertAsync(
                sequences,
                _chunking.SequenceRowSequences,
                _chunking.SequenceRows,
                _chunking.Sequences,
                _throttling.Sequences,
                retryMode,
                sanitationMode,
                token).ConfigureAwait(false);
        }
        #endregion

        #region stream_records
        /// <summary>
        /// Retrieve a stream, or create it if it does not exist.
        /// </summary>
        /// <param name="stream">Stream to create or retrieve</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>Created or retrieved stream</returns>
        /// <exception cref="ArgumentNullException"></exception>
        public async Task<CogniteSdk.Beta.Stream> GetOrCreateStreamAsync(
            StreamWrite stream,
            CancellationToken token)
        {
            if (stream == null) throw new ArgumentNullException(nameof(stream));
            _logger.LogDebug("Getting or creating stream with ID {Stream}", stream.ExternalId);
            return await _client.Beta.StreamRecords.GetOrCreateStreamAsync(stream, token).ConfigureAwait(false);
        }

        /// <summary>
        /// Insert the given stream records into <paramref name="stream"/>. The stream
        /// must exist.
        /// </summary>
        /// <param name="stream">Stream to ingest into</param>
        /// <param name="records">Stream records to insert</param>
        /// <param name="token">Cancellation token</param>
        /// <exception cref="ArgumentNullException"></exception>
        public async Task InsertRecordsAsync(
            string stream,
            ICollection<StreamRecordWrite> records,
            CancellationToken token)
        {
            if (stream == null) throw new ArgumentNullException(nameof(stream));
            if (records == null) throw new ArgumentNullException(nameof(records));
            _logger.LogDebug("Inserting {Count} records into the stream {Stream}",
                records.Count, stream);
            await _client.Beta.StreamRecords.InsertRecordsAsync(
                stream,
                records,
                _chunking.StreamRecords,
                _throttling.StreamRecords,
                token
            ).ConfigureAwait(false);
        }
        #endregion
    }
}