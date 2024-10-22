using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extensions;
using Cognite.Extensions.DataModels.CogniteExtractorExtensions;
using CogniteSdk;
using CogniteSdk.DataModels;
using CogniteSdk.DataModels.Core;
using Microsoft.Extensions.Logging;

namespace Cognite.Extractor.Utils
{
    /// <summary>
    /// Class with utility methods supporting extraction of data into CDF.
    /// These methods complement the ones offered by the <see cref="Client"/> and use a
    /// <see cref="CogniteConfig"/> object to determine chunking of data and throttling of
    /// requests against the client
    /// </summary>
    public class CogniteDestinationWithIDM : CogniteDestination
    {
        private readonly Client _client;
        private readonly ILogger<CogniteDestination> _logger;
        private readonly CogniteConfig _config;

        /// <summary>
        /// View identifier for IDM TimeSeries
        /// </summary>
        public static ViewIdentifier IDMViewIdentifier { get; protected set; } = new ViewIdentifier("cdf_extraction_extensions", "CogniteExtractorTimeSeries", "v1");

        /// <summary>
        /// Initializes the Cognite destination with the provided parameters
        /// </summary>
        /// <param name="client"><see cref="Client"/> object</param>
        /// <param name="logger">Logger</param>
        /// <param name="config">Configuration object</param>
        /// <param name="viewIdentifier">Optional view identifier</param>
        public CogniteDestinationWithIDM(Client client, ILogger<CogniteDestination> logger, CogniteConfig config, ViewIdentifier? viewIdentifier = null) : base(client, logger, config)
        {
            _client = client;
            _logger = logger;
            _config = config;
            if (viewIdentifier != null)
            {
                IDMViewIdentifier = viewIdentifier;
            }
        }

        #region timeseries
        /// <summary>
        /// Ensures the the time series with the provided <paramref name="instanceIds"/> exist in CDF.
        /// If one or more do not exist, use the <paramref name="buildTimeSeries"/> function to construct
        /// the missing time series objects and upload them to CDF.
        /// This method uses the <see cref="CogniteConfig"/> object to determine chunking of items and throttling
        /// against CDF
        /// If any items fail to be created due to missing asset, duplicated externalId, duplicated
        /// legacy name, or missing dataSetId, they can be removed before retrying by setting
        /// <paramref name="retryMode"/>
        /// </summary>
        /// <param name="instanceIds">Instance Ids</param>
        /// <param name="buildTimeSeries">Function that builds CogniteSdk TimeSeries objects</param>
        /// <param name="retryMode">How to handle failed requests</param>
        /// <param name="sanitationMode">The type of sanitation to apply to TimeSeries before creating</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>A <see cref="CogniteResult{TResult, TError}"/> containing errors that occurred and a list of the created and found TimeSeries</returns>
        public async Task<CogniteResult<SourcedNode<T>, SourcedNodeWrite<T>>> GetOrCreateTimeSeriesAsync<T>(
            IEnumerable<InstanceIdentifier> instanceIds,
            Func<IEnumerable<InstanceIdentifier>, IEnumerable<SourcedNodeWrite<T>>> buildTimeSeries,
            RetryMode retryMode,
            SanitationMode sanitationMode,
            CancellationToken token) where T : CogniteExtractorTimeSeries
        {
            _logger.LogInformation("Getting or creating {Number} time series in CDF", instanceIds.Count());
            return await _client.CoreDataModel.TimeSeries<T>(IDMViewIdentifier)
                .GetOrCreateTimeSeriesAsync(
                    instanceIds,
                    buildTimeSeries,
                    _config.CdfChunking.Instances,
                    _config.CdfThrottling.Instances,
                    retryMode,
                    sanitationMode,
                    token
                ).ConfigureAwait(false);
        }
        /// <summary>
        /// Ensures the the time series with the provided <paramref name="instanceIds"/> exist in CDF.
        /// If one or more do not exist, use the <paramref name="buildTimeSeries"/> function to construct
        /// the missing time series objects and upload them to CDF.
        /// This method uses the <see cref="CogniteConfig"/> object to determine chunking of items and throttling
        /// against CDF
        /// By default, if any items fail to be created due to missing asset, duplicated externalId, duplicated
        /// legacy name, or missing dataSetId, they can be removed before retrying by setting
        /// <paramref name="retryMode"/>
        /// </summary>
        /// <param name="instanceIds">Instance Ids</param>
        /// <param name="buildTimeSeries">Async function that builds CogniteSdk TimeSeries objects</param>
        /// <param name="retryMode">How to handle failed requests</param>
        /// <param name="sanitationMode">The type of sanitation to apply to TimeSeries before creating</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>A <see cref="CogniteResult{TResult, TError}"/> containing errors that occured and a list of the created and found TimeSeries</returns>
        public async Task<CogniteResult<SourcedNode<T>, SourcedNodeWrite<T>>> GetOrCreateTimeSeriesAsync<T>(
            IEnumerable<InstanceIdentifier> instanceIds,
            Func<IEnumerable<InstanceIdentifier>, Task<IEnumerable<SourcedNodeWrite<T>>>> buildTimeSeries,
            RetryMode retryMode,
            SanitationMode sanitationMode,
            CancellationToken token) where T : CogniteExtractorTimeSeries
        {
            _logger.LogInformation("Getting or creating {Number} time series in CDF", instanceIds.Count());
            return await _client.CoreDataModel.TimeSeries<T>(IDMViewIdentifier).GetOrCreateTimeSeriesAsync(
                instanceIds,
                buildTimeSeries,
                _config.CdfChunking.Instances,
                _config.CdfThrottling.Instances,
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
        /// <param name="timeSeries">List of CogniteSdk TimeSeries objects</param>
        /// <param name="retryMode">How to do retries. Keeping duplicates is not valid for
        /// this method.</param>
        /// <param name="sanitationMode">The type of sanitation to apply to timeseries before creating</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>A <see cref="CogniteResult{TResult, TError}"/> containing errors that occured and a list of the created timeseries</returns>
        public async Task<CogniteResult<SourcedNode<T>, SourcedNodeWrite<T>>> EnsureTimeSeriesExistsAsync<T>(
            IEnumerable<SourcedNodeWrite<T>> timeSeries,
            RetryMode retryMode,
            SanitationMode sanitationMode,
            CancellationToken token) where T : CogniteExtractorTimeSeries
        {
            _logger.LogInformation("Ensuring that {Number} time series exist in CDF", timeSeries.Count());
            return await _client.CoreDataModel.TimeSeries<T>(IDMViewIdentifier).EnsureTimeSeriesExistsAsync<T>(
                timeSeries,
                _config.CdfChunking.Instances,
                _config.CdfThrottling.Instances,
                retryMode,
                sanitationMode,
                token).ConfigureAwait(false);
        }

        /// <summary>
        /// Gets TimeSeries by ids in <paramref name="timeSeries"/>, ignoring errors.
        /// </summary>
        /// <param name="timeSeries">List of TimeSeries instance ids to fetch</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>A <see cref="CogniteResult{TResult, TError}"/> containing errors that occured and a list of the created timeseries</returns>
        public async Task<IEnumerable<SourcedNode<T>>> GetTimeSeriesByIdsIgnoreErrors<T>(
            IEnumerable<Identity> timeSeries,
            CancellationToken token) where T : CogniteExtractorTimeSeries
        {
            _logger.LogInformation("Ensuring that {Number} time series exist in CDF", timeSeries.Count());
            return await _client.CoreDataModel.TimeSeries<T>(IDMViewIdentifier).GetTimeSeriesByIdsIgnoreErrors<T>(
                timeSeries,
                _config.CdfChunking.Instances,
                _config.CdfThrottling.Instances,
                token).ConfigureAwait(false);
        }

        /// <summary>
        /// Upsert timeseries in <paramref name="updates"/>.
        /// If items fail due to duplicated instance ids, they can be removed before retrying
        /// by setting <paramref name="retryMode"/>.
        /// TimeSeries will be returned in the same order as given.
        /// </summary>
        /// <param name="updates">List of TimeSeries objects</param>
        /// <param name="retryMode">How to do retries. Keeping duplicates is not valid for this method.</param>
        /// <param name="sanitationMode">The type of sanitation to apply to assets before updating</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>A <see cref="CogniteResult{TResult, TError}"/> containing errors that occured and a list of the updated TimeSeries</returns>
        public async Task<CogniteResult<SlimInstance, SourcedNodeWrite<T>>> UpsertTimeSeriesAsync<T>(
            IEnumerable<SourcedNodeWrite<T>> updates,
            RetryMode retryMode,
            SanitationMode sanitationMode,
            CancellationToken token) where T : CogniteExtractorTimeSeries
        {
            _logger.LogInformation("Updating {Number} timeseries in CDF", updates.Count());
            return await _client.CoreDataModel.TimeSeries<T>(IDMViewIdentifier).UpsertAsync(
                updates,
                _config.CdfChunking.Instances,
                _config.CdfThrottling.Instances,
                retryMode,
                sanitationMode,
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
        public async Task<CogniteResult<DataPointInsertError>> InsertDataPointsIDMAsync(
            IDictionary<Identity, IEnumerable<Datapoint>>? points,
            SanitationMode sanitationMode,
            RetryMode retryMode,
            CancellationToken token)
        {
            if (points == null || !points.Any()) return new CogniteResult<DataPointInsertError>(null);

            _logger.LogDebug("Uploading {Number} data points to CDF for {NumberTs} time series",
                points.Values.Select(dp => dp.Count()).Sum(),
                points.Keys.Count);
            return await DataPointExtensionsWithInstanceId.InsertAsync(
                _client,
                points,
                _config.CdfChunking.DataPointTimeSeries,
                _config.CdfChunking.DataPoints,
                _config.CdfThrottling.DataPoints,
                _config.CdfChunking.TimeSeries,
                _config.CdfThrottling.TimeSeries,
                _config.CdfChunking.DataPointsGzipLimit,
                sanitationMode,
                retryMode,
                _config.NanReplacement,
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
        /// <param name="token">Cancellation token</param>
        /// <returns>Results with a list of errors. If TimeSeriesResult is null, no timeseries were attempted created.</returns>
        public async Task<(CogniteResult<DataPointInsertError> DataPointResult, CogniteResult<SourcedNode<CogniteTimeSeriesBase>, SourcedNodeWrite<CogniteTimeSeriesBase>>? TimeSeriesResult)> InsertDataPointsCreateMissingAsync(
            IDictionary<Identity, IEnumerable<Datapoint>>? points,
            SanitationMode sanitationMode,
            RetryMode retryMode,
            CancellationToken token)
        {
            if (points == null || !points.Any()) return (new CogniteResult<DataPointInsertError>(null), null);

            return await DataPointExtensionsWithInstanceId.InsertAsyncCreateMissing(
                _client,
                points,
                _config.CdfChunking.DataPointTimeSeries,
                _config.CdfChunking.DataPoints,
                _config.CdfThrottling.DataPoints,
                _config.CdfChunking.Instances,
                _config.CdfThrottling.Instances,
                _config.CdfChunking.DataPointsGzipLimit,
                sanitationMode,
                retryMode,
                _config.NanReplacement,
                token).ConfigureAwait(false);
        }
        #endregion
    }
}