using Cognite.Extensions;
using Cognite.Extensions.DataModels.CogniteExtractorExtensions;
using Cognite.Extractor.Utils.Unstable.Configuration;
using CogniteSdk;
using CogniteSdk.DataModels;
using CogniteSdk.DataModels.Core;
using CogniteSdk.Resources;
using CogniteSdk.Resources.DataModels;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

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
            if (viewIdentifier != null)
            {
                IDMViewIdentifier = viewIdentifier;
            }
        }

        /// <summary>
        /// Initializes the Cognite destination with the provided parameters
        /// </summary>
        /// <param name="client"><see cref="Client"/> object</param>
        /// <param name="logger">Logger</param>
        /// <param name="config">Configuration object</param>
        /// <param name="project">Configured project</param>
        /// <param name="viewIdentifier">Optional view identifier</param>
        public CogniteDestinationWithIDM(Client client, ILogger<CogniteDestination> logger, BaseCogniteConfig config, string project, ViewIdentifier? viewIdentifier = null) : base(client, logger, config, project)
        {
            if (config == null) throw new ArgumentNullException(nameof(config));
            _client = client;
            _logger = logger;
            if (viewIdentifier != null)
            {
                IDMViewIdentifier = viewIdentifier;
            }
        }

        private Task<CogniteResult<SourcedNode<T>, SourcedNodeWrite<T>>> GetOrCreateDispatch<T>(
            bool isBeta,
            IEnumerable<InstanceIdentifier> instanceIds,
            Func<IEnumerable<InstanceIdentifier>, Task<IEnumerable<SourcedNodeWrite<T>>>> buildTimeSeries,
            RetryMode retryMode,
            SanitationMode sanitationMode,
            CancellationToken token) where T : CogniteTimeSeriesBase
        {
            if (instanceIds == null) return Task.FromResult(new CogniteResult<SourcedNode<T>, SourcedNodeWrite<T>>(null, null));
            if (buildTimeSeries == null) throw new ArgumentNullException(nameof(buildTimeSeries));
            _logger.LogInformation("Getting or creating {Number} time series in CDF", instanceIds.Count());
            if (isBeta)
            {
                var betaParams = new BetaResourceParams(Chunking.Instances, Throttling.Instances, retryMode, sanitationMode);
                return BetaTimeSeriesExtensions.GetOrCreateTimeSeriesAsync<T>(
                    _client.Beta.TimeSeries, instanceIds, buildTimeSeries, betaParams, token);
            }
            return _client.CoreDataModel.TimeSeries<T>(IDMViewIdentifier, new List<ViewIdentifier> { CoreTimeSeriesResource<T>.DefaultView })
                .GetOrCreateTimeSeriesAsync(instanceIds, buildTimeSeries, Chunking.Instances, Throttling.Instances, retryMode, sanitationMode, token);
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
        /// <param name="isBeta">If true, use the beta CDM time series API instead of the IDM view.
        /// Required to create state time series.</param>
        /// <returns>A <see cref="CogniteResult{TResult, TError}"/> containing errors that occurred and a list of the created and found TimeSeries</returns>
        public async Task<CogniteResult<SourcedNode<T>, SourcedNodeWrite<T>>> GetOrCreateTimeSeriesAsync<T>(
            IEnumerable<InstanceIdentifier> instanceIds,
            Func<IEnumerable<InstanceIdentifier>, IEnumerable<SourcedNodeWrite<T>>> buildTimeSeries,
            RetryMode retryMode,
            SanitationMode sanitationMode,
            CancellationToken token,
            bool isBeta = false) where T : CogniteTimeSeriesBase
        {
            return await GetOrCreateDispatch(isBeta, instanceIds,
                ids => Task.FromResult(buildTimeSeries(ids)), retryMode, sanitationMode, token).ConfigureAwait(false);
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
        /// <param name="isBeta">If true, use the beta CDM time series API instead of the IDM view.
        /// Required to create state time series.</param>
        /// <returns>A <see cref="CogniteResult{TResult, TError}"/> containing errors that occured and a list of the created and found TimeSeries</returns>
        public async Task<CogniteResult<SourcedNode<T>, SourcedNodeWrite<T>>> GetOrCreateTimeSeriesAsync<T>(
            IEnumerable<InstanceIdentifier> instanceIds,
            Func<IEnumerable<InstanceIdentifier>, Task<IEnumerable<SourcedNodeWrite<T>>>> buildTimeSeries,
            RetryMode retryMode,
            SanitationMode sanitationMode,
            CancellationToken token,
            bool isBeta = false) where T : CogniteTimeSeriesBase
        {
            return await
                GetOrCreateDispatch(isBeta, instanceIds, buildTimeSeries, retryMode, sanitationMode, token)
                .ConfigureAwait(false);
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
        /// <param name="isBeta">If true, use the beta CDM time series API instead of the IDM view.
        /// Required to create state time series.</param>
        /// <returns>A <see cref="CogniteResult{TResult, TError}"/> containing errors that occured and a list of the created timeseries</returns>
        public async Task<CogniteResult<SourcedNode<T>, SourcedNodeWrite<T>>> EnsureTimeSeriesExistsAsync<T>(
            IEnumerable<SourcedNodeWrite<T>> timeSeries,
            RetryMode retryMode,
            SanitationMode sanitationMode,
            CancellationToken token,
            bool isBeta = false) where T : CogniteTimeSeriesBase
        {
            if (timeSeries == null) return new CogniteResult<SourcedNode<T>, SourcedNodeWrite<T>>(null, null);
            _logger.LogInformation("Ensuring that {Number} time series exist in CDF", timeSeries.Count());
            if (isBeta)
            {
                var betaParams = new BetaResourceParams(Chunking.Instances, Throttling.Instances, retryMode, sanitationMode);
                return await
                    BetaTimeSeriesExtensions
                        .EnsureTimeSeriesExistsAsync<T>(_client.Beta.TimeSeries, timeSeries, betaParams, token)
                        .ConfigureAwait(false);
            }
            return await
                _client.CoreDataModel
                    .TimeSeries<T>(IDMViewIdentifier)
                    .EnsureTimeSeriesExistsAsync<T>(timeSeries, Chunking.Instances, Throttling.Instances, retryMode, sanitationMode, token)
                    .ConfigureAwait(false);
        }

        /// <summary>
        /// Gets TimeSeries by ids in <paramref name="timeSeries"/>, ignoring errors.
        /// </summary>
        /// <param name="timeSeries">List of TimeSeries instance ids to fetch</param>
        /// <param name="token">Cancellation token</param>
        /// <param name="isBeta">If true, use the beta CDM time series API instead of the IDM view.
        /// Required to fetch state time series.</param>
        /// <returns>A <see cref="CogniteResult{TResult, TError}"/> containing errors that occured and a list of the created timeseries</returns>
        public async Task<IEnumerable<SourcedNode<T>>> GetTimeSeriesByIdsIgnoreErrors<T>(
            IEnumerable<Identity> timeSeries,
            CancellationToken token,
            bool isBeta = false) where T : CogniteTimeSeriesBase
        {
            if (timeSeries == null) return new List<SourcedNode<T>>();
            _logger.LogInformation("Ensuring that {Number} time series exist in CDF", timeSeries.Count());
            if (isBeta)
            {
                return await
                    BetaTimeSeriesExtensions
                        .GetTimeSeriesByIdsIgnoreErrors<T>(_client.Beta.TimeSeries, timeSeries, Chunking.Instances, Throttling.Instances, token)
                        .ConfigureAwait(false);
            }
            return await
                _client.CoreDataModel.TimeSeries<T>(IDMViewIdentifier)
                    .GetTimeSeriesByIdsIgnoreErrors<T>(timeSeries, Chunking.Instances, Throttling.Instances, token)
                    .ConfigureAwait(false);
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
        /// <param name="isBeta">If true, use the beta CDM time series API instead of the IDM view.
        /// Required to update state time series.</param>
        /// <returns>A <see cref="CogniteResult{TResult, TError}"/> containing errors that occured and a list of the updated TimeSeries</returns>
        public async Task<CogniteResult<SlimInstance, SourcedNodeWrite<T>>> UpsertTimeSeriesAsync<T>(
            IEnumerable<SourcedNodeWrite<T>> updates,
            RetryMode retryMode,
            SanitationMode sanitationMode,
            CancellationToken token,
            bool isBeta = false) where T : CogniteTimeSeriesBase
        {
            if (updates == null) return new CogniteResult<SlimInstance, SourcedNodeWrite<T>>(null, null);
            _logger.LogInformation("Updating {Number} timeseries in CDF", updates.Count());
            if (isBeta)
            {
                var betaParams = new BetaResourceParams(Chunking.Instances, Throttling.Instances, retryMode, sanitationMode);
                return await
                    BetaTimeSeriesExtensions.UpsertAsync(_client.Beta.TimeSeries, updates, betaParams, token).ConfigureAwait(false);
            }
            return await
                _client.CoreDataModel
                    .TimeSeries<T>(IDMViewIdentifier)
                    .UpsertAsync(updates, Chunking.Instances, Throttling.Instances, retryMode, sanitationMode, token)
                    .ConfigureAwait(false);
        }
        #endregion

        #region state sets
        /// <summary>
        /// Ensures the the state sets with the provided <paramref name="instanceIds"/> exist in CDF.
        /// If one or more do not exist, use the <paramref name="buildStateSets"/> function to construct
        /// the missing state set objects and upload them to CDF.
        /// State sets are only available through the beta CDM state set API.
        /// </summary>
        /// <param name="instanceIds">Instance Ids</param>
        /// <param name="buildStateSets">Function that builds CogniteSdk StateSet objects</param>
        /// <param name="retryMode">How to handle failed requests</param>
        /// <param name="sanitationMode">The type of sanitation to apply to state sets before creating</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>A <see cref="CogniteResult{TResult, TError}"/> containing errors that occurred and a list of the created and found state sets</returns>
        public async Task<CogniteResult<SourcedNode<T>, SourcedNodeWrite<T>>> GetOrCreateStateSetsAsync<T>(
            IEnumerable<InstanceIdentifier> instanceIds,
            Func<IEnumerable<InstanceIdentifier>, IEnumerable<SourcedNodeWrite<T>>> buildStateSets,
            RetryMode retryMode,
            SanitationMode sanitationMode,
            CancellationToken token) where T : CogniteStateSet
        {
            if (instanceIds == null) return new CogniteResult<SourcedNode<T>, SourcedNodeWrite<T>>(null, null);
            if (buildStateSets == null) throw new ArgumentNullException(nameof(buildStateSets));
            _logger.LogInformation("Getting or creating {Number} state sets in CDF", instanceIds.Count());
            return await _client.Beta.StateSets.GetOrCreateStateSetsAsync(
                instanceIds,
                buildStateSets,
                new BetaResourceParams(Chunking.Instances, Throttling.Instances, retryMode, sanitationMode),
                token).ConfigureAwait(false);
        }

        /// <summary>
        /// Ensures the the state sets with the provided <paramref name="instanceIds"/> exist in CDF.
        /// If one or more do not exist, use the <paramref name="buildStateSets"/> function to construct
        /// the missing state set objects and upload them to CDF.
        /// State sets are only available through the beta CDM state set API.
        /// </summary>
        /// <param name="instanceIds">Instance Ids</param>
        /// <param name="buildStateSets">Async function that builds CogniteSdk StateSet objects</param>
        /// <param name="retryMode">How to handle failed requests</param>
        /// <param name="sanitationMode">The type of sanitation to apply to state sets before creating</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>A <see cref="CogniteResult{TResult, TError}"/> containing errors that occured and a list of the created and found state sets</returns>
        public async Task<CogniteResult<SourcedNode<T>, SourcedNodeWrite<T>>> GetOrCreateStateSetsAsync<T>(
            IEnumerable<InstanceIdentifier> instanceIds,
            Func<IEnumerable<InstanceIdentifier>, Task<IEnumerable<SourcedNodeWrite<T>>>> buildStateSets,
            RetryMode retryMode,
            SanitationMode sanitationMode,
            CancellationToken token) where T : CogniteStateSet
        {
            if (instanceIds == null) return new CogniteResult<SourcedNode<T>, SourcedNodeWrite<T>>(null, null);
            if (buildStateSets == null) throw new ArgumentNullException(nameof(buildStateSets));
            _logger.LogInformation("Getting or creating {Number} state sets in CDF", instanceIds.Count());
            return await _client.Beta.StateSets.GetOrCreateStateSetsAsync(
                instanceIds,
                buildStateSets,
                new BetaResourceParams(Chunking.Instances, Throttling.Instances, retryMode, sanitationMode),
                token).ConfigureAwait(false);
        }

        /// <summary>
        /// Ensures that all state sets in <paramref name="stateSets"/> exist in CDF.
        /// Tries to create the state sets and returns when all are created or have been removed
        /// due to issues with the request. State sets are only available through the beta CDM state set API.
        /// </summary>
        /// <param name="stateSets">List of CogniteSdk StateSet objects</param>
        /// <param name="retryMode">How to do retries. Keeping duplicates is not valid for
        /// this method.</param>
        /// <param name="sanitationMode">The type of sanitation to apply to state sets before creating</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>A <see cref="CogniteResult{TResult, TError}"/> containing errors that occured and a list of the created state sets</returns>
        public async Task<CogniteResult<SourcedNode<T>, SourcedNodeWrite<T>>> EnsureStateSetsExistAsync<T>(
            IEnumerable<SourcedNodeWrite<T>> stateSets,
            RetryMode retryMode,
            SanitationMode sanitationMode,
            CancellationToken token) where T : CogniteStateSet
        {
            if (stateSets == null) throw new ArgumentNullException(nameof(stateSets));
            _logger.LogInformation("Ensuring that {Number} state sets exist in CDF", stateSets.Count());
            return await _client.Beta.StateSets.EnsureStateSetsExistAsync(
                stateSets,
                new BetaResourceParams(Chunking.Instances, Throttling.Instances, retryMode, sanitationMode),
                token).ConfigureAwait(false);
        }

        /// <summary>
        /// Gets state sets by ids in <paramref name="stateSets"/>, ignoring errors.
        /// State sets are only available through the beta CDM state set API.
        /// </summary>
        /// <param name="stateSets">List of StateSet instance ids to fetch</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>A <see cref="CogniteResult{TResult, TError}"/> containing errors that occured and a list of the created state sets</returns>
        public async Task<IEnumerable<SourcedNode<T>>> GetStateSetsByIdsIgnoreErrors<T>(
            IEnumerable<Identity> stateSets,
            CancellationToken token) where T : CogniteStateSet
        {
            if (stateSets == null) return new List<SourcedNode<T>>();
            _logger.LogInformation("Getting {Number} state sets from CDF", stateSets.Count());
            return await _client.Beta.StateSets.GetStateSetsByIdsIgnoreErrors<T>(
                stateSets,
                Chunking.Instances,
                Throttling.Instances,
                token).ConfigureAwait(false);
        }

        /// <summary>
        /// Upsert state sets in <paramref name="updates"/>.
        /// If items fail due to duplicated instance ids, they can be removed before retrying
        /// by setting <paramref name="retryMode"/>. State sets are only available through the beta CDM state set API.
        /// </summary>
        /// <param name="updates">List of StateSet objects</param>
        /// <param name="retryMode">How to do retries. Keeping duplicates is not valid for this method.</param>
        /// <param name="sanitationMode">The type of sanitation to apply to state sets before updating</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>A <see cref="CogniteResult{TResult, TError}"/> containing errors that occured and a list of the updated state sets</returns>
        public async Task<CogniteResult<SlimInstance, SourcedNodeWrite<T>>> UpsertStateSetsAsync<T>(
            IEnumerable<SourcedNodeWrite<T>> updates,
            RetryMode retryMode,
            SanitationMode sanitationMode,
            CancellationToken token) where T : CogniteStateSet
        {
            if (updates == null) return new CogniteResult<SlimInstance, SourcedNodeWrite<T>>(null, null);
            _logger.LogInformation("Updating {Number} state sets in CDF", updates.Count());
            return await _client.Beta.StateSets.UpsertAsync(
                updates,
                new BetaResourceParams(Chunking.Instances, Throttling.Instances, retryMode, sanitationMode),
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
                points.Values.Sum(dp => dp.Count()),
                points.Keys.Count);
            return await DataPointExtensionsWithInstanceId.InsertAsync(
                _client,
                points,
                Chunking.DataPointTimeSeries,
                Chunking.DataPoints,
                Throttling.DataPoints,
                Chunking.TimeSeries,
                Throttling.TimeSeries,
                Chunking.DataPointsGzipLimit,
                sanitationMode,
                retryMode,
                NanReplacement,
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
                Chunking.DataPointTimeSeries,
                Chunking.DataPoints,
                Throttling.DataPoints,
                Chunking.Instances,
                Throttling.Instances,
                Chunking.DataPointsGzipLimit,
                sanitationMode,
                retryMode,
                NanReplacement,
                token).ConfigureAwait(false);
        }
        #endregion
    }
}
