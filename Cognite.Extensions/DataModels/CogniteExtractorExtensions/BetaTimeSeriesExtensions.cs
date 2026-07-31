using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using CogniteSdk;
using CogniteSdk.DataModels;
using CogniteSdk.DataModels.Core;
using CogniteSdk.Resources.Beta;
using CogniteSdk.Resources.DataModels;

namespace Cognite.Extensions.DataModels.CogniteExtractorExtensions
{
    /// <summary>
    /// Extension methods for creating and managing time series through the beta CDM time series
    /// resource. State time series (<see cref="TimeSeriesType.State"/>) are currently only
    /// available through this beta API, targeting <see cref="CogniteSdk.Resources.Beta.TimeSeriesResource"/>
    /// instead of the generic <see cref="CoreTimeSeriesResource{T}"/>. These are thin wrappers around
    /// <see cref="BetaResourceExtensions"/>, which they share with <see cref="BetaStateSetsExtensions"/>.
    /// </summary>
    public static class BetaTimeSeriesExtensions
    {
        /// <summary>
        /// Get or create the time series with the provided <paramref name="instanceIds"/> if they exist in CDF.
        /// If one or more do not exist, use the <paramref name="buildTimeSeries"/> function to construct
        /// the missing time series objects and upload them to CDF using the chunking and throttling in
        /// <paramref name="options"/>.
        /// </summary>
        /// <param name="timeSeries">CogniteSdk beta CDM TimeSeries resource</param>
        /// <param name="instanceIds">Instance Ids</param>
        /// <param name="buildTimeSeries">Function that builds CogniteSdk SourcedNodeWrite objects</param>
        /// <param name="options">Chunking, throttling, retry, and sanitation options</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>A <see cref="CogniteResult{TResult, TError}"/> containing errors that occured and a list of the created and found timeseries</returns>
        public static Task<CogniteResult<SourcedNode<T>, SourcedNodeWrite<T>>> fGetOrCreateTimeSeriesAsync<T>(
            this TimeSeriesResource timeSeries,
            IEnumerable<InstanceIdentifier> instanceIds,
            Func<IEnumerable<InstanceIdentifier>, IEnumerable<SourcedNodeWrite<T>>> buildTimeSeries,
            BetaResourceParams options,
            CancellationToken token) where T : CogniteTimeSeriesBase
        {
            return BetaResourceExtensions.GetOrCreateAsync(
                TimeSeriesResource.View,
                (ids, tok) => timeSeries.RetrieveAsync<T>(ids, tok),
                (items, tok) => timeSeries.UpsertAsync(items, null, tok),
                CoreTSSanitation.CleanTimeSeriesRequest,
                instanceIds, buildTimeSeries, options, token);
        }

        /// <summary>
        /// Get or create the time series with the provided <paramref name="instanceIds"/> if they exist in CDF.
        /// If one or more do not exist, use the <paramref name="buildTimeSeries"/> function to construct
        /// the missing time series objects and upload them to CDF using the chunking and throttling in
        /// <paramref name="options"/>.
        /// </summary>
        /// <param name="timeSeries">CogniteSdk beta CDM TimeSeries resource</param>
        /// <param name="instanceIds">Instance Ids</param>
        /// <param name="buildTimeSeries">Async function that builds CogniteSdk SourcedNodeWrite objects</param>
        /// <param name="options">Chunking, throttling, retry, and sanitation options</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>A <see cref="CogniteResult{TResult, TError}"/> containing errors that occured and a list of the created and found timeseries</returns>
        public static Task<CogniteResult<SourcedNode<T>, SourcedNodeWrite<T>>> GetOrCreateTimeSeriesAsync<T>(
            this TimeSeriesResource timeSeries,
            IEnumerable<InstanceIdentifier> instanceIds,
            Func<IEnumerable<InstanceIdentifier>, Task<IEnumerable<SourcedNodeWrite<T>>>> buildTimeSeries,
            BetaResourceParams options,
            CancellationToken token) where T : CogniteTimeSeriesBase
        {
            if (options == null) throw new ArgumentNullException(nameof(options));
            return BetaResourceExtensions.GetOrCreateAsync(
                TimeSeriesResource.View,
                (ids, tok) => timeSeries.RetrieveAsync<T>(ids, tok),
                (items, tok) => timeSeries.UpsertAsync(items, null, tok),
                CoreTSSanitation.CleanTimeSeriesRequest,
                instanceIds, buildTimeSeries, options, token);
        }

        /// <summary>
        /// Ensures that all time series in <paramref name="timeSeriesToEnsure"/> exists in CDF.
        /// Tries to create the time series and returns when all are created or have been removed
        /// due to issues with the request.
        /// </summary>
        /// <param name="timeSeries">CogniteSdk beta CDM TimeSeries resource</param>
        /// <param name="timeSeriesToEnsure">List of CogniteSdk SourcedNodeWrite objects</param>
        /// <param name="options">Chunking, throttling, retry, and sanitation options. Keeping duplicates
        /// via the retry mode is not valid for this method.</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>A <see cref="CogniteResult{TResult, TError}"/> containing errors that occured and a list of the created timeseries</returns>
        public static Task<CogniteResult<SourcedNode<T>, SourcedNodeWrite<T>>> EnsureTimeSeriesExistsAsync<T>(
            this TimeSeriesResource timeSeries,
            IEnumerable<SourcedNodeWrite<T>> timeSeriesToEnsure,
            BetaResourceParams options,
            CancellationToken token) where T : CogniteTimeSeriesBase
        {
            if (options == null) throw new ArgumentNullException(nameof(options));
            return BetaResourceExtensions.EnsureExistsAsync(
                TimeSeriesResource.View,
                (items, tok) => timeSeries.UpsertAsync(items, null, tok),
                CoreTSSanitation.CleanTimeSeriesRequest,
                timeSeriesToEnsure, options, token);
        }

        /// <summary>
        /// Get the time series with the provided <paramref name="ids"/>. Ignore any unknown ids.
        /// </summary>
        /// <param name="timeSeries">CogniteSdk beta CDM TimeSeries resource</param>
        /// <param name="ids">List of <see cref="Identity"/> objects</param>
        /// <param name="chunkSize">Chunk size</param>
        /// <param name="throttleSize">Throttle size</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>Retrieved timeseries</returns>
        public static Task<IEnumerable<SourcedNode<T>>> GetTimeSeriesByIdsIgnoreErrors<T>(
            this TimeSeriesResource timeSeries,
            IEnumerable<Identity> ids,
            int chunkSize,
            int throttleSize,
            CancellationToken token) where T : CogniteTimeSeriesBase
        {
            return BetaResourceExtensions.GetByIdsIgnoreErrors(
                TimeSeriesResource.View,
                (chunkIds, tok) => timeSeries.RetrieveAsync<T>(chunkIds, tok),
                ids, chunkSize, throttleSize, token);
        }

        /// <summary>
        /// Upsert time series.
        /// If any items fail to be created due to duplicated instance ids, they can be removed before
        /// retrying by setting the retry mode in <paramref name="options"/>.
        /// </summary>
        /// <param name="resource">CogniteSdk beta CDM TimeSeries resource</param>
        /// <param name="items">List of timeseries updates</param>
        /// <param name="options">Chunking, throttling, retry, and sanitation options</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>A <see cref="CogniteResult{TResult, TError}"/> containing errors that occured and a list of the updated timeseries</returns>
        public static Task<CogniteResult<SlimInstance, SourcedNodeWrite<T>>> UpsertAsync<T>(
            this TimeSeriesResource resource,
            IEnumerable<SourcedNodeWrite<T>> items,
            BetaResourceParams options,
            CancellationToken token) where T : CogniteTimeSeriesBase
        {
            if (options == null) throw new ArgumentNullException(nameof(options));
            return BetaResourceExtensions.UpsertAsync(
                TimeSeriesResource.View,
                (chunkItems, tok) => resource.UpsertAsync(chunkItems, null, tok),
                CoreTSSanitation.CleanTimeSeriesRequest,
                items, options, token);
        }
    }
}
