using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using CogniteSdk;
using CogniteSdk.DataModels;
using CogniteSdk.DataModels.Core;
using CogniteSdk.Resources.DataModels;

namespace Cognite.Extensions.DataModels.CogniteExtractorExtensions
{
    /// <summary>
    /// Extension utility methods for <see cref="Client"/>
    /// </summary>
    public static class ExtractorTimeSeriesExtensions
    {
        /// <summary>
        /// Get or create the time series with the provided <paramref name="instanceIds"/> if they exist in CDF.
        /// If one or more do not exist, use the <paramref name="buildTimeSeries"/> function to construct
        /// the missing time series objects and upload them to CDF using the chunking of items and throttling
        /// passed as parameters
        /// If any items fail to be created due to missing asset, duplicated externalId, duplicated
        /// legacy name, or missing dataSetId, they can be removed before retrying by setting <paramref name="retryMode"/>
        /// </summary>
        /// <param name="timeSeries">CogniteSdk CDM TimeSeries resource</param>
        /// <param name="instanceIds">Instance Ids</param>
        /// <param name="buildTimeSeries">Function that builds CogniteSdk SourcedNodeWrite objects</param>
        /// <param name="chunkSize">Chunk size</param>
        /// <param name="throttleSize">Throttle size</param>
        /// <param name="retryMode">How to handle failed requests</param>
        /// <param name="sanitationMode">The type of sanitation to apply to timeseries before creating</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>A <see cref="CogniteResult{TResult, TError}"/> containing errors that occured and a list of the created and found timeseries</returns>
        public static Task<CogniteResult<SourcedNode<T>, SourcedNodeWrite<T>>> GetOrCreateTimeSeriesAsync<T, TValue>(
            this CoreTimeSeriesResource<T> timeSeries,
            IEnumerable<InstanceIdentifier> instanceIds,
            Func<IEnumerable<InstanceIdentifier>, IEnumerable<SourcedNodeWrite<T>>> buildTimeSeries,
            int chunkSize,
            int throttleSize,
            RetryMode retryMode,
            SanitationMode sanitationMode,
            CancellationToken token) where T : CogniteTimeSeriesBase
        {
            Task<IEnumerable<SourcedNodeWrite<T>>> asyncBuildTimeSeries(IEnumerable<InstanceIdentifier> ids)
            {
                return Task.FromResult(buildTimeSeries(ids));
            }
            return timeSeries.GetOrCreateTimeSeriesAsync<T, TValue>(instanceIds, asyncBuildTimeSeries,
                chunkSize, throttleSize, retryMode, sanitationMode, token);
        }

        /// <summary>
        /// Get or create the time series with the provided <paramref name="instanceIds"/> if they exist in CDF.
        /// If one or more do not exist, use the <paramref name="buildTimeSeries"/> function to construct
        /// the missing time series objects and upload them to CDF using the chunking of items and throttling
        /// passed as parameters
        /// If any items fail to be created due to missing asset, duplicated externalId, duplicated
        /// legacy name, or missing dataSetId, they can be removed before retrying by setting <paramref name="retryMode"/>
        /// </summary>
        /// <param name="timeSeries">CogniteSdk CDM TimeSeries resource</param>
        /// <param name="instanceIds">External Ids</param>
        /// <param name="buildTimeSeries">Async function that builds CogniteSdk SourcedNodeWrite objects</param>
        /// <param name="chunkSize">Chunk size</param>
        /// <param name="throttleSize">Throttle size</param>
        /// <param name="retryMode">How to handle failed requests</param>
        /// <param name="sanitationMode">The type of sanitation to apply to timeseries before creating</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>A <see cref="CogniteResult{TResult, TError}"/> containing errors that occured and a list of the created and found timeseries</returns>
        public static async Task<CogniteResult<SourcedNode<T>, SourcedNodeWrite<T>>> GetOrCreateTimeSeriesAsync<T, TValue>(
            this CoreTimeSeriesResource<T> timeSeries,
            IEnumerable<InstanceIdentifier> instanceIds,
            Func<IEnumerable<InstanceIdentifier>, Task<IEnumerable<SourcedNodeWrite<T>>>> buildTimeSeries,
            int chunkSize,
            int throttleSize,
            RetryMode retryMode,
            SanitationMode sanitationMode,
            CancellationToken token) where T : CogniteTimeSeriesBase
        {
            return await DataModelUtils.GetOrCreateResourcesAsync(timeSeries, instanceIds, buildTimeSeries, CoreTSSanitation.CleanTimeSeriesRequest, chunkSize, throttleSize, retryMode, sanitationMode, token).ConfigureAwait(false);
        }

        /// <summary>
        /// Ensures that all time series in <paramref name="timeSeriesToEnsure"/> exists in CDF.
        /// Tries to create the time series and returns when all are created or have been removed
        /// due to issues with the request.
        /// If any items fail to be created due to missing asset, duplicated externalId, duplicated
        /// legacy name, or missing dataSetId, they can be removed before retrying by setting <paramref name="retryMode"/>
        /// Timeseries will be returned in the same order as given in <paramref name="timeSeriesToEnsure"/>
        /// </summary>
        /// <param name="timeSeries">CogniteSdk CDM TimeSeries resource</param>
        /// <param name="timeSeriesToEnsure">List of CogniteSdk SourcedNodeWrite objects</param>
        /// <param name="chunkSize">Chunk size</param>
        /// <param name="throttleSize">Throttle size</param>
        /// <param name="retryMode">How to do retries. Keeping duplicates is not valid for
        /// this method.</param>
        /// <param name="sanitationMode">The type of sanitation to apply to timeseries before creating</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>A <see cref="CogniteResult{TResult, TError}"/> containing errors that occured and a list of the created timeseries</returns>
        public static async Task<CogniteResult<SourcedNode<T>, SourcedNodeWrite<T>>> EnsureTimeSeriesExistsAsync<T>(
            this CoreTimeSeriesResource<T> timeSeries,
            IEnumerable<SourcedNodeWrite<T>> timeSeriesToEnsure,
            int chunkSize,
            int throttleSize,
            RetryMode retryMode,
            SanitationMode sanitationMode,
            CancellationToken token) where T : CogniteTimeSeriesBase
        {
            return await DataModelUtils.EnsureResourcesExistsAsync(timeSeries, timeSeriesToEnsure, CoreTSSanitation.CleanTimeSeriesRequest, chunkSize, throttleSize, retryMode, sanitationMode, token).ConfigureAwait(false);
        }

        /// <summary>
        /// Get the time series with the provided <paramref name="ids"/>. Ignore any
        /// unknown ids
        /// </summary>
        /// <param name="timeSeries">CogniteSdk CDM TimeSeries resource</param>
        /// <param name="ids">List of <see cref="Identity"/> objects</param>
        /// <param name="chunkSize">Chunk size</param>
        /// <param name="throttleSize">Throttle size</param>
        /// <param name="token">Cancellation token</param>
        /// <returns></returns>
        public static async Task<IEnumerable<SourcedNode<T>>> GetTimeSeriesByIdsIgnoreErrors<T>(
            this CoreTimeSeriesResource<T> timeSeries,
            IEnumerable<Identity> ids,
            int chunkSize,
            int throttleSize,
            CancellationToken token) where T : CogniteTimeSeriesBase
        {
            return await DataModelUtils.GetResourcesByIdsIgnoreErrors<T, CoreTimeSeriesResource<T>>(timeSeries, ids, chunkSize, throttleSize, token).ConfigureAwait(false);
        }

        /// <summary>
        /// Upsert time series.
        /// If any items fail to be created due to duplicated instance ids, they can be removed before retrying by setting <paramref name="retryMode"/>
        /// Timeseries will be returned in the same order as given in <paramref name="items"/>
        /// </summary>
        /// <param name="resource">CogniteSdk CDM TimeSeries resource</param>
        /// <param name="items">List of timeseries updates</param>
        /// <param name="chunkSize">Maximum number of timeseries per request</param>
        /// <param name="throttleSize">Maximum number of parallel requests</param>
        /// <param name="retryMode">How to handle retries</param>
        /// <param name="sanitationMode">What kind of pre-request sanitation to perform</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>A <see cref="CogniteResult{TResult, TError}"/> containing errors that occured and a list of the updated timeseries</returns>
        public static async Task<CogniteResult<SlimInstance, SourcedNodeWrite<T>>> UpsertAsync<T>(
            this CoreTimeSeriesResource<T> resource,
            IEnumerable<SourcedNodeWrite<T>> items,
            int chunkSize,
            int throttleSize,
            RetryMode retryMode,
            SanitationMode sanitationMode,
            CancellationToken token) where T : CogniteTimeSeriesBase
        {
            return await DataModelUtils.UpsertAsync(resource, items, CoreTSSanitation.CleanTimeSeriesRequest, chunkSize, throttleSize, retryMode, sanitationMode, token).ConfigureAwait(false);
        }
    }
}