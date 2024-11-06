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
    public static class ExtractorAssetExtensions
    {
        /// <summary>
        /// Get or create the assets with the provided <paramref name="assets"/> if they exist in CDF.
        /// If one or more do not exist, use the <paramref name="buildAsset"/> function to construct
        /// the missing asset objects and upload them to CDF using the chunking of items and throttling
        /// passed as parameters
        /// If any items fail to be created due to missing assets, duplicated externalId, duplicated
        /// legacy name, or missing dataSetId, they can be removed before retrying by setting <paramref name="retryMode"/>
        /// </summary>
        /// <param name="resource">CogniteSdk CDM Asset resource</param>
        /// <param name="assets">Asset instance ids</param>
        /// <param name="buildAsset">Function that builds CogniteSdk SourcedNodeWrite objects</param>
        /// <param name="chunkSize">Chunk size</param>
        /// <param name="throttleSize">Throttle size</param>
        /// <param name="retryMode">How to handle failed requests</param>
        /// <param name="sanitationMode">The type of sanitation to apply to assets before creating</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>A <see cref="CogniteResult{TResult, TError}"/> containing errors that occured and a list of the created and found assets</returns>
        public static Task<CogniteResult<SourcedNode<T>, SourcedNodeWrite<T>>> GetOrCreateAssetsAsync<T>(
            this CoreAssetResource<T> resource,
            IEnumerable<InstanceIdentifier> assets,
            Func<IEnumerable<InstanceIdentifier>, IEnumerable<SourcedNodeWrite<T>>> buildAsset,
            int chunkSize,
            int throttleSize,
            RetryMode retryMode,
            SanitationMode sanitationMode,
            CancellationToken token) where T : CogniteExtractorAsset
        {
            Task<IEnumerable<SourcedNodeWrite<T>>> asyncBuildAsset(IEnumerable<InstanceIdentifier> ids)
            {
                return Task.FromResult(buildAsset(ids));
            }
            return resource.GetOrCreateAssetsAsync<T>(assets, asyncBuildAsset,
                chunkSize, throttleSize, retryMode, sanitationMode, token);
        }

        /// <summary>
        /// Get or create the assets with the provided <paramref name="assets"/> if they exist in CDF.
        /// If one or more do not exist, use the <paramref name="buildAsset"/> function to construct
        /// the missing asset objects and upload them to CDF using the chunking of items and throttling
        /// passed as parameters
        /// If any items fail to be created due to missing assets, duplicated externalId, duplicated
        /// legacy name, or missing dataSetId, they can be removed before retrying by setting <paramref name="retryMode"/>
        /// </summary>
        /// <param name="resource">CogniteSdk CDM Asset resource</param>
        /// <param name="assets">Asset instance ids</param>
        /// <param name="buildAsset">Async function that builds CogniteSdk SourcedNodeWrite objects</param>
        /// <param name="chunkSize">Chunk size</param>
        /// <param name="throttleSize">Throttle size</param>
        /// <param name="retryMode">How to handle failed requests</param>
        /// <param name="sanitationMode">The type of sanitation to apply to assets before creating</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>A <see cref="CogniteResult{TResult, TError}"/> containing errors that occured and a list of the created and found assets</returns>
        public static async Task<CogniteResult<SourcedNode<T>, SourcedNodeWrite<T>>> GetOrCreateAssetsAsync<T>(
            this CoreAssetResource<T> resource,
            IEnumerable<InstanceIdentifier> assets,
            Func<IEnumerable<InstanceIdentifier>, Task<IEnumerable<SourcedNodeWrite<T>>>> buildAsset,
            int chunkSize,
            int throttleSize,
            RetryMode retryMode,
            SanitationMode sanitationMode,
            CancellationToken token) where T : CogniteExtractorAsset
        {
            return await DataModelUtils.GetOrCreateResourcesAsync(resource, assets, buildAsset, DataModelSanitation.CleanInstanceRequest, chunkSize, throttleSize, retryMode, sanitationMode, token).ConfigureAwait(false);
        }

        /// <summary>
        /// Ensures that all assets in <paramref name="assetsToEnsure"/> exists in CDF.
        /// Tries to create the assets and returns when all are created or have been removed
        /// due to issues with the request.
        /// If any items fail to be created due to missing assets, duplicated externalId, duplicated
        /// legacy name, or missing dataSetId, they can be removed before retrying by setting <paramref name="retryMode"/>
        /// Assets will be returned in the same order as given in <paramref name="assetsToEnsure"/>
        /// </summary>
        /// <param name="resource">CogniteSdk CDM Asset resource</param>
        /// <param name="assetsToEnsure">List of CogniteSdk SourcedNodeWrite objects</param>
        /// <param name="chunkSize">Chunk size</param>
        /// <param name="throttleSize">Throttle size</param>
        /// <param name="retryMode">How to do retries. Keeping duplicates is not valid for
        /// this method.</param>
        /// <param name="sanitationMode">The type of sanitation to apply to assets before creating</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>A <see cref="CogniteResult{TResult, TError}"/> containing errors that occured and a list of the created assets</returns>
        public static async Task<CogniteResult<SourcedNode<T>, SourcedNodeWrite<T>>> EnsureAssetsExistsAsync<T>(
            this CoreAssetResource<T> resource,
            IEnumerable<SourcedNodeWrite<T>> assetsToEnsure,
            int chunkSize,
            int throttleSize,
            RetryMode retryMode,
            SanitationMode sanitationMode,
            CancellationToken token) where T : CogniteAssetBase
        {
            return await DataModelUtils.EnsureResourcesExistsAsync(resource, assetsToEnsure, DataModelSanitation.CleanInstanceRequest, chunkSize, throttleSize, retryMode, sanitationMode, token).ConfigureAwait(false);
        }

        /// <summary>
        /// Get the assets with the provided <paramref name="ids"/>. Ignore any
        /// unknown ids
        /// </summary>
        /// <param name="resource">CogniteSdk CDM Asset resource</param>
        /// <param name="ids">List of <see cref="Identity"/> objects</param>
        /// <param name="chunkSize">Chunk size</param>
        /// <param name="throttleSize">Throttle size</param>
        /// <param name="token">Cancellation token</param>
        /// <returns></returns>
        public static async Task<IEnumerable<SourcedNode<T>>> GetAssetsByIdsIgnoreErrors<T>(
            this CoreAssetResource<T> resource,
            IEnumerable<Identity> ids,
            int chunkSize,
            int throttleSize,
            CancellationToken token) where T : CogniteAssetBase
        {
            return await DataModelUtils.GetResourcesByIdsIgnoreErrors<T, CoreAssetResource<T>>(resource, ids, chunkSize, throttleSize, token).ConfigureAwait(false);
        }

        /// <summary>
        /// Upsert assets.
        /// If any items fail to be created due to duplicated instance ids, they can be removed before retrying by setting <paramref name="retryMode"/>
        /// Assets will be returned in the same order as given in <paramref name="items"/>
        /// </summary>
        /// <param name="resource">CogniteSdk CDM Asset resource</param>
        /// <param name="items">List of asset updates</param>
        /// <param name="chunkSize">Maximum number of assets per request</param>
        /// <param name="throttleSize">Maximum number of parallel requests</param>
        /// <param name="retryMode">How to handle retries</param>
        /// <param name="sanitationMode">What kind of pre-request sanitation to perform</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>A <see cref="CogniteResult{TResult, TError}"/> containing errors that occured and a list of the updated assets</returns>
        public static async Task<CogniteResult<SlimInstance, SourcedNodeWrite<T>>> UpsertAsync<T>(
            this CoreAssetResource<T> resource,
            IEnumerable<SourcedNodeWrite<T>> items,
            int chunkSize,
            int throttleSize,
            RetryMode retryMode,
            SanitationMode sanitationMode,
            CancellationToken token) where T : CogniteAssetBase
        {
            return await DataModelUtils.UpsertAsync(resource, items, DataModelSanitation.CleanInstanceRequest, chunkSize, throttleSize, retryMode, sanitationMode, token).ConfigureAwait(false);
        }
    }
}