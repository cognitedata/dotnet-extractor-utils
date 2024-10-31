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
        /// Get or create the asset with the provided <paramref name="instanceIds"/> if they exist in CDF.
        /// If one or more do not exist, use the <paramref name="buildAsset"/> function to construct
        /// the missing asset objects and upload them to CDF using the chunking of items and throttling
        /// passed as parameters
        /// If any items fail to be created due to missing asset, duplicated externalId, duplicated
        /// legacy name, or missing dataSetId, they can be removed before retrying by setting <paramref name="retryMode"/>
        /// </summary>
        /// <param name="asset">Cognite asset resource</param>
        /// <param name="instanceIds">Instance Ids</param>
        /// <param name="buildAsset">Function that builds CogniteSdk SourcedNodeWrite objects</param>
        /// <param name="chunkSize">Chunk size</param>
        /// <param name="throttleSize">Throttle size</param>
        /// <param name="retryMode">How to handle failed requests</param>
        /// <param name="sanitationMode">The type of sanitation to apply to asset before creating</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>A <see cref="CogniteResult{TResult, TError}"/> containing errors that occured and a list of the created and found asset</returns>
        public static Task<CogniteResult<SourcedNode<T>, SourcedNodeWrite<T>>> GetOrCreateAssetAsync<T>(
            this CoreAssetResource<T> asset,
            IEnumerable<InstanceIdentifier> instanceIds,
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
            return asset.GetOrCreateAssetAsync<T>(instanceIds, asyncBuildAsset,
                chunkSize, throttleSize, retryMode, sanitationMode, token);
        }

        /// <summary>
        /// Get or create the asset with the provided <paramref name="instanceIds"/> if they exist in CDF.
        /// If one or more do not exist, use the <paramref name="buildAsset"/> function to construct
        /// the missing asset objects and upload them to CDF using the chunking of items and throttling
        /// passed as parameters
        /// If any items fail to be created due to missing asset, duplicated externalId, duplicated
        /// legacy name, or missing dataSetId, they can be removed before retrying by setting <paramref name="retryMode"/>
        /// </summary>
        /// <param name="asset">Cognite client</param>
        /// <param name="instanceIds">External Ids</param>
        /// <param name="buildAsset">Async function that builds CogniteSdk SourcedNodeWrite objects</param>
        /// <param name="chunkSize">Chunk size</param>
        /// <param name="throttleSize">Throttle size</param>
        /// <param name="retryMode">How to handle failed requests</param>
        /// <param name="sanitationMode">The type of sanitation to apply to asset before creating</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>A <see cref="CogniteResult{TResult, TError}"/> containing errors that occured and a list of the created and found asset</returns>
        public static async Task<CogniteResult<SourcedNode<T>, SourcedNodeWrite<T>>> GetOrCreateAssetAsync<T>(
            this CoreAssetResource<T> asset,
            IEnumerable<InstanceIdentifier> instanceIds,
            Func<IEnumerable<InstanceIdentifier>, Task<IEnumerable<SourcedNodeWrite<T>>>> buildAsset,
            int chunkSize,
            int throttleSize,
            RetryMode retryMode,
            SanitationMode sanitationMode,
            CancellationToken token) where T : CogniteExtractorAsset
        {
            return await DataModelUtils.GetOrCreateResourceAsync(asset, instanceIds, buildAsset, DataModelSanitation.CleanInstanceRequest, chunkSize, throttleSize, retryMode, sanitationMode, token).ConfigureAwait(false);
        }

        /// <summary>
        /// Ensures that all asset in <paramref name="assetToEnsure"/> exists in CDF.
        /// Tries to create the asset and returns when all are created or have been removed
        /// due to issues with the request.
        /// If any items fail to be created due to missing asset, duplicated externalId, duplicated
        /// legacy name, or missing dataSetId, they can be removed before retrying by setting <paramref name="retryMode"/>
        /// Asset will be returned in the same order as given in <paramref name="assetToEnsure"/>
        /// </summary>
        /// <param name="asset">Cognite client</param>
        /// <param name="assetToEnsure">List of CogniteSdk SourcedNodeWrite objects</param>
        /// <param name="chunkSize">Chunk size</param>
        /// <param name="throttleSize">Throttle size</param>
        /// <param name="retryMode">How to do retries. Keeping duplicates is not valid for
        /// this method.</param>
        /// <param name="sanitationMode">The type of sanitation to apply to asset before creating</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>A <see cref="CogniteResult{TResult, TError}"/> containing errors that occured and a list of the created asset</returns>
        public static async Task<CogniteResult<SourcedNode<T>, SourcedNodeWrite<T>>> EnsureAssetExistsAsync<T>(
            this CoreAssetResource<T> asset,
            IEnumerable<SourcedNodeWrite<T>> assetToEnsure,
            int chunkSize,
            int throttleSize,
            RetryMode retryMode,
            SanitationMode sanitationMode,
            CancellationToken token) where T : CogniteAssetBase
        {
            return await DataModelUtils.EnsureResourceExistsAsync(asset, assetToEnsure, DataModelSanitation.CleanInstanceRequest, chunkSize, throttleSize, retryMode, sanitationMode, token).ConfigureAwait(false);
        }

        /// <summary>
        /// Get the asset with the provided <paramref name="ids"/>. Ignore any
        /// unknown ids
        /// </summary>
        /// <param name="asset">A CogniteSdk Asset resource</param>
        /// <param name="ids">List of <see cref="Identity"/> objects</param>
        /// <param name="chunkSize">Chunk size</param>
        /// <param name="throttleSize">Throttle size</param>
        /// <param name="token">Cancellation token</param>
        /// <returns></returns>
        public static async Task<IEnumerable<SourcedNode<T>>> GetAssetByIdsIgnoreErrors<T>(
            this CoreAssetResource<T> asset,
            IEnumerable<Identity> ids,
            int chunkSize,
            int throttleSize,
            CancellationToken token) where T : CogniteAssetBase
        {
            return await DataModelUtils.GetResourceByIdsIgnoreErrors<T, CoreAssetResource<T>>(asset, ids, chunkSize, throttleSize, token).ConfigureAwait(false);
        }

        /// <summary>
        /// Upsert asset.
        /// If any items fail to be created due to duplicated instance ids, they can be removed before retrying by setting <paramref name="retryMode"/>
        /// Asset will be returned in the same order as given in <paramref name="items"/>
        /// </summary>
        /// <param name="resource">CogniteSdk asset resource</param>
        /// <param name="items">List of asset updates</param>
        /// <param name="chunkSize">Maximum number of asset per request</param>
        /// <param name="throttleSize">Maximum number of parallel requests</param>
        /// <param name="retryMode">How to handle retries</param>
        /// <param name="sanitationMode">What kind of pre-request sanitation to perform</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>A <see cref="CogniteResult{TResult, TError}"/> containing errors that occured and a list of the updated asset</returns>
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