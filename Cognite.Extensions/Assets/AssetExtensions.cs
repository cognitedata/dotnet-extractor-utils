using Cognite.Extractor.Common;
using CogniteSdk;
using CogniteSdk.Resources;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Prometheus;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.Extensions
{
    /// <summary>
    /// Extension utility methods for CogniteSDK Client.
    /// </summary>
    public static class AssetExtensions
    {
        private static ILogger _logger = new NullLogger<Client>();

        internal static void SetLogger(ILogger logger)
        {
            _logger = logger;
        }
        /// <summary>
        /// Get or create the assets with the provided <paramref name="externalIds"/> exist in CDF.
        /// If one or more do not exist, use the <paramref name="buildAssets"/> function to construct
        /// the missing asset objects and upload them to CDF using the chunking of items and throttling
        /// passed as parameters
        /// If any items fail to be created due to missing parent, duplicated externalId or missing dataset
        /// they can be removed before retrying by setting <paramref name="retryMode"/>.
        /// </summary>
        /// <param name="assets">Cognite assets resource</param>
        /// <param name="externalIds">External Ids</param>
        /// <param name="buildAssets">Async function that builds AssetCreate objects</param>
        /// <param name="chunkSize">Chunk size</param>
        /// <param name="throttleSize">Throttle size</param>
        /// <param name="retryMode">How to handle failed requests</param>
        /// <param name="sanitationMode">The type of sanitation to apply to assets before creating</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>A <see cref="CogniteResult{TResult, TError}"/> containing errors that occured and a list of the created and found assets</returns>
        public static Task<CogniteResult<Asset, AssetCreate>> GetOrCreateAsync(
            this AssetsResource assets,
            IEnumerable<string> externalIds,
            Func<IEnumerable<string>, IEnumerable<AssetCreate>> buildAssets,
            int chunkSize,
            int throttleSize,
            RetryMode retryMode,
            SanitationMode sanitationMode,
            CancellationToken token)
        {
            Task<IEnumerable<AssetCreate>> asyncBuildAssets(IEnumerable<string> ids)
            {
                return Task.FromResult(buildAssets(ids));
            }
            return assets.GetOrCreateAsync(externalIds, asyncBuildAssets, chunkSize, throttleSize, retryMode, sanitationMode, token);
        }
        /// <summary>
        /// Get or create the assets with the provided <paramref name="externalIds"/> exist in CDF.
        /// If one or more do not exist, use the <paramref name="buildAssets"/> function to construct
        /// the missing asset objects and upload them to CDF using the chunking of items and throttling
        /// passed as parameters
        /// If any items fail to be created due to missing parent, duplicated externalId or missing dataset
        /// they can be removed before retrying by setting <paramref name="retryMode"/>.
        /// </summary>
        /// <param name="assets">Cognite assets resource</param>
        /// <param name="externalIds">External Ids</param>
        /// <param name="buildAssets">Async function that builds AssetCreate objects</param>
        /// <param name="chunkSize">Chunk size</param>
        /// <param name="throttleSize">Throttle size</param>
        /// <param name="retryMode">How to handle failed requests</param>
        /// <param name="sanitationMode">The type of sanitation to apply to assets before creating</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>A <see cref="CogniteResult{TResult, TError}"/> containing errors that occured and a list of the created and found assets</returns>
        public static async Task<CogniteResult<Asset, AssetCreate>> GetOrCreateAsync(
            this AssetsResource assets,
            IEnumerable<string> externalIds,
            Func<IEnumerable<string>, Task<IEnumerable<AssetCreate>>> buildAssets,
            int chunkSize,
            int throttleSize,
            RetryMode retryMode,
            SanitationMode sanitationMode,
            CancellationToken token)
        {
            var chunks = externalIds
                .ChunkBy(chunkSize)
                .ToList();
            if (!chunks.Any()) return new CogniteResult<Asset, AssetCreate>(null, null);

            var results = new CogniteResult<Asset, AssetCreate>[chunks.Count];

            _logger.LogDebug("Getting or creating assets. Number of external ids: {Number}. Number of chunks: {Chunks}", externalIds.Count(), chunks.Count);
            var generators = chunks
                .Select<IEnumerable<string>, Func<Task>>(
                    (chunk, idx) => async () => {
                        var result = await GetOrCreateAssetsChunk(assets, chunk, buildAssets, chunkSize, throttleSize,
                            0, retryMode, sanitationMode, token).ConfigureAwait(false);
                        results[idx] = result;
                    });

            int taskNum = 0;
            await generators.RunThrottled(
                throttleSize,
                (_) => {
                    if (chunks.Count > 1)
                        _logger.LogDebug("{MethodName} completed {NumDone}/{TotalNum} tasks",
                            nameof(GetOrCreateAsync), ++taskNum, chunks.Count);
                },
                token).ConfigureAwait(false);
                 
            return CogniteResult<Asset, AssetCreate>.Merge(results);
        }
        /// <summary>
        /// Ensures that all assets in <paramref name="assetsToEnsure"/> exist in CDF.
        /// Tries to create the assets and returns when all are created or have been removed
        /// due to issues with the request.
        /// If any items fail to be created due to missing parent, duplicated externalId or missing dataset
        /// they can be removed before retrying by setting <paramref name="retryMode"/>.
        /// Assets will be returned in the same order as given in <paramref name="assetsToEnsure"/>.
        /// </summary>
        /// <param name="assets">Cognite assets resource</param>
        /// <param name="assetsToEnsure">List of AssetCreate objects</param>
        /// <param name="chunkSize">Chunk size</param>
        /// <param name="throttleSize">Throttle size</param>
        /// <param name="retryMode">How to do retries. Keeping duplicates is not valid for
        /// this method.</param>
        /// <param name="sanitationMode">The type of sanitation to apply to assets before creating</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>A <see cref="CogniteResult{TResult, TError}"/> containing errors that occured and a list of the created assets</returns>
        public static async Task<CogniteResult<Asset, AssetCreate>> EnsureExistsAsync(
            this AssetsResource assets,
            IEnumerable<AssetCreate> assetsToEnsure,
            int chunkSize,
            int throttleSize,
            RetryMode retryMode,
            SanitationMode sanitationMode,
            CancellationToken token)
        {
            IEnumerable<CogniteError<AssetCreate>> errors;
            (assetsToEnsure, errors) = Sanitation.CleanAssetRequest(assetsToEnsure, sanitationMode);

            var chunks = assetsToEnsure
                .ChunkBy(chunkSize)
                .ToList();
            _logger.LogDebug("Ensuring assets. Number of assets: {Number}. Number of chunks: {Chunks}", assetsToEnsure.Count(), chunks.Count);
            int size = chunks.Count + (errors.Any() ? 1 : 0);
            var results = new CogniteResult<Asset, AssetCreate>[size];
            if (errors.Any())
            {
                results[size - 1] = new CogniteResult<Asset, AssetCreate>(errors, null);
                if (size == 1) return results[size - 1];
            }
            if (size == 0) return new CogniteResult<Asset, AssetCreate>(null, null);

            var generators = chunks
                .Select<IEnumerable<AssetCreate>, Func<Task>>(
                (chunk, idx) => async () => {
                    var result = await CreateAssetsHandleErrors(assets, chunk, chunkSize, throttleSize, retryMode, token).ConfigureAwait(false);
                    results[idx] = result;
                });

            int taskNum = 0;
            await generators.RunThrottled(
                throttleSize,
                (_) => {
                    if (chunks.Count > 1)
                        _logger.LogDebug("{MethodName} completed {NumDone}/{TotalNum} tasks",
                            nameof(EnsureExistsAsync), ++taskNum, chunks.Count);
                },
                token).ConfigureAwait(false);

            return CogniteResult<Asset, AssetCreate>.Merge(results);
        }

        private static async Task<CogniteResult<Asset, AssetCreate>> GetOrCreateAssetsChunk(
            AssetsResource assets,
            IEnumerable<string> externalIds,
            Func<IEnumerable<string>, Task<IEnumerable<AssetCreate>>> buildAssets,
            int chunkSize,
            int throttleSize,
            int backoff,
            RetryMode retryMode,
            SanitationMode sanitationMode,
            CancellationToken token)
        {
            IEnumerable<Asset> found;
            using (CdfMetrics.Assets.WithLabels("retrieve").NewTimer())
            {
                found = await assets.RetrieveAsync(externalIds.Select(Identity.Create), true, token).ConfigureAwait(false);
            }
            _logger.LogDebug("Retrieved {Existing} assets from CDF", found.Count());

            var missing = externalIds.Except(found.Select(asset => asset.ExternalId)).ToList();

            if (!missing.Any())
            {
                return new CogniteResult<Asset, AssetCreate>(null, found);
            }

            _logger.LogDebug("Could not fetch {Missing} out of {Found} assets. Attempting to create the missing ones", missing.Count, externalIds.Count());
            var toCreate = await buildAssets(missing).ConfigureAwait(false);

            IEnumerable<CogniteError<AssetCreate>> errors;
            (toCreate, errors) = Sanitation.CleanAssetRequest(toCreate, sanitationMode);

            var result = await CreateAssetsHandleErrors(assets, toCreate, chunkSize, throttleSize, retryMode, token).ConfigureAwait(false);
            result.Results = result.Results == null ? found : result.Results.Concat(found);

            if (errors.Any())
            {
                result.Errors = result.Errors == null ? errors : result.Errors.Concat(errors);
            }

            if (!result.Errors?.Any() ?? false
                || retryMode != RetryMode.OnErrorKeepDuplicates
                && retryMode != RetryMode.OnFatalKeepDuplicates) return result;

            var duplicateErrors = result.Errors.Where(err =>
                err.Resource == ResourceType.ExternalId
                && err.Type == ErrorType.ItemExists)
                .ToList();

            var duplicatedIds = new HashSet<string>();
            if (duplicateErrors.Any())
            {
                foreach (var error in duplicateErrors)
                {
                    if (error.Values == null || !error.Values.Any()) continue;
                    foreach (var idt in error.Values) duplicatedIds.Add(idt.ExternalId);
                }
            }

            if (!duplicatedIds.Any()) return result;
            _logger.LogDebug("Found {cnt} duplicated assets, retrying", duplicatedIds.Count);

            await Task
                .Delay(TimeSpan.FromSeconds(0.1 * Math.Pow(2, backoff)), token)
                .ConfigureAwait(false);
            var nextResult = await GetOrCreateAssetsChunk(assets, duplicatedIds, buildAssets,
                chunkSize, throttleSize, backoff + 1, retryMode, sanitationMode, token)
                .ConfigureAwait(false);
            result = result.Merge(nextResult);

            return result;
        }

        /// <summary>
        /// Get the assets with the provided <paramref name="ids"/>. Ignore any
        /// unknown ids
        /// </summary>
        /// <param name="assets">A CogniteSdk Assets resource</param>
        /// <param name="ids">List of <see cref="Identity"/> objects</param>
        /// <param name="chunkSize">Chunk size</param>
        /// <param name="throttleSize">Throttle size</param>
        /// <param name="token">Cancellation token</param>
        /// <returns></returns>
        public static async Task<IEnumerable<Asset>> GetAssetsByIdsIgnoreErrors(
            this AssetsResource assets,
            IEnumerable<Identity> ids,
            int chunkSize,
            int throttleSize,
            CancellationToken token)
        {
            var result = new List<Asset>();
            object mutex = new object();

            var chunks = ids
                .ChunkBy(chunkSize)
                .ToList();

            var generators = chunks
                .Select<IEnumerable<Identity>, Func<Task>>(
                chunk => async () => {
                    IEnumerable<Asset> found;
                    using (CdfMetrics.Assets.WithLabels("retrieve").NewTimer())
                    {
                        found = await assets.RetrieveAsync(chunk, true, token).ConfigureAwait(false);
                    }
                    lock (mutex)
                    {
                        result.AddRange(found);
                    }
                });
            int numTasks = 0;
            await generators.RunThrottled(throttleSize, (_) =>
                _logger.LogDebug("{MethodName} completed {NumDone}/{TotalNum} tasks", nameof(GetAssetsByIdsIgnoreErrors), ++numTasks, chunks.Count),
                token).ConfigureAwait(false);
            return result;
        }

        private static async Task<CogniteResult<Asset, AssetCreate>> CreateAssetsHandleErrors(
            AssetsResource assets,
            IEnumerable<AssetCreate> toCreate,
            int assetsChunk,
            int throttleSize,
            RetryMode retryMode,
            CancellationToken token)
        {
            var errors = new List<CogniteError<AssetCreate>>();
            while (toCreate != null && toCreate.Any() && !token.IsCancellationRequested)
            {
                try
                {
                    IEnumerable<Asset> newAssets;
                    using (CdfMetrics.Assets.WithLabels("create").NewTimer())
                    {
                        newAssets = await assets
                            .CreateAsync(toCreate, token)
                            .ConfigureAwait(false);
                    }

                    _logger.LogDebug("Created {New} new assets in CDF", newAssets.Count());
                    return new CogniteResult<Asset, AssetCreate>(errors, newAssets);
                }
                catch (Exception ex)
                {
                    _logger.LogDebug("Failed to create {cnt} assets: {msg}",
                        toCreate.Count(), ex.Message);
                    var error = ResultHandlers.ParseException<AssetCreate>(ex, RequestType.CreateAssets);
                    errors.Add(error);
                    if (error.Type == ErrorType.FatalFailure
                        && (retryMode == RetryMode.OnFatal
                            || retryMode == RetryMode.OnFatalKeepDuplicates))
                    {
                        await Task.Delay(1000, token).ConfigureAwait(false);
                    }
                    else if (retryMode == RetryMode.None) break;
                    else
                    {
                        if (!error.Complete) await ResultHandlers
                                .CompleteAssetError(assets, error, toCreate, assetsChunk, throttleSize, token)
                                .ConfigureAwait(false);

                        toCreate = ResultHandlers.CleanFromError(error, toCreate);
                    }
                }
            }
            return new CogniteResult<Asset, AssetCreate>(errors, null);
        }





        /// <summary>
        /// Attempt to update all assets in <paramref name="updates"/>, will retry
        /// and attempt to handle errors.
        /// Assets will be returned in the same order as given.
        /// </summary>
        /// <param name="assets">Cognite assets resource</param>
        /// <param name="updates">List of AssetUpdateItem objects</param>
        /// <param name="chunkSize">Chunk size</param>
        /// <param name="throttleSize">Throttle size</param>
        /// <param name="retryMode">How to do retries. Keeping duplicates is not valid for
        /// this method.</param>
        /// <param name="sanitationMode">The type of sanitation to apply to assets before updating</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>A <see cref="CogniteResult{TResult, TError}"/> containing errors that occured and a list of the updated assets</returns>
        public static async Task<CogniteResult<Asset, AssetUpdateItem>> UpdateAsync(
            this AssetsResource assets,
            IEnumerable<AssetUpdateItem> updates,
            int chunkSize,
            int throttleSize,
            RetryMode retryMode,
            SanitationMode sanitationMode,
            CancellationToken token)
        {
            IEnumerable<CogniteError<AssetUpdateItem>> errors;
            (updates, errors) = Sanitation.CleanAssetUpdateRequest(updates, sanitationMode);

            var chunks = updates
                .ChunkBy(chunkSize)
                .ToList();
            _logger.LogDebug("Updating assets. Number of assets: {Number}. Number of chunks: {Chunks}", updates.Count(), chunks.Count);
            int size = chunks.Count + (errors.Any() ? 1 : 0);
            var results = new CogniteResult<Asset, AssetUpdateItem>[size];
            if (errors.Any())
            {
                results[size - 1] = new CogniteResult<Asset, AssetUpdateItem>(errors, null);
                if (size == 1) return results[size - 1];
            }
            if (size == 0) return new CogniteResult<Asset, AssetUpdateItem>(null, null);

            var generators = chunks
                .Select<IEnumerable<AssetUpdateItem>, Func<Task>>(
                (chunk, idx) => async () => {
                    var result = await
                        UpdateAssetsHandleErrors(assets, chunk, chunkSize, throttleSize, retryMode, token)
                        .ConfigureAwait(false);
                    results[idx] = result;
                });

            int taskNum = 0;
            await generators.RunThrottled(
                throttleSize,
                (_) => {
                    if (chunks.Count > 1)
                        _logger.LogDebug("{MethodName} completed {NumDone}/{TotalNum} tasks",
                            nameof(UpdateAsync), ++taskNum, chunks.Count);
                },
                token).ConfigureAwait(false);

            return CogniteResult<Asset, AssetUpdateItem>.Merge(results);
        }

        private static async Task<CogniteResult<Asset, AssetUpdateItem>> UpdateAssetsHandleErrors(
            AssetsResource assets,
            IEnumerable<AssetUpdateItem> toUpdate,
            int assetsChunk,
            int throttleSize,
            RetryMode retryMode,
            CancellationToken token)
        {
            var errors = new List<CogniteError<AssetUpdateItem>>();
            while (toUpdate != null && toUpdate.Any() && !token.IsCancellationRequested)
            {
                try
                {
                    IEnumerable<Asset> updated;
                    using (CdfMetrics.Assets.WithLabels("update").NewTimer())
                    {
                        updated = await assets
                            .UpdateAsync(toUpdate, token)
                            .ConfigureAwait(false);
                    }

                    _logger.LogDebug("Updated {Count} assets in CDF", updated.Count());
                    return new CogniteResult<Asset, AssetUpdateItem>(errors, updated);
                }
                catch (Exception ex)
                {
                    _logger.LogDebug("Failed to update {Count} assets: {Message}",
                        toUpdate.Count(), ex.Message);
                    var error = ResultHandlers.ParseException<AssetUpdateItem>(ex, RequestType.UpdateAssets);
                    if (error.Complete) errors.Add(error);
                    if (error.Type == ErrorType.FatalFailure
                        && (retryMode == RetryMode.OnFatal
                            || retryMode == RetryMode.OnFatalKeepDuplicates))
                    {
                        await Task.Delay(1000, token).ConfigureAwait(false);
                    }
                    else if (retryMode == RetryMode.None) break;
                    else
                    {
                        if (!error.Complete)
                        {
                            var newErrors = await ResultHandlers
                                .VerifyAssetUpdateParents(assets, toUpdate, assetsChunk, throttleSize, token)
                                .ConfigureAwait(false);
                            foreach (var err in newErrors)
                            {
                                errors.Add(err);
                                toUpdate = ResultHandlers.CleanFromError(err, toUpdate);
                            }
                        }
                        else
                        {
                            toUpdate = ResultHandlers.CleanFromError(error, toUpdate);
                        }
                    }
                }
            }

            return new CogniteResult<Asset, AssetUpdateItem>(errors, null);
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
        /// <param name="assets">Assets resource</param>
        /// <param name="upserts">Assets to upsert</param>
        /// <param name="chunkSize">Number of asset creates, retrieves or updates per request</param>
        /// <param name="throttleSize">Maximum number of parallel requests</param>
        /// <param name="retryMode">How to handle retries on errors</param>
        /// <param name="sanitationMode">How to sanitize creates and updates</param>
        /// <param name="options">How to update existing assets</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>Result with failed creates/updates and list of assets</returns>
        /// <exception cref="ArgumentException">All upserted assets must have external id</exception>
        public static async Task<CogniteResult<Asset, AssetCreate>> UpsertAsync(
            this AssetsResource assets,
            IEnumerable<AssetCreate> upserts,
            int chunkSize,
            int throttleSize,
            RetryMode retryMode,
            SanitationMode sanitationMode,
            UpsertParams? options,
            CancellationToken token)
        {
            if (!upserts.All(a => a.ExternalId != null)) throw new ArgumentException("All inserts must have externalId");

            var assetDict = upserts.ToDictionary(a => a.ExternalId);

            var createResult = await assets.GetOrCreateAsync(
                assetDict.Keys,
                keys => keys.Select(key => assetDict[key]),
                chunkSize, throttleSize,
                retryMode,
                sanitationMode,
                token).ConfigureAwait(false);

            if (createResult.Errors?.Any() ?? false)
            {
                var badAssets = new HashSet<AssetCreate>(createResult.Errors.Where(e => e.Skipped != null).SelectMany(e => e.Skipped));
                assetDict = assetDict.Where(kvp => !badAssets.Contains(kvp.Value)).ToDictionary(kvp => kvp.Key, kvp => kvp.Value);
            }

            if (!assetDict.Any() || createResult.Results == null || !createResult.Results.Any()) return createResult;

            var resultDict = createResult.Results.ToDictionary(a => a.ExternalId);
            var updates = assetDict.Values
                .Select(asset => (asset.ToUpdate(resultDict[asset.ExternalId], options)!, asset))
                .Where(upd => upd.Item1 != null)
                .ToDictionary(upd => upd.Item1.Id!.Value);

            var updateResult = await assets.UpdateAsync(
                updates.Values.Select(pair => pair.Item1),
                chunkSize,
                throttleSize,
                retryMode,
                sanitationMode,
                token).ConfigureAwait(false);

            var merged = updateResult.Replace(upd => updates[upd.Id!.Value].asset).Merge(createResult);

            var resultAssets = createResult.Results;

            if (updateResult.Results != null && updateResult.Results.Any())
            {
                var updated = new HashSet<long>(updateResult.Results.Select(a => a.Id));
                var finalResultDict = resultAssets
                    .Where(asset => !updated.Contains(asset.Id))
                    .Union(updateResult.Results)
                    .ToDictionary(a => a.ExternalId);

                // To maintain the order as given we have to do this mapping if updates have been made.
                resultAssets = upserts
                    .Where(asset => finalResultDict.ContainsKey(asset.ExternalId))
                    .Select(asset => finalResultDict[asset.ExternalId])
                    .ToList();
                merged.Results = resultAssets;
            }

            return merged;
        }
    }
}
