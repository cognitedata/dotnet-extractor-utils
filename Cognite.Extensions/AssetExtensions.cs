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
        /// <param name="removeDirty">Whether to remove assets that do not satisfy CDF limits, or modify them to fit</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>A <see cref="CogniteResult"/> containing errors that occured and a list of the created and found assets</returns>
        public static Task<CogniteResult<Asset>> GetOrCreateAsync(
            this AssetsResource assets,
            IEnumerable<string> externalIds,
            Func<IEnumerable<string>, IEnumerable<AssetCreate>> buildAssets,
            int chunkSize,
            int throttleSize,
            RetryMode retryMode,
            bool removeDirty,
            CancellationToken token)
        {
            Task<IEnumerable<AssetCreate>> asyncBuildAssets(IEnumerable<string> ids)
            {
                return Task.FromResult(buildAssets(ids));
            }
            return assets.GetOrCreateAsync(externalIds, asyncBuildAssets, chunkSize, throttleSize, retryMode, removeDirty, token);
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
        /// <param name="removeDirty">Whether to remove assets that do not satisfy CDF limits, or modify them to fit</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>A <see cref="CogniteResult"/> containing errors that occured and a list of the created and found assets</returns>
        public static async Task<CogniteResult<Asset>> GetOrCreateAsync(
            this AssetsResource assets,
            IEnumerable<string> externalIds,
            Func<IEnumerable<string>, Task<IEnumerable<AssetCreate>>> buildAssets,
            int chunkSize,
            int throttleSize,
            RetryMode retryMode,
            bool removeDirty,
            CancellationToken token)
        {
            var chunks = externalIds
                .ChunkBy(chunkSize)
                .ToList();
            if (!chunks.Any()) return new CogniteResult<Asset>(null, null);

            var results = new CogniteResult<Asset>[chunks.Count];

            _logger.LogDebug("Getting or creating assets. Number of external ids: {Number}. Number of chunks: {Chunks}", externalIds.Count(), chunks.Count());
            var generators = chunks
                .Select<IEnumerable<string>, Func<Task>>(
                    (chunk, idx) => async () => {
                        var result = await GetOrCreateAssetsChunk(assets, chunk, buildAssets, 0, retryMode, removeDirty, token);
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
                token);
                 
            return CogniteResult<Asset>.Merge(results);
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
        /// <param name="removeDirty">Whether to remove assets that do not satisfy CDF limits, or modify them to fit</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>A <see cref="CogniteResult"/> containing errors that occured and a list of the created assets</returns>
        public static async Task<CogniteResult<Asset>> EnsureExistsAsync(
            this AssetsResource assets,
            IEnumerable<AssetCreate> assetsToEnsure,
            int chunkSize,
            int throttleSize,
            RetryMode retryMode,
            bool removeDirty,
            CancellationToken token)
        {
            IEnumerable<CogniteError> errors;
            (assetsToEnsure, errors) = Sanitation.CleanAssetRequest(assetsToEnsure, removeDirty);

            var chunks = assetsToEnsure
                .ChunkBy(chunkSize)
                .ToList();
            _logger.LogDebug("Ensuring assets. Number of assets: {Number}. Number of chunks: {Chunks}", assetsToEnsure.Count(), chunks.Count());
            int size = chunks.Count + (errors.Any() ? 1 : 0);
            var results = new CogniteResult<Asset>[size];
            if (errors.Any())
            {
                results[size - 1] = new CogniteResult<Asset>(errors, null);
                if (size == 1) return results[size - 1];
            }
            if (size == 0) return new CogniteResult<Asset>(null, null);

            var generators = chunks
                .Select<IEnumerable<AssetCreate>, Func<Task>>(
                (chunk, idx) => async () => {
                    var result = await CreateAssetsHandleErrors(assets, chunk, retryMode, token);
                    results[idx] = result;
                });

            int taskNum = 0;
            await generators.RunThrottled(
                throttleSize,
                (_) => {
                    if (chunks.Count() > 1)
                        _logger.LogDebug("{MethodName} completed {NumDone}/{TotalNum} tasks",
                            nameof(EnsureExistsAsync), ++taskNum, chunks.Count());
                },
                token);

            return CogniteResult<Asset>.Merge(results);
        }

        private static async Task<CogniteResult<Asset>> GetOrCreateAssetsChunk(
            AssetsResource assets,
            IEnumerable<string> externalIds,
            Func<IEnumerable<string>, Task<IEnumerable<AssetCreate>>> buildAssets,
            int backoff,
            RetryMode retryMode,
            bool removeDirty,
            CancellationToken token)
        {
            IEnumerable<Asset> found;
            using (CdfMetrics.Assets.WithLabels("retrieve").NewTimer())
            {
                found = await assets.RetrieveAsync(externalIds.Select(Identity.Create), true, token);
            }
            _logger.LogDebug("Retrieved {Existing} assets from CDF", found.Count());

            var missing = externalIds.Except(found.Select(asset => asset.ExternalId)).ToList();

            if (!missing.Any())
            {
                return new CogniteResult<Asset>(null, found);
            }

            _logger.LogDebug("Could not fetch {Missing} out of {Found} assets. Attempting to create the missing ones", missing.Count, externalIds.Count());
            var toCreate = await buildAssets(missing);

            IEnumerable<CogniteError> errors;
            (toCreate, errors) = Sanitation.CleanAssetRequest(toCreate, removeDirty);

            var result = await CreateAssetsHandleErrors(assets, toCreate, retryMode, token);
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
                    if (!error.Values?.Any() ?? false) continue;
                    foreach (var idt in error.Values) duplicatedIds.Add(idt.ExternalId);
                }
            }

            if (!duplicatedIds.Any()) return result;
            _logger.LogDebug("Found {cnt} duplicated assets, retrying", duplicatedIds.Count);

            await Task.Delay(TimeSpan.FromSeconds(0.1 * Math.Pow(2, backoff)));
            var nextResult = await GetOrCreateAssetsChunk(assets, duplicatedIds, buildAssets, backoff + 1, retryMode, removeDirty, token);
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
                        found = await assets.RetrieveAsync(chunk, true, token);
                    }
                    lock (mutex)
                    {
                        result.AddRange(found);
                    }
                });
            int numTasks = 0;
            await generators.RunThrottled(throttleSize, (_) =>
                _logger.LogDebug("{MethodName} completed {NumDone}/{TotalNum} tasks", nameof(GetAssetsByIdsIgnoreErrors), ++numTasks, chunks.Count),
                token);
            return result;
        }

        private static async Task<CogniteResult<Asset>> CreateAssetsHandleErrors(
            AssetsResource assets,
            IEnumerable<AssetCreate> toCreate,
            RetryMode retryMode,
            CancellationToken token)
        {
            var errors = new List<CogniteError>();
            while (toCreate != null && toCreate.Any() && !token.IsCancellationRequested)
            {
                try
                {
                    IEnumerable<Asset> newAssets;
                    using (CdfMetrics.Assets.WithLabels("create").NewTimer())
                    {
                        newAssets = await assets.CreateAsync(toCreate, token);
                    }

                    _logger.LogDebug("Created {New} new assets in CDF", newAssets.Count());
                    return new CogniteResult<Asset>(errors, newAssets);
                }
                catch (Exception ex)
                {
                    _logger.LogDebug("Failed to create {cnt} assets: {msg}",
                        toCreate.Count(), ex.Message);
                    var error = ResultHandlers.ParseException(ex, RequestType.CreateAssets);
                    errors.Add(error);
                    if (error.Type == ErrorType.FatalFailure
                        && (retryMode == RetryMode.OnFatal
                            || retryMode == RetryMode.OnFatalKeepDuplicates))
                    {
                        await Task.Delay(1000);
                    }
                    else if (retryMode == RetryMode.None) break;
                    else
                    {
                        toCreate = await ResultHandlers.CleanFromError(assets, error, toCreate, 1000, 1, token);
                    }
                }
            }
            return new CogniteResult<Asset>(errors, null);
        }
    }
}
