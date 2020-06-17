using Cognite.Extractor.Common;
using Cognite.Extractor.Logging;
using CogniteSdk;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.Extractor.Utils
{
    /// <summary>
    /// Extension utility methods for <see cref="Client"/>
    /// </summary>
    public static class AssetExtensions
    {
        private static ILogger _logger = LoggingUtils.GetDefault();

        internal static void SetLogger(ILogger logger)
        {
            _logger = logger;
        }
        /// <summary>
        /// Get or create the assets with the provided <paramref name="externalIds"/> exist in CDF.
        /// If one or more do not exist, use the <paramref name="buildAssets"/> function to construct
        /// the missing asset objects and upload them to CDF using the chunking of items and throttling
        /// passed as parameters
        /// </summary>
        /// <param name="client">Cognite client</param>
        /// <param name="externalIds">External Ids</param>
        /// <param name="buildAssets">Async function that builds <see cref="AssetCreate"/> objects</param>
        /// <param name="chunkSize">Chunk size</param>
        /// <param name="throttleSize">Throttle size</param>
        /// <param name="token">Cancellation token</param>
        /// <returns></returns>
        public static Task<IEnumerable<Asset>> GetOrCreateAssetsAsync(
            this Client client,
            IEnumerable<string> externalIds,
            Func<IEnumerable<string>, IEnumerable<AssetCreate>> buildAssets,
            int chunkSize,
            int throttleSize,
            CancellationToken token)
        {
            Task<IEnumerable<AssetCreate>> asyncBuildAssets(IEnumerable<string> ids)
            {
                return Task.FromResult(buildAssets(ids));
            }
            return client.GetOrCreateAssetsAsync(externalIds, asyncBuildAssets, chunkSize, throttleSize, token);
        }
        /// <summary>
        /// Get or create the assets with the provided <paramref name="externalIds"/> exist in CDF.
        /// If one or more do not exist, use the <paramref name="buildAssets"/> function to construct
        /// the missing asset objects and upload them to CDF using the chunking of items and throttling
        /// passed as parameters
        /// </summary>
        /// <param name="client">Cognite client</param>
        /// <param name="externalIds">External Ids</param>
        /// <param name="buildAssets">Async function that builds <see cref="AssetCreate"/> objects</param>
        /// <param name="chunkSize">Chunk size</param>
        /// <param name="throttleSize">Throttle size</param>
        /// <param name="token">Cancellation token</param>
        /// <returns></returns>
        public static async Task<IEnumerable<Asset>> GetOrCreateAssetsAsync(
            this Client client,
            IEnumerable<string> externalIds,
            Func<IEnumerable<string>, Task<IEnumerable<AssetCreate>>> buildAssets,
            int chunkSize,
            int throttleSize,
            CancellationToken token)
        {
            var result = new List<Asset>();
            var chunks = externalIds
                .ChunkBy(chunkSize)
                .ToList();
            _logger.LogDebug("Getting or creating assets. Number of external ids: {Number}. Number of chunks: {Chunks}", externalIds.Count(), chunks.Count());
            var generators = chunks
                .Select<IEnumerable<string>, Func<Task>>(
                    chunk => async () => {
                        var existing = await GetOrCreateAssetsChunk(client, chunk, buildAssets, 0, token);
                        result.AddRange(existing);
                    });

            int taskNum = 0;
            await generators.RunThrottled(
                throttleSize,
                (_) => {
                    if (chunks.Count > 1)
                        _logger.LogDebug("{MethodName} completed {NumDone}/{TotalNum} tasks",
                            nameof(GetOrCreateAssetsAsync), ++taskNum, chunks.Count);
                },
                token);
            return result;
        }
        /// <summary>
        /// Ensures that all assets in <paramref name="assets"/> exist in CDF.
        /// Tries to create the assets and returns when all are created or reported as 
        /// duplicates (already exist in CDF)
        /// </summary>
        /// <param name="client">Cognite client</param>
        /// <param name="assets">List of <see cref="AssetCreate"/> objects</param>
        /// <param name="chunkSize">Chunk size</param>
        /// <param name="throttleSize">Throttle size</param>
        /// <param name="token">Cancellation token</param>
        public static async Task EnsureAssetsExistsAsync(
            this Client client,
            IEnumerable<AssetCreate> assets,
            int chunkSize,
            int throttleSize,
            CancellationToken token)
        {
            var chunks = assets
                .ChunkBy(chunkSize);
            _logger.LogDebug("Ensuring assets. Number of assets: {Number}. Number of chunks: {Chunks}", assets.Count(), chunks.Count());
            var generators = chunks
                .Select<IEnumerable<AssetCreate>, Func<Task>>(
                chunk => async () => {
                    await EnsureAssetsChunk(client, chunk, token);
                });

            int taskNum = 0;
            await generators.RunThrottled(
                throttleSize,
                (_) => {
                    if (chunks.Count() > 1)
                        _logger.LogDebug("{MethodName} completed {NumDone}/{TotalNum} tasks",
                            nameof(EnsureAssetsExistsAsync), ++taskNum, chunks.Count());
                },
                token);
        }

        private static async Task<IEnumerable<Asset>> GetOrCreateAssetsChunk(
            Client client,
            IEnumerable<string> externalIds,
            Func<IEnumerable<string>, Task<IEnumerable<AssetCreate>>> buildAssets,
            int backoff,
            CancellationToken token)
        {
            var found = await client.Assets.RetrieveAsync(externalIds.Select(Identity.Create), true, token);
            _logger.LogDebug("Retrieved {Existing} assets from CDF", found.Count());

            var existingAssets = found.ToList();
            var missing = externalIds.Except(existingAssets.Select(asset => asset.ExternalId)).ToList();

            if (!missing.Any())
            {
                return existingAssets;
            }

            _logger.LogDebug("Could not fetch {Missing} out of {Found} assets. Attempting to create the missing ones", missing.Count, externalIds.Count());
            try
            {
                var newAssets = await client.Assets.CreateAsync(await buildAssets(missing), token);
                existingAssets.AddRange(newAssets);
                _logger.LogDebug("Created {New} new assets in CDF", newAssets.Count());
                return existingAssets;
            }
            catch (ResponseException e) when (e.Code == 409 && e.Duplicated.Any())
            {
                if (backoff > 10) // ~3.5 min total backoff time
                {
                    throw;
                }
                _logger.LogDebug("Found {NumDuplicated} duplicates, during the creation of {NumAssets} assets", e.Duplicated.Count(), missing.Count);
            }

            await Task.Delay(TimeSpan.FromSeconds(0.1 * Math.Pow(2, backoff)));
            var ensured = await GetOrCreateAssetsChunk(client, missing, buildAssets, backoff + 1, token);
            existingAssets.AddRange(ensured);

            return existingAssets;
        }

        private static async Task EnsureAssetsChunk(
            Client client,
            IEnumerable<AssetCreate> assets,
            CancellationToken token)
        {
            var create = assets;
            while (!token.IsCancellationRequested && create.Any())
            {
                try
                {
                    var newTs = await client.Assets.CreateAsync(create, token);
                    _logger.LogDebug("Created {New} new assets in CDF", newTs.Count());
                    return;
                }
                catch (ResponseException e) when (e.Code == 409 && e.Duplicated.Any())
                {
                    // Remove duplicates - already exists
                    // also a case for legacyName...
                    var duplicated = new HashSet<string>(e.Duplicated
                        .Select(d => d.GetValue("externalId", null))
                        .Where(mv => mv != null)
                        .Select(mv => mv.ToString()));
                    create = assets.Where(ts => !duplicated.Contains(ts.ExternalId));
                    await Task.Delay(TimeSpan.FromMilliseconds(100), token);
                    _logger.LogDebug("Found {NumDuplicated} duplicates, during the creation of {NumAssets} assets",
                        e.Duplicated.Count(), create.Count());
                    continue;
                }
#pragma warning disable CA1031 // Do not catch general exception types
                catch (Exception e)
                {
                    _logger.LogWarning("CDF create assets failed: {Message} - Retrying in 1 second", e.Message);
                }
#pragma warning restore CA1031 // Do not catch general exception types
                await Task.Delay(TimeSpan.FromSeconds(1), token);
            }
        }
    }
}
