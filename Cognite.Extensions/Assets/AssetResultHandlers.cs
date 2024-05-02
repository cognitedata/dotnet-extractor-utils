using CogniteSdk;
using CogniteSdk.Resources;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.Extensions
{
    public static partial class ResultHandlers
    {
        private static void ParseAssetException(ResponseException ex, CogniteError err)
        {
            if (ex.Missing?.Any() ?? false)
            {
                err.Type = ErrorType.ItemMissing;
                err.Resource = ResourceType.Labels;
                err.Values = ex.Missing.Select(dict =>
                    (dict["externalId"] as MultiValue.String)?.Value)
                    .Where(id => id != null)
                    .Select(Identity.Create);
            }
            else if (ex.Duplicated?.Any() ?? false)
            {
                // Only externalIds may be duplicated when creating assets
                err.Type = ErrorType.ItemExists;
                err.Resource = ResourceType.ExternalId;
                err.Values = ex.Duplicated.Select(dict =>
                    (dict["externalId"] as MultiValue.String)?.Value)
                    .Where(id => id != null)
                    .Select(Identity.Create);
            }
            else if (ex.Code == 400)
            {
                if (ex.Message.StartsWith("Reference to unknown parent with externalId", StringComparison.InvariantCulture))
                {
                    // Missing parentExternalId only returns one value for some reason.
                    var missingId = ex.Message.Replace("Reference to unknown parent with externalId ", "");
                    err.Complete = false;
                    err.Type = ErrorType.ItemMissing;
                    err.Resource = ResourceType.ParentExternalId;
                    err.Values = new[] { Identity.Create(missingId) };
                }
                else if (ex.Message.StartsWith("The given parent ids do not exist", StringComparison.InvariantCulture))
                {
                    var idString = ex.Message.Replace("The given parent ids do not exist: ", "");
                    err.Type = ErrorType.ItemMissing;
                    err.Resource = ResourceType.ParentId;
                    err.Values = ParseIdString(idString);
                }
                else if (ex.Message.StartsWith("Invalid dataSetIds", StringComparison.InvariantCulture))
                {
                    var idString = ex.Message.Replace("Invalid dataSetIds: ", "");
                    err.Type = ErrorType.ItemMissing;
                    err.Resource = ResourceType.DataSetId;
                    err.Values = ParseIdString(idString);
                }
            }
        }

        private static bool IsAffected(AssetCreate asset, HashSet<Identity> badValues, CogniteError<AssetCreate> error)
        {
            return error.Resource switch
            {
                ResourceType.DataSetId => badValues.ContainsIdentity(asset.DataSetId),
                ResourceType.ExternalId => badValues.ContainsIdentity(asset.ExternalId),
                ResourceType.ParentExternalId => badValues.ContainsIdentity(asset.ParentExternalId),
                ResourceType.ParentId => badValues.ContainsIdentity(asset.ParentId),
                ResourceType.Labels => asset.Labels != null && asset.Labels.Any(l => badValues.ContainsIdentity(l.ExternalId)),
                _ => false
            };
        }

        /// <summary>
        /// Clean list of AssetCreate objects based on CogniteError object
        /// </summary>
        /// <param name="error">Error that occured with a previous push</param>
        /// <param name="assets">Assets to clean</param>
        /// <returns>Assets that are not affected by the error</returns>
        public static IEnumerable<AssetCreate> CleanFromError(
            CogniteError<AssetCreate> error,
            IEnumerable<AssetCreate> assets)
        {
            return CleanFromErrorCommon(error, assets, IsAffected,
                asset => asset.ExternalId == null ? null : Identity.Create(asset.ExternalId),
                CdfMetrics.AssetsSkipped);
        }
        /// <summary>
        /// Fetch missing parents for an asset create that failed due to missing parent external ids.
        /// </summary>
        /// <param name="resource">CogniteSdk assets resource</param>
        /// <param name="error">Incomplete error</param>
        /// <param name="assets">List of assets to check</param>
        /// <param name="assetChunkSize">Chunk size for reading assets</param>
        /// <param name="assetThrottleSize">Throttle size for reading assets</param>
        /// <param name="token">Cancellation token</param>
        public static async Task CompleteAssetError(
            AssetsResource resource,
            CogniteError error,
            IEnumerable<AssetCreate> assets,
            int assetChunkSize,
            int assetThrottleSize,
            CancellationToken token)
        {
            if (error == null || error.Complete) return;

            if (error.Resource == ResourceType.ParentExternalId)
            {
                // Retrieve all parents, unless they are already checked (i.e. retrieved from the initial request),
                // or if they are included in the request itself.
                var ids = assets.Select(asset => asset.ParentExternalId)
                    .Where(id => id != null)
                    .Distinct()
                    .Select(Identity.Create)
                    .Except(error.Values ?? Enumerable.Empty<Identity>())
                    .Except(assets
                        .Where(asset => asset.ExternalId != null)
                        .Select(asset => Identity.Create(asset.ExternalId))
                    );

                if (!ids.Any())
                {
                    error.Complete = true;
                    return;
                }

                try
                {
                    var parents = await resource
                        .GetAssetsByIdsIgnoreErrors(ids, assetChunkSize, assetThrottleSize, token)
                        .ConfigureAwait(false);

                    error.Complete = true;
                    error.Values = ids
                        .Except(parents.Select(asset => Identity.Create(asset.ExternalId)))
                        .Concat(error.Values ?? Enumerable.Empty<Identity>())
                        .Distinct();
                }
                catch
                {
                    return;
                }
            }
        }
    }
}
