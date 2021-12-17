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

        /// <summary>
        /// Clean list of AssetCreate objects based on CogniteError object
        /// </summary>
        /// <param name="resource">CogniteSdk assets resource</param>
        /// <param name="error">Error that occured with a previous push</param>
        /// <param name="assets">Assets to clean</param>
        /// <param name="assetChunkSize">Maximum number of ids per asset read</param>
        /// <param name="assetThrottleSize">Maximum number of parallel asset read requests</param>
        /// <param name="token"></param>
        /// <returns>Assets that are not affected by the error</returns>
        public static async Task<IEnumerable<AssetCreate>> CleanFromError(
            AssetsResource resource,
            CogniteError<AssetCreate> error,
            IEnumerable<AssetCreate> assets,
            int assetChunkSize,
            int assetThrottleSize,
            CancellationToken token)
        {
            if (assets == null)
            {
                throw new ArgumentNullException(nameof(assets));
            }
            if (error == null)
            {
                return assets;
            }
            // This is mostly to avoid infinite loops. If there are no bad values then
            // there is no way to correctly clean the request, so there must be something
            // else wrong
            if (!error.Values?.Any() ?? true)
            {
                error.Values = assets.Where(asset => asset.ExternalId != null).Select(asset => Identity.Create(asset.ExternalId));
                return Array.Empty<AssetCreate>();
            }

            if (!error.Complete)
            {
                await CompleteError(resource, error, assets, assetChunkSize, assetThrottleSize, token).ConfigureAwait(false);
            }

            // If we failed to complete the error
            // TODO: Improve this
            if (!error.Complete) return Enumerable.Empty<AssetCreate>();

            var items = new HashSet<Identity>(error.Values);

            var ret = new List<AssetCreate>();
            var skipped = new List<AssetCreate>();

            foreach (var asset in assets)
            {
                bool added = false;
                switch (error.Resource)
                {
                    case ResourceType.DataSetId:
                        if (!asset.DataSetId.HasValue || !items.Contains(Identity.Create(asset.DataSetId.Value))) added = true;
                        break;
                    case ResourceType.ExternalId:
                        if (asset.ExternalId == null || !items.Contains(Identity.Create(asset.ExternalId))) added = true;
                        break;
                    case ResourceType.ParentExternalId:
                        if (asset.ParentExternalId == null || !items.Contains(Identity.Create(asset.ParentExternalId))) added = true;
                        break;
                    case ResourceType.ParentId:
                        if (!asset.ParentId.HasValue || !items.Contains(Identity.Create(asset.ParentId.Value))) added = true;
                        break;
                    case ResourceType.Labels:
                        if (asset.Labels == null || !asset.Labels.Any(label => items.Contains(Identity.Create(label.ExternalId)))) added = true;
                        break;
                }
                if (added)
                {
                    ret.Add(asset);
                }
                else
                {
                    CdfMetrics.AssetsSkipped.Inc();
                    skipped.Add(asset);
                }
            }
            if (skipped.Any())
            {
                error.Skipped = skipped;
            }
            else
            {
                error.Skipped = assets;
                return Array.Empty<AssetCreate>();
            }
            return ret;
        }
        private static async Task CompleteError(
            AssetsResource resource,
            CogniteError error,
            IEnumerable<AssetCreate> assets,
            int assetChunkSize,
            int assetThrottleSize,
            CancellationToken token)
        {
            if (error.Complete) return;

            if (error.Resource == ResourceType.ParentExternalId)
            {
                var ids = assets.Select(asset => asset.ParentExternalId)
                    .Where(id => id != null)
                    .Distinct()
                    .Select(Identity.Create)
                    .Except(error.Values);

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
                        .Concat(error.Values)
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
