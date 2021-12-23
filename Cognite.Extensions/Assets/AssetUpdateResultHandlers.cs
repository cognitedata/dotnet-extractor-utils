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
        private static void ParseAssetUpdateException(ResponseException ex, CogniteError err)
        {
            if (ex.Missing?.Any() ?? false)
            {
                err.Type = ErrorType.ItemMissing;
                err.Values = ex.Missing.Select(dict =>
                        (dict["externalId"] as MultiValue.String)?.Value)
                        .Where(id => id != null)
                        .Select(Identity.Create);
                if (ex.Message.StartsWith("Label ids not found", StringComparison.InvariantCulture))
                {
                    err.Resource = ResourceType.Labels;
                }
                else
                {
                    // Can also be ParentExternalId, actually, same error message so no way to check.
                    err.Resource = ResourceType.ExternalId;
                }
            }
            else if (ex.Duplicated?.Any() ?? false)
            {
                err.Type = ErrorType.ItemExists;
                err.Resource = ResourceType.ExternalId;
                err.Values = ex.Duplicated.Select(dict =>
                    (dict["externalId"] as MultiValue.String)?.Value)
                    .Where(id => id != null)
                    .Select(Identity.Create);
            }
            else if (ex.Code == 400)
            {
                if (ex.Message.StartsWith("Changing from/to being root", StringComparison.InvariantCulture)
                    || ex.Message.StartsWith("Asset must stay within same asset hierarchy", StringComparison.InvariantCulture))
                {
                    err.Complete = false;
                    err.Type = ErrorType.IllegalItem;
                    err.Resource = ResourceType.ParentId;
                }
                else if (ex.Message.StartsWith("Bad parent", StringComparison.InvariantCulture))
                {
                    err.Complete = false;
                    err.Type = ErrorType.ItemMissing;
                    err.Resource = ResourceType.ParentId;
                }
                else if (ex.Message.StartsWith("Invalid dataSetIds", StringComparison.InvariantCulture))
                {
                    var idString = ex.Message.Replace("Invalid dataSetIds: ", "");
                    err.Type = ErrorType.ItemMissing;
                    err.Resource = ResourceType.DataSetId;
                    err.Values = ParseIdString(idString);
                }
                else if (ex.Message.StartsWith("Asset ids not found", StringComparison.InvariantCulture))
                {
                    var idString = ex.Message.Replace("Asset ids not found: ", "");
                    err.Type = ErrorType.ItemMissing;
                    err.Resource = ResourceType.Id;
                    err.Values = ParseIdString(idString);
                }
            }
        }

        /// <summary>
        /// Clean list of AssetCreate objects based on CogniteError object
        /// </summary>
        /// <param name="error">Error that occured with a previous push</param>
        /// <param name="items">Assets to clean</param>
        /// <returns>Assets that are not affected by the error</returns>
        public static IEnumerable<AssetUpdateItem> CleanFromError(
            CogniteError<AssetUpdateItem> error,
            IEnumerable<AssetUpdateItem> items)
        {
            if (items == null)
            {
                throw new ArgumentNullException(nameof(items));
            }
            if (error == null)
            {
                return items;
            }

            if (!error.Values?.Any() ?? true)
            {
                error.Values = items.Select(update => update.ExternalId != null
                    ? Identity.Create(update.ExternalId)
                    : Identity.Create(update.Id!.Value));
                return Array.Empty<AssetUpdateItem>();
            }

            var badValues = new HashSet<Identity>(error.Values);

            var ret = new List<AssetUpdateItem>();
            var skipped = new List<AssetUpdateItem>();

            foreach (var item in items)
            {
                bool added = false;
                var update = item.Update;

                switch (error.Resource)
                {
                    case ResourceType.Id:
                        if (!item.Id.HasValue || !badValues.Contains(Identity.Create(item.Id.Value))) added = true;
                        break;
                    case ResourceType.DataSetId:
                        if (update.DataSetId?.Set == null || !badValues.Contains(Identity.Create(update.DataSetId.Set.Value))) added = true;
                        break;
                    case ResourceType.ExternalId:
                        if (error.Type == ErrorType.ItemMissing)
                        {
                            if ((update.ExternalId?.Set == null || !badValues.Contains(Identity.Create(update.ExternalId.Set)))
                                && (item.ExternalId == null || !badValues.Contains(Identity.Create(item.ExternalId)))
                                && (update.ParentExternalId?.Set == null || !badValues.Contains(Identity.Create(update.ParentExternalId.Set))))
                                    added = true;
                        }
                        else if (error.Type == ErrorType.ItemExists)
                        {
                            if (update.ExternalId?.Set == null || !badValues.Contains(Identity.Create(update.ExternalId.Set)))
                                added = true;
                        }
                        break;
                    case ResourceType.ParentId:
                        if (error.Type == ErrorType.IllegalItem)
                        {
                            if ((item.ExternalId == null || !badValues.Contains(Identity.Create(item.ExternalId)))
                                && (item.Id == null || !badValues.Contains(Identity.Create(item.Id.Value)))) added = true;
                        }
                        else
                        {
                            if ((update.ParentId?.Set == null || !badValues.Contains(Identity.Create(update.ParentId.Set.Value)))
                            && (update.ParentExternalId?.Set == null || !badValues.Contains(Identity.Create(update.ParentExternalId.Set))))
                                added = true;
                        }
                        break;
                    case ResourceType.Labels:
                        if (update.Labels?.Add == null
                            || !update.Labels.Add.Any(label => badValues.Contains(Identity.Create(label.ExternalId)))) added = true;
                        break;
                }

                if (added)
                {
                    ret.Add(item);
                }
                else
                {
                    CdfMetrics.AssetUpdatesSkipped.Inc();
                    skipped.Add(item);
                }
            }

            if (skipped.Any())
            {
                error.Skipped = skipped;
            }
            else
            {
                error.Skipped = skipped;
                return Array.Empty<AssetUpdateItem>();
            }
            return ret;
        }

        /// <summary>
        /// Ensure that the assets have legal parents, and are not being moved
        /// between root hierarchies.
        /// </summary>
        /// <param name="resource">CogniteSdk assets resource</param>
        /// <param name="items">Asset update items to check</param>
        /// <param name="assetChunkSize">Chunk size for reading assets</param>
        /// <param name="assetThrottleSize">Throttle size for reading assets</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>A list of errors if anything failed</returns>
        public static async Task<IEnumerable<CogniteError<AssetUpdateItem>>> VerifyAssetUpdateParents(
            AssetsResource resource,
            IEnumerable<AssetUpdateItem> items,
            int assetChunkSize,
            int assetThrottleSize,
            CancellationToken token)
        {
            if (items == null || !items.Any()) return Enumerable.Empty<CogniteError<AssetUpdateItem>>();
            var assetsToFetch = new HashSet<Identity>();

            foreach (var item in items)
            {
                bool hasParent = false;
                if (item.Update.ParentId?.Set != null)
                {
                    assetsToFetch.Add(Identity.Create(item.Update.ParentId.Set.Value));
                    hasParent = true;
                }
                else if (item.Update.ParentExternalId?.Set != null)
                {
                    assetsToFetch.Add(Identity.Create(item.Update.ParentExternalId.Set));
                    hasParent = true;
                }

                
                if (hasParent)
                {
                    var idt = item.ExternalId != null ? Identity.Create(item.ExternalId) : Identity.Create(item.Id!.Value);
                    if (idt.ExternalId != null && (item.Update.ExternalId?.SetNull ?? false)) idt = null;
                    if (item.Update.ExternalId?.Set != null)
                    {
                        if (idt == null || idt.Id == null) idt = Identity.Create(item.Update.ExternalId.Set);
                    }
                    if (idt != null) assetsToFetch.Add(idt);
                }
            }

            IEnumerable<Asset> assets;
            try
            {
                assets = await resource
                    .GetAssetsByIdsIgnoreErrors(assetsToFetch, assetChunkSize, assetThrottleSize, token)
                    .ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                var err = ParseSimpleError(ex, assetsToFetch, items);
                return new[] { err };
            }
            
            var byId = assets.ToDictionary(asset => asset.Id);
            var byExtId = assets.Where(asset => asset.ExternalId != null).ToDictionary(asset => asset.ExternalId);

            var missingParents = new List<(Identity parent, AssetUpdateItem item)>();
            var rootAssetChange = new List<AssetUpdateItem>();

            foreach (var item in items)
            {
                Asset self;
                if (item.Id.HasValue) self = byId[item.Id.Value];
                else self = byExtId[item.ExternalId];

                Asset? parent = null;

                if (item.Update.ParentId?.Set != null && !byId.TryGetValue(item.Update.ParentId.Set.Value, out parent))
                {
                    missingParents.Add((Identity.Create(item.Update.ParentId.Set.Value), item));
                    continue;
                }
                else if (item.Update.ParentExternalId?.Set != null
                    && !byExtId.TryGetValue(item.Update.ParentExternalId.Set, out parent))
                {
                    missingParents.Add((Identity.Create(item.Update.ParentExternalId.Set), item));
                    continue;
                }
                
                // No parentId is set
                if (parent == null) continue;

                // RootId is not allowed to change
                if (self.RootId != parent.RootId)
                {
                    rootAssetChange.Add(item);
                }
            }

            var errors = new List<CogniteError<AssetUpdateItem>>();
            if (missingParents.Any())
            {
                errors.Add(new CogniteError<AssetUpdateItem>
                {
                    Message = "Missing asset parents",
                    Resource = ResourceType.ParentId,
                    Type = ErrorType.ItemMissing,
                    Skipped = missingParents.Select(pair => pair.item),
                    Values = missingParents.Select(pair => pair.parent)
                });
            }
            if (rootAssetChange.Any())
            {
                errors.Add(new CogniteError<AssetUpdateItem>
                {
                    Message = "Changing from/to being root is not allowed",
                    Resource = ResourceType.ParentId,
                    Type = ErrorType.IllegalItem,
                    Skipped = rootAssetChange,
                    Values = rootAssetChange.Select(item => item.Id.HasValue
                        ? Identity.Create(item.Id.Value) : Identity.Create(item.ExternalId))
                });
            }

            return errors;
        }
    }
}
