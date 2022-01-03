using Cognite.Extractor.Common;
using CogniteSdk;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Cognite.Extensions
{
    public static partial class Sanitation
    {
        /// <summary>
        /// Sanitize an AssetUpdateItem so that it can be safely sent to CDF.
        /// Requests may still fail due to conflicts or missing ids.
        /// </summary>
        /// <param name="item">Asset update to sanitize</param>
        public static void Sanitize(this AssetUpdateItem item)
        {
            if (item == null) throw new ArgumentNullException(nameof(item));
            if (item.Id == null) item.ExternalId = item.ExternalId.Truncate(ExternalIdMax);

            var update = item.Update;
            update.ExternalId = update.ExternalId.Truncate(ExternalIdMax);
            update.Name = update.Name.Truncate(AssetNameMax);
            update.Description = update.Description.Truncate(AssetDescriptionMax);
            if (update.DataSetId?.Set != null && update.DataSetId.Set < 0) update.DataSetId = null;
            if (update.Metadata != null)
            {
                update.Metadata.Add = update.Metadata.Add.SanitizeMetadata(
                    AssetMetadataMaxPerKey, AssetMetadataMaxPairs, AssetMetadataMaxPerValue, AssetMetadataMaxBytes, out _);
                update.Metadata.Set = update.Metadata.Set.SanitizeMetadata(
                    AssetMetadataMaxPerKey, AssetMetadataMaxPairs, AssetMetadataMaxPerValue, AssetMetadataMaxBytes, out _);
            }
            update.Source = update.Source.Truncate(AssetSourceMax);
            if (update.ParentId?.Set != null && update.ParentId.Set < 0) update.ParentId = null;
            update.ParentExternalId = update.ParentExternalId.Truncate(ExternalIdMax);
            if (update.Labels != null)
            {
                update.Labels.Add = update.Labels.Add?
                    .Where(label => label != null && label.ExternalId != null)
                    .Select(label => label.Truncate(ExternalIdMax))
                    .Take(10)
                    .ToList();
                update.Labels.Set = update.Labels.Set?
                    .Where(label => label != null && label.ExternalId != null)
                    .Select(label => label.Truncate(ExternalIdMax))
                    .Take(10)
                    .ToList();
            }
        }

        /// <summary>
        /// Check that given AssetUpdateItem satisfies CDF limits.
        /// </summary>
        /// <param name="item">Asset to check</param>
        /// <returns>Failed resourceType or null if nothing failed</returns>
        public static ResourceType? Verify(this AssetUpdateItem item)
        {
            if (item == null) throw new ArgumentNullException(nameof(item));
            if (!item.ExternalId.CheckLength(ExternalIdMax)) return ResourceType.ExternalId;
            if (item.Id != null && item.Id < 1) return ResourceType.Id;
            if ((item.Id == null) == (item.ExternalId == null)) return ResourceType.Id;

            var update = item.Update;
            if (update.DataSetId == null && update.Description == null && update.ExternalId == null
                && update.Labels == null && update.Metadata == null && update.Name == null
                && update.ParentExternalId == null && update.ParentId == null && update.Source == null)
                return ResourceType.Update;

            if (!update.ExternalId?.Set?.CheckLength(ExternalIdMax) ?? false) return ResourceType.ExternalId;
            if (update.Name != null && (update.Name.Set == null || !update.Name.Set.CheckLength(AssetNameMax))) return ResourceType.Name;
            if (!update.Description?.Set?.CheckLength(AssetDescriptionMax) ?? false) return ResourceType.Description;
            if (update.DataSetId?.Set != null && update.DataSetId.Set < 0) return ResourceType.DataSetId;
            if (update.Metadata != null)
            {
                if (!update.Metadata.Set.VerifyMetadata(
                    AssetMetadataMaxPerKey, AssetMetadataMaxPairs, AssetMetadataMaxPerValue, AssetMetadataMaxBytes, out _))
                    return ResourceType.Metadata;
                if (!update.Metadata.Add.VerifyMetadata(
                    AssetMetadataMaxPerKey, AssetMetadataMaxPairs, AssetMetadataMaxPerValue, AssetMetadataMaxBytes, out _))
                    return ResourceType.Metadata;
            }
            if (!update.Source?.Set?.CheckLength(AssetSourceMax) ?? false) return ResourceType.Source;
            if (update.ParentId?.Set != null && update.ParentId.Set < 0) return ResourceType.ParentId;
            if (update.ParentId?.Set != null && update.ParentExternalId?.Set != null) return ResourceType.ParentId;
            if (!update.ParentExternalId?.Set?.CheckLength(ExternalIdMax) ?? false) return ResourceType.ParentExternalId;
            if (update.Labels?.Add != null && (update.Labels.Add.Count() > AssetLabelsMax
                || update.Labels.Add.Any(label => !label.ExternalId.CheckLength(ExternalIdMax))))
                return ResourceType.Labels;
            if (update.Labels?.Set != null && (update.Labels.Set.Count() > AssetLabelsMax
                || update.Labels.Set.Any(label => !label.ExternalId.CheckLength(ExternalIdMax))))
                return ResourceType.Labels;

            return null;
        }



        private static readonly DistinctResource<AssetUpdateItem>[] assetUpdateDistinct = new[] {
            new DistinctResource<AssetUpdateItem>("Duplicated ids", ResourceType.Id, a => a)
        };

        /// <summary>
        /// Clean list of AssetUpdateItem objects, sanitizing each and removing any duplicates.
        /// The first encountered duplicate is kept.
        /// </summary>
        /// <param name="items">AssetUpdate request to clean</param>
        /// <param name="mode">The type of sanitation to apply</param>
        /// <returns>Cleaned update request and a list of errors</returns>
        public static (IEnumerable<AssetUpdateItem>, IEnumerable<CogniteError<AssetUpdateItem>>) CleanAssetUpdateRequest(
            IEnumerable<AssetUpdateItem> items,
            SanitationMode mode)
        {
            return CleanRequest(assetUpdateDistinct, items, Verify, Sanitize, mode);
        }
    }
}
