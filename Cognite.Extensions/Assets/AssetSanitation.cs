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
        /// Maximum length of Asset name
        /// </summary>
        public const int AssetNameMax = 140;

        /// <summary>
        /// Maximum length of Asset description
        /// </summary>
        public const int AssetDescriptionMax = 500;

        /// <summary>
        /// Maximum size in bytes of the Asset metadata field
        /// </summary>
        public const int AssetMetadataMaxBytes = 10240;

        /// <summary>
        /// Maximum size in bytes of each Asset metadata key
        /// </summary>
        public const int AssetMetadataMaxPerKey = 128;

        /// <summary>
        /// Maximum size in bytes of each Asset metadata value
        /// </summary>
        public const int AssetMetadataMaxPerValue = 10240;

        /// <summary>
        /// Maximum number of Asset metadata key/value pairs
        /// </summary>
        public const int AssetMetadataMaxPairs = 256;

        /// <summary>
        /// Maximum length of Asset source
        /// </summary>
        public const int AssetSourceMax = 128;

        /// <summary>
        /// Maximum number of Asset labels
        /// </summary>
        public const int AssetLabelsMax = 10;

        /// <summary>
        /// Sanitize an AssetCreate so that it can be safely sent to CDF.
        /// Requests may still fail due to conflicts or missing ids.
        /// </summary>
        /// <param name="asset">Asset to sanitize</param>
        public static void Sanitize(this AssetCreate asset)
        {
            if (asset == null) throw new ArgumentNullException(nameof(asset));
            asset.ExternalId = asset.ExternalId.Truncate(ExternalIdMax);
            asset.Name = asset.Name.Truncate(AssetNameMax);
            if (asset.ParentId < 1) asset.ParentId = null;
            asset.ParentExternalId = asset.ParentExternalId.Truncate(ExternalIdMax);
            asset.Description = asset.Description.Truncate(AssetDescriptionMax);
            if (asset.DataSetId < 1) asset.DataSetId = null;
            asset.Metadata = asset.Metadata?.SanitizeMetadata(
                AssetMetadataMaxPerKey, AssetMetadataMaxPairs, AssetMetadataMaxPerValue, AssetMetadataMaxBytes, out _);
            asset.Source = asset.Source.Truncate(AssetSourceMax);
            asset.Labels = asset.Labels?
                .Where(label => label != null && label.ExternalId != null)
                .Select(label => label.Truncate(ExternalIdMax))
                .Take(10)
                .ToList();
        }

        /// <summary>
        /// Check that given AssetCreate satisfies CDF limits.
        /// </summary>
        /// <param name="asset">Asset to check</param>
        /// <returns>Failed resourceType or null if nothing failed</returns>
        public static ResourceType? Verify(this AssetCreate asset)
        {
            if (asset == null) throw new ArgumentNullException(nameof(asset));
            if (!asset.ExternalId.CheckLength(ExternalIdMax)) return ResourceType.ExternalId;
            if (!asset.Name.CheckLength(AssetNameMax) || asset.Name == null) return ResourceType.Name;
            if (asset.ParentId != null && asset.ParentId < 1) return ResourceType.ParentId;
            if (!asset.ParentExternalId.CheckLength(ExternalIdMax)) return ResourceType.ParentExternalId;
            if (!asset.Description.CheckLength(AssetDescriptionMax)) return ResourceType.Description;
            if (asset.DataSetId != null && asset.DataSetId < 1) return ResourceType.DataSetId;
            if (!asset.Metadata.VerifyMetadata(AssetMetadataMaxPerKey, AssetMetadataMaxPairs,
                AssetMetadataMaxPerValue, AssetMetadataMaxBytes, out _))
                return ResourceType.Metadata;
            if (!asset.Source.CheckLength(AssetSourceMax)) return ResourceType.Source;
            if (asset.Labels != null && (asset.Labels.Count() > AssetLabelsMax || asset.Labels.Any(label => !label.ExternalId.CheckLength(ExternalIdMax))))
                return ResourceType.Labels;
            return null;
        }


        private static readonly DistinctResource<AssetCreate>[] assetDistinct = new[]
        {
            new DistinctResource<AssetCreate>("Duplicate external ids", ResourceType.ExternalId,
                a => a != null ? Identity.Create(a.ExternalId) : null)
        };


        /// <summary>
        /// Clean list of AssetCreate objects, sanitizing each and removing any duplicates.
        /// The first encountered duplicate is kept.
        /// </summary>
        /// <param name="assets">AssetCreate request to clean</param>
        /// <param name="mode">The type of sanitation to apply</param>
        /// <returns>Cleaned create request and an optional error if any ids were duplicated</returns>
        public static (IEnumerable<AssetCreate>, IEnumerable<CogniteError<AssetCreate>>) CleanAssetRequest(
            IEnumerable<AssetCreate> assets,
            SanitationMode mode)
        {
            return CleanRequest(assetDistinct, assets, Verify, Sanitize, mode);
        }
    }
}
