using CogniteSdk;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Cognite.Extensions
{
    /// <summary>
    /// Extensions to CogniteSdk types
    /// </summary>
    public static class TypeExtensions
    {
        private static bool AnyDiff<T>(IEnumerable<T>? newElems, IEnumerable<T>? oldElems, bool replace)
        {
            bool diff = true;
            if (newElems != null && oldElems != null)
            {
                diff = newElems.Except(oldElems).Any()
                    || replace && oldElems.Except(newElems).Any();
            }
            else if (newElems == null && !replace)
            {
                diff = false;
            }

            return diff;
        }


        private static UpdateLabels<IEnumerable<CogniteExternalId>>? GetLabelUpdate(
            IEnumerable<CogniteExternalId>? newLabels,
            IEnumerable<CogniteExternalId>? oldLabels,
            bool replace)
        {
            bool labelDiff = AnyDiff(newLabels?.Select(l => l.ExternalId), oldLabels?.Select(l => l.ExternalId), replace);

            if (!labelDiff) return null;

            if (replace)
            {
                return new UpdateLabels<IEnumerable<CogniteExternalId>>
                {
                    Set = newLabels ?? Enumerable.Empty<CogniteExternalId>()
                };
            }
            
            return new UpdateLabels<IEnumerable<CogniteExternalId>>(newLabels ?? Enumerable.Empty<CogniteExternalId>());
        }

        private static UpdateDictionary<string>? GetMetadataUpdate(
            Dictionary<string, string> newMeta,
            Dictionary<string, string> oldMeta,
            bool replace)
        {
            bool metaDiff = AnyDiff(newMeta, oldMeta, replace);

            if (!metaDiff) return null;

            if (replace)
            {
                return new UpdateDictionary<string>(newMeta ?? new Dictionary<string, string>());
            }

            return new UpdateDictionary<string>(newMeta ?? new Dictionary<string, string>(), Enumerable.Empty<string>());
        }

        /// <summary>
        /// Build an update from the diff between <paramref name="asset"/> and <paramref name="old"/>,
        /// where <paramref name="old"/> is retrieved from CDF.
        /// </summary>
        /// <param name="asset">New asset that could not be created during upsert</param>
        /// <param name="old">Old asset retrieved from CDF</param>
        /// <param name="opt">Options for how fields should be replaced</param>
        /// <returns>Update item</returns>
        /// <exception cref="ArgumentNullException">If asset or old are null</exception>
        public static AssetUpdateItem? ToUpdate(this AssetCreate asset, Asset old, UpsertParams? opt)
        {
            if (asset == null) throw new ArgumentNullException(nameof(asset));
            if (old == null) throw new ArgumentNullException(nameof(old));
            if (opt == null) opt = new UpsertParams();

            var upd = new AssetUpdate();

            if (asset.DataSetId != old.DataSetId && (asset.DataSetId != null || opt.SetNull))
                upd.DataSetId = new UpdateNullable<long?>(asset.DataSetId);
            if (asset.Description != old.Description && (asset.Description != null || opt.SetNull))
                upd.Description = new UpdateNullable<string?>(asset.Description);
            if (asset.ExternalId != old.ExternalId && (asset.ExternalId != null || opt.SetNull))
                upd.ExternalId = new UpdateNullable<string?>(asset.ExternalId);
            upd.Labels = GetLabelUpdate(asset.Labels, old.Labels, opt.ReplaceLabels);
            upd.Metadata = GetMetadataUpdate(asset.Metadata, old.Metadata, opt.ReplaceMetadata);

            if (asset.Name != old.Name && asset.Name != null)
                upd.Name = new Update<string>(asset.Name);
            if (asset.ParentExternalId != old.ParentExternalId && asset.ParentExternalId != null)
                upd.ParentExternalId = new Update<string?>(asset.ParentExternalId);
            if (asset.ParentId != old.ParentId && asset.ParentId != null)
                upd.ParentId = new Update<long?>(asset.ParentId);
            if (asset.Source != old.Source && (asset.Source != null || opt.SetNull))
                upd.Source = new UpdateNullable<string?>(asset.Source);


            if (upd.DataSetId == null && upd.Description == null && upd.ExternalId == null
                && upd.Labels == null && upd.Metadata == null && upd.Name == null
                && upd.ParentExternalId == null && upd.ParentId == null && upd.Source == null) return null;

            return new AssetUpdateItem(old.Id)
            {
                Update = upd
            };
        }


        /// <summary>
        /// Build an update from the diff between <paramref name="ts"/> and <paramref name="old"/>,
        /// where <paramref name="old"/> is retrieved from CDF.
        /// </summary>
        /// <param name="ts">New timeseries that could not be created during upsert</param>
        /// <param name="old">Old timeseries retrieved from CDF</param>
        /// <param name="opt">Options for how fields should be replaced</param>
        /// <returns>Update item</returns>
        /// <exception cref="ArgumentNullException">If ts or old are null</exception>
        public static TimeSeriesUpdateItem? ToUpdate(this TimeSeriesCreate ts, TimeSeries old, UpsertParams? opt)
        {
            if (ts == null) throw new ArgumentNullException(nameof(ts));
            if (old == null) throw new ArgumentNullException(nameof(old));
            if (opt == null) opt = new UpsertParams();

            var upd = new TimeSeriesUpdate();

            if (ts.AssetId != old.AssetId && (ts.AssetId != null || opt.SetNull))
                upd.AssetId = new UpdateNullable<long?>(ts.AssetId);
            if (ts.DataSetId != old.DataSetId && (ts.DataSetId != null || opt.SetNull))
                upd.DataSetId = new UpdateNullable<long?>(ts.DataSetId);
            if (ts.Description != old.Description && (ts.Description != null || opt.SetNull))
                upd.Description = new UpdateNullable<string?>(ts.Description);
            if (ts.ExternalId != old.ExternalId && (ts.ExternalId != null || opt.SetNull))
                upd.ExternalId = new UpdateNullable<string?>(ts.ExternalId);

            upd.Metadata = GetMetadataUpdate(ts.Metadata, old.Metadata, opt.ReplaceMetadata);

            if (ts.Name != old.Name && (ts.Name != null || opt.SetNull))
                upd.Name = new UpdateNullable<string?>(ts.Name);

            bool secCatDiff = AnyDiff(ts.SecurityCategories, old.SecurityCategories, opt.ReplaceLabels);
            if (secCatDiff)
            {
                upd.SecurityCategories = opt.ReplaceSecurityCategories
                    ? new UpdateEnumerable<long?>(ts.SecurityCategories.Select(s => (long?)s) ?? Enumerable.Empty<long?>())
                    : new UpdateEnumerable<long?>(ts.SecurityCategories.Select(s => (long?)s) ?? Enumerable.Empty<long?>(),
                        Enumerable.Empty<long?>());
            }

            if (ts.Unit != old.Unit && (ts.Unit != null || opt.SetNull))
                upd.Unit = new UpdateNullable<string?>(ts.Unit);

            if (upd.AssetId == null && upd.DataSetId == null && upd.Description == null
                && upd.ExternalId == null && upd.Metadata == null && upd.Name == null
                && upd.SecurityCategories == null && upd.Unit == null) return null;

            return new TimeSeriesUpdateItem(old.Id) { Update = upd };
        }
    }

    /// <summary>
    /// Settings for upsert
    /// </summary>
    public class UpsertParams
    {
        /// <summary>
        /// Whether to use "Add" or "Set" when creating metadata.
        /// If true, metadata is replaced entirely. Otherwise only the provided fields are replaced.
        /// </summary>
        public bool ReplaceMetadata { get; set; }

        /// <summary>
        /// Whether to use "Add" or "Set" when adding labels.
        /// If true, labels are replaced entirely. Otherwise new labels are only added if they do not exist.
        /// If true and no labels are provided, all are removed.
        /// </summary>
        public bool ReplaceLabels { get; set; }

        /// <summary>
        /// Whether to use "Add" or "Set" when 
        /// </summary>
        public bool ReplaceSecurityCategories { get; set; }
        /// <summary>
        /// Whether to set fields to null if they are not defined.
        /// Default is true.
        /// </summary>
        public bool SetNull { get; set; } = true;
    }
}
