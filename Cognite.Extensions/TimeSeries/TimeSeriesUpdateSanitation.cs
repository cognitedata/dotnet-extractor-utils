﻿using CogniteSdk;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Cognite.Extensions
{
    public partial class Sanitation
    {
        /// <summary>
        /// Sanitize a TimeSeriesUpdateItem object so that it can be safely sent to CDF.
        /// Requests may still fail due to conflicts or missing ids.
        /// </summary>
        /// <param name="item">TimeSeries update to sanitize</param>
        public static void Sanitize(this TimeSeriesUpdateItem item)
        {
            if (item == null) throw new ArgumentNullException(nameof(item));
            if (item.Id == null) item.ExternalId = item.ExternalId.Truncate(ExternalIdMax);

            var update = item.Update;
            update.ExternalId = update.ExternalId.Truncate(ExternalIdMax);
            update.Name = update.Name.Truncate(TimeSeriesNameMax);
            if (update.AssetId?.Set != null && update.AssetId.Set < 1) update.AssetId = null;
            update.Description = update.Description.Truncate(TimeSeriesDescriptionMax);
            if (update.DataSetId?.Set != null && update.DataSetId.Set < 1) update.DataSetId = null;
            if (update.Metadata != null)
            {
                update.Metadata.Add = update.Metadata.Add.SanitizeMetadata(
                    TimeSeriesMetadataMaxPerKey, TimeSeriesMetadataMaxPairs,
                    TimeSeriesMetadataMaxPerValue, TimeSeriesMetadataMaxBytes, out _);
                update.Metadata.Set = update.Metadata.Set.SanitizeMetadata(
                    TimeSeriesMetadataMaxPerKey, TimeSeriesMetadataMaxPairs,
                    TimeSeriesMetadataMaxPerValue, TimeSeriesMetadataMaxBytes, out _);
            }
            update.Unit = update.Unit.Truncate(TimeSeriesUnitMax);
        }

        /// <summary>
        /// Check that given TimeSeriesUpdateItem satisfies CDF limits.
        /// Update may fail if new size of timeseries metadata exceeds limit for total size.
        /// </summary>
        /// <param name="item">Timeseries update to check</param>
        /// <returns>True if timeseries update satisfies limits</returns>
        public static ResourceType? Verify(this TimeSeriesUpdateItem item)
        {
            if (item == null) throw new ArgumentNullException(nameof(item));
            if (!item.ExternalId.CheckLength(ExternalIdMax)) return ResourceType.ExternalId;
            if (item.Id != null && item.Id < 1) return ResourceType.Id;
            if ((item.Id == null) == (item.ExternalId == null)) return ResourceType.Id;

            var update = item.Update;

            if (update.DataSetId == null && update.Description == null && update.ExternalId == null
                && update.Metadata == null && update.Name == null && update.AssetId == null && update.Unit == null
                && update.SecurityCategories == null)
                return ResourceType.Update;

            if (!update.ExternalId?.Set?.CheckLength(ExternalIdMax) ?? false) return ResourceType.ExternalId;
            if (!update.Name?.Set?.CheckLength(TimeSeriesNameMax) ?? false) return ResourceType.Name;
            if (update.AssetId?.Set != null && update.AssetId.Set < 1) return ResourceType.AssetId;
            if (!update.Description?.Set?.CheckLength(TimeSeriesDescriptionMax) ?? false) return ResourceType.Description;
            if (update.DataSetId?.Set != null && update.DataSetId.Set < 1) return ResourceType.DataSetId;
            if (update.Metadata != null)
            {
                if (!update.Metadata.Set.VerifyMetadata(
                    TimeSeriesMetadataMaxPerKey, TimeSeriesMetadataMaxPairs,
                    TimeSeriesMetadataMaxPerValue, TimeSeriesMetadataMaxBytes, out _))
                    return ResourceType.Metadata;
                if (!update.Metadata.Add.VerifyMetadata(
                    TimeSeriesMetadataMaxPerKey, TimeSeriesMetadataMaxPairs,
                    TimeSeriesMetadataMaxPerValue, TimeSeriesMetadataMaxBytes, out _))
                    return ResourceType.Metadata;
            }
            if (!update.Unit?.Set?.CheckLength(TimeSeriesUnitMax) ?? false) return ResourceType.Unit;

            return null;
        }

        private static readonly DistinctResource<TimeSeriesUpdateItem>[] timeSeriesUpdateDistinct = new[] {
            new DistinctResource<TimeSeriesUpdateItem>("Duplicated ids", ResourceType.Id, ts => ts)
        };

        /// <summary>
        /// Clean list of TimeSeriesUpdateItem objects, sanitizing each and removing any duplicates.
        /// The first encountered duplicate is kept.
        /// </summary>
        /// <param name="items">TimeSeriesUpdate request to clean</param>
        /// <param name="mode">The type of sanitation to apply</param>
        /// <returns>Cleaned update request and a list of errors</returns>
        public static (IEnumerable<TimeSeriesUpdateItem>, IEnumerable<CogniteError<TimeSeriesUpdateItem>>) CleanTimeSeriesUpdateRequest(
            IEnumerable<TimeSeriesUpdateItem> items,
            SanitationMode mode)
        {
            return CleanRequest(timeSeriesUpdateDistinct, items, Verify, Sanitize, mode);
        }
    }
}
