using CogniteSdk;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Cognite.Extensions
{
    public static partial class Sanitation
    {
        /// <summary>
        /// Maximum length of Timeseries name
        /// </summary>
        public const int TimeSeriesNameMax = 255;

        /// <summary>
        /// Maximum length of Timeseries description
        /// </summary>
        public const int TimeSeriesDescriptionMax = 1000;

        /// <summary>
        /// Maximum length of Timeseries unit
        /// </summary>
        public const int TimeSeriesUnitMax = 32;

        /// <summary>
        /// Maximum size in bytes of each Timeseries metadata key
        /// </summary>
        public const int TimeSeriesMetadataMaxPerKey = 128;

        /// <summary>
        /// Maximum size in bytes of each Timeseries metadata value
        /// </summary>
        public const int TimeSeriesMetadataMaxPerValue = 10000;

        /// <summary>
        /// Maximum size in bytes of Timeseries metadata field
        /// </summary>
        public const int TimeSeriesMetadataMaxBytes = 10000;

        /// <summary>
        /// Maximum number of Timeseries metadata key/value pairs
        /// </summary>
        public const int TimeSeriesMetadataMaxPairs = 256;

        /// <summary>
        /// Sanitize a TimeSeriesCreate object so that it can be safely sent to CDF.
        /// Requests may still fail due to conflicts or missing ids.
        /// </summary>
        /// <param name="ts">TimeSeries to sanitize</param>
        public static void Sanitize(this TimeSeriesCreate ts)
        {
            if (ts == null) throw new ArgumentNullException(nameof(ts));
            ts.ExternalId = ts.ExternalId.Truncate(ExternalIdMax);
            ts.Name = ts.Name.Truncate(TimeSeriesNameMax);
            if (ts.AssetId < 1) ts.AssetId = null;
            ts.Description = ts.Description.Truncate(TimeSeriesDescriptionMax);
            if (ts.DataSetId < 1) ts.DataSetId = null;
            ts.Metadata = ts.Metadata.SanitizeMetadata(TimeSeriesMetadataMaxPerKey, TimeSeriesMetadataMaxPairs,
                TimeSeriesMetadataMaxPerValue, TimeSeriesMetadataMaxBytes, out _);
            ts.Unit = ts.Unit.Truncate(TimeSeriesUnitMax);
            ts.LegacyName = ts.LegacyName.Truncate(ExternalIdMax);
        }

        /// <summary>
        /// Check that given TimeSeriesCreate satisfies CDF limits.
        /// </summary>
        /// <param name="ts">Timeseries to check</param>
        /// <returns>True if timeseries satisfies limits</returns>
        public static ResourceType? Verify(this TimeSeriesCreate ts)
        {
            if (ts == null) throw new ArgumentNullException(nameof(ts));
            if (!ts.ExternalId.CheckLength(ExternalIdMax)) return ResourceType.ExternalId;
            if (!ts.Name.CheckLength(TimeSeriesNameMax)) return ResourceType.Name;
            if (ts.AssetId != null && ts.AssetId < 1) return ResourceType.AssetId;
            if (!ts.Description.CheckLength(TimeSeriesDescriptionMax)) return ResourceType.Description;
            if (ts.DataSetId != null && ts.DataSetId < 1) return ResourceType.DataSetId;
            if (!ts.Metadata.VerifyMetadata(TimeSeriesMetadataMaxPerKey, TimeSeriesMetadataMaxPairs,
                TimeSeriesMetadataMaxPerValue, TimeSeriesMetadataMaxBytes, out _)) return ResourceType.Metadata;
            if (!ts.Unit.CheckLength(TimeSeriesUnitMax)) return ResourceType.Unit;
            if (!ts.LegacyName.CheckLength(ExternalIdMax)) return ResourceType.LegacyName;
            return null;
        }

        private static readonly DistinctResource<TimeSeriesCreate>[] timeSeriesDistinct = new[]
        {
            new DistinctResource<TimeSeriesCreate>("Duplicated externalIds", ResourceType.ExternalId,
                ts => Identity.Create(ts.ExternalId)),
            new DistinctResource<TimeSeriesCreate>("Duplicated metric names in request", ResourceType.LegacyName,
                ts => Identity.Create(ts.LegacyName))
        };

        /// <summary>
        /// Clean list of TimeSeriesCreate objects, sanitizing each and removing any duplicates.
        /// The first encountered duplicate is kept.
        /// </summary>
        /// <param name="timeseries">TimeSeriesCreate request to clean</param>
        /// <param name="mode">The type of sanitation to apply</param>
        /// <returns>Cleaned create request and optional errors for duplicated ids and legacyNames</returns>
        public static (IEnumerable<TimeSeriesCreate>, IEnumerable<CogniteError<TimeSeriesCreate>>) CleanTimeSeriesRequest(
            IEnumerable<TimeSeriesCreate> timeseries,
            SanitationMode mode)
        {
            return CleanRequest(timeSeriesDistinct, timeseries, Verify, Sanitize, mode);
        }
    }
}
