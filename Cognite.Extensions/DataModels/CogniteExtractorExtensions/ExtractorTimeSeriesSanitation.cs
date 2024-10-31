using System;
using System.Collections.Generic;
using Cognite.Extensions.DataModels.CogniteExtractorExtensions;
using CogniteSdk;
using CogniteSdk.DataModels;
using CogniteSdk.DataModels.Core;
using static Cognite.Extensions.Sanitation;

namespace Cognite.Extensions.DataModels
{
    /// <summary>
    /// Collection of methods for cleaning and sanitizing objects used in
    /// requests to CDM TimeSeries
    /// </summary>
    public static partial class CoreTSSanitation
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
        /// Sanitize a T object so that it can be safely sent to CDF.
        /// Requests may still fail due to conflicts or missing ids.
        /// </summary>
        /// <param name="ts">TimeSeries to sanitize</param>
        public static void Sanitize<T>(this SourcedNodeWrite<T> ts) where T : CogniteTimeSeriesBase
        {
            DataModelSanitation.Sanitize(ts);
            ts.Properties.Name = ts.Properties.Name.Truncate(TimeSeriesNameMax);
            ts.Properties.Description = ts.Properties.Description.Truncate(TimeSeriesDescriptionMax);
            ts.Properties.SourceUnit = ts.Properties.SourceUnit.Truncate(TimeSeriesUnitMax);
            if (typeof(CogniteExtractorTimeSeries).IsAssignableFrom(typeof(T)))
            {
                (ts.Properties as CogniteExtractorTimeSeries)!.extractedData = (ts.Properties as CogniteExtractorTimeSeries)!.extractedData?.SanitizeMetadata(TimeSeriesMetadataMaxPerKey, TimeSeriesMetadataMaxPairs,
                    TimeSeriesMetadataMaxPerValue, TimeSeriesMetadataMaxBytes, out _);
            }
        }

        /// <summary>
        /// Check that given T satisfies CDF limits.
        /// </summary>
        /// <param name="ts">Timeseries to check</param>
        /// <returns>True if timeseries satisfies limits</returns>
        public static ResourceType? Verify<T>(this SourcedNodeWrite<T> ts) where T : CogniteTimeSeriesBase
        {
            var baseChecks = DataModelSanitation.Verify(ts);
            if (baseChecks != null)
            {
                return baseChecks;
            }
            if (!ts.Properties.Name.CheckLength(TimeSeriesNameMax)) return ResourceType.Name;
            if (!ts.Properties.Description.CheckLength(TimeSeriesDescriptionMax)) return ResourceType.Description;
            if (!ts.Properties.SourceUnit.CheckLength(TimeSeriesUnitMax)) return ResourceType.Unit;
            if (typeof(CogniteExtractorTimeSeries).IsAssignableFrom(typeof(T)) && !(ts.Properties as CogniteExtractorTimeSeries)!.extractedData.VerifyMetadata(TimeSeriesMetadataMaxPerKey, TimeSeriesMetadataMaxPairs,
                TimeSeriesMetadataMaxPerValue, TimeSeriesMetadataMaxBytes, out _)) return ResourceType.Metadata;
            return null;
        }

        /// <summary>
        /// Clean list of T objects, sanitizing each and removing any duplicates.
        /// The first encountered duplicate is kept.
        /// </summary>
        /// <param name="timeseries">T request to clean</param>
        /// <param name="mode">The type of sanitation to apply</param>
        /// <returns>Cleaned create request and optional errors for duplicated ids and legacyNames</returns>
        public static (IEnumerable<SourcedNodeWrite<T>>, IEnumerable<CogniteError<SourcedNodeWrite<T>>>) CleanTimeSeriesRequest<T>(
            IEnumerable<SourcedNodeWrite<T>> timeseries,
            SanitationMode mode) where T : CogniteTimeSeriesBase
        {
            DistinctResource<SourcedNodeWrite<T>>[] timeSeriesDistinct = new[]
            {
                new DistinctResource<SourcedNodeWrite<T>>("Duplicated instance ids", ResourceType.InstanceId,
                    ts => ts.ExternalId != null && ts.Space != null ? Identity.Create(new InstanceIdentifier(ts.Space, ts.ExternalId)) : null),
            };
            return CleanRequest(timeSeriesDistinct, timeseries, Verify, Sanitize, mode);
        }
    }
}
