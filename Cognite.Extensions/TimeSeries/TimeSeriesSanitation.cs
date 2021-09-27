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
            if (mode == SanitationMode.None) return (timeseries, Enumerable.Empty<CogniteError<TimeSeriesCreate>>());
            if (timeseries == null)
            {
                throw new ArgumentNullException(nameof(timeseries));
            }
            var result = new List<TimeSeriesCreate>();
            var errors = new List<CogniteError<TimeSeriesCreate>>();

            var ids = new HashSet<string>();
            var duplicatedIds = new HashSet<string>();

            var names = new HashSet<string>();
            var duplicatedNames = new HashSet<string>();

            var bad = new List<(ResourceType, TimeSeriesCreate)>();


            foreach (var ts in timeseries)
            {
                bool toAdd = true;
                if (mode == SanitationMode.Remove)
                {
                    var failedField = ts.Verify();
                    if (failedField.HasValue)
                    {
                        bad.Add((failedField.Value, ts));
                        toAdd = false;
                    }
                }
                else if (mode == SanitationMode.Clean)
                {
                    ts.Sanitize();
                }
                if (ts.ExternalId != null)
                {
                    if (!ids.Add(ts.ExternalId))
                    {
                        duplicatedIds.Add(ts.ExternalId);
                        toAdd = false;
                    }
                }
                if (ts.LegacyName != null)
                {
                    if (!names.Add(ts.LegacyName))
                    {
                        duplicatedNames.Add(ts.LegacyName);
                        toAdd = false;
                    }
                }
                if (toAdd)
                {
                    result.Add(ts);
                }
            }
            if (duplicatedIds.Any())
            {
                errors.Add(new CogniteError<TimeSeriesCreate>
                {
                    Status = 409,
                    Message = "Conflicting identifiers",
                    Resource = ResourceType.ExternalId,
                    Type = ErrorType.ItemDuplicated,
                    Values = duplicatedIds.Select(Identity.Create).ToArray()
                });
            }
            if (duplicatedNames.Any())
            {
                errors.Add(new CogniteError<TimeSeriesCreate>
                {
                    Status = 409,
                    Message = "Duplicated metric names in request",
                    Resource = ResourceType.LegacyName,
                    Type = ErrorType.ItemDuplicated,
                    Values = duplicatedNames.Select(Identity.Create).ToArray()
                });
            }
            if (bad.Any())
            {
                errors.AddRange(bad.GroupBy(pair => pair.Item1).Select(group => new CogniteError<TimeSeriesCreate>
                {
                    Skipped = group.Select(pair => pair.Item2).ToList(),
                    Resource = group.Key,
                    Type = ErrorType.SanitationFailed,
                    Status = 400
                }));
            }
            return (result, errors);
        }
    }
}
