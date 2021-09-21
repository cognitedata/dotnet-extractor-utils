using CogniteSdk;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Cognite.Extensions
{
    public static partial class Sanitation
    {
        /// <summary>
        /// Maximum length of Event type
        /// </summary>
        public const int EventTypeMax = 64;

        /// <summary>
        /// Maximum length of Event description
        /// </summary>
        public const int EventDescriptionMax = 500;

        /// <summary>
        /// Maximum length of Event source
        /// </summary>
        public const int EventSourceMax = 128;

        /// <summary>
        /// Maximum size in bytes of each Event metadata key
        /// </summary>
        public const int EventMetadataMaxPerKey = 128;

        /// <summary>
        /// Maximum size in bytes of each Event metadata value
        /// </summary>
        public const int EventMetadataMaxPerValue = 128_000;

        /// <summary>
        /// Maximum number Event metadata key/value pairs
        /// </summary>
        public const int EventMetadataMaxPairs = 256;

        /// <summary>
        /// Maximum size in bytes of Event metadata field
        /// </summary>
        public const int EventMetadataMaxBytes = 200_000;

        /// <summary>
        /// Maximum number of Event asset ids
        /// </summary>
        public const int EventAssetIdsMax = 10_000;

        /// <summary>
        /// Sanitize a EventCreate object so that it can be safely sent to CDF.
        /// Requests may still fail due to conflicts or missing ids.
        /// </summary>
        /// <param name="evt">Event to sanitize</param>
        public static void Sanitize(this EventCreate evt)
        {
            if (evt == null) throw new ArgumentNullException(nameof(evt));
            evt.ExternalId = evt.ExternalId.Truncate(ExternalIdMax);
            evt.Type = evt.Type.Truncate(EventTypeMax);
            evt.Subtype = evt.Subtype.Truncate(EventTypeMax);
            evt.Source = evt.Source.Truncate(EventSourceMax);
            evt.Description = evt.Description.Truncate(EventDescriptionMax);
            evt.AssetIds = evt.AssetIds?
                .Where(id => id > 0)
                .Take(EventAssetIdsMax);
            if (evt.StartTime < 0) evt.StartTime = 0;
            if (evt.EndTime < 0) evt.EndTime = 0;
            if (evt.StartTime > evt.EndTime) evt.EndTime = evt.StartTime;
            if (evt.DataSetId < 1) evt.DataSetId = null;
            evt.Metadata = evt.Metadata.SanitizeMetadata(
                EventMetadataMaxPerKey, EventMetadataMaxPairs, EventMetadataMaxPerValue, EventMetadataMaxBytes, out _);
        }

        /// <summary>
        /// Check that given EventCreate satisfies CDF limits.
        /// </summary>
        /// <param name="evt">Event to check</param>
        /// <returns>True if event satisfies limits</returns>
        public static ResourceType? Verify(this EventCreate evt)
        {
            if (evt == null) throw new ArgumentNullException(nameof(evt));
            if (!evt.ExternalId.CheckLength(ExternalIdMax)) return ResourceType.ExternalId;
            if (!evt.Type.CheckLength(EventTypeMax)) return ResourceType.Type;
            if (!evt.Subtype.CheckLength(EventTypeMax)) return ResourceType.SubType;
            if (!evt.Source.CheckLength(EventSourceMax)) return ResourceType.Source;
            if (evt.AssetIds != null && (evt.AssetIds.Count() > EventAssetIdsMax || evt.AssetIds.Any(id => id < 1))) return ResourceType.AssetId;
            if (evt.StartTime != null && evt.StartTime < 1
                || evt.EndTime != null && evt.EndTime < 1
                || evt.StartTime != null && evt.EndTime != null && evt.StartTime > evt.EndTime) return ResourceType.TimeRange;
            if (evt.DataSetId != null && evt.DataSetId < 1) return ResourceType.DataSetId;
            if (!evt.Metadata.VerifyMetadata(EventMetadataMaxPerKey, EventMetadataMaxPairs,
                EventMetadataMaxPerValue, EventMetadataMaxBytes, out _))
                return ResourceType.Metadata;
            return null;
        }

        /// <summary>
        /// Clean list of EventCreate objects, sanitizing each and removing any duplicates.
        /// The first encountered duplicate is kept.
        /// </summary>
        /// <param name="events">EventCreate request to clean</param>
        /// <param name="mode">The type of sanitation to apply</param>
        /// <returns>Cleaned request and optional error if any ids were duplicated</returns>
        public static (IEnumerable<EventCreate>, IEnumerable<CogniteError>) CleanEventRequest(
            IEnumerable<EventCreate> events,
            SanitationMode mode)
        {
            if (mode == SanitationMode.None) return (events, Enumerable.Empty<CogniteError>());
            if (events == null)
            {
                throw new ArgumentNullException(nameof(events));
            }
            var result = new List<EventCreate>();
            var errors = new List<CogniteError>();

            var ids = new HashSet<string>();
            var duplicated = new HashSet<string>();

            var bad = new List<(ResourceType, EventCreate)>();

            foreach (var evt in events)
            {
                bool toAdd = true;
                if (mode == SanitationMode.Remove)
                {
                    var failedField = evt.Verify();
                    if (failedField.HasValue)
                    {
                        bad.Add((failedField.Value, evt));
                        toAdd = false;
                    }
                }
                else if (mode == SanitationMode.Clean)
                {
                    evt.Sanitize();
                }
                if (evt.ExternalId != null)
                {
                    if (!ids.Add(evt.ExternalId))
                    {
                        duplicated.Add(evt.ExternalId);
                        toAdd = false;
                    }
                }
                if (toAdd)
                {
                    result.Add(evt);
                }
            }
            if (duplicated.Any())
            {
                errors.Add(new CogniteError
                {
                    Status = 409,
                    Message = "ExternalIds duplicated",
                    Resource = ResourceType.ExternalId,
                    Type = ErrorType.ItemDuplicated,
                    Values = duplicated.Select(Identity.Create).ToArray()
                });
            }
            if (bad.Any())
            {
                errors.AddRange(bad.GroupBy(pair => pair.Item1).Select(group => new CogniteError
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
