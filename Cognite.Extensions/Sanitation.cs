using CogniteSdk;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Cognite.Extensions
{
    public static class Sanitation
    {
        public const int ExternalIdMax = 255;

        public const int AssetNameMax = 140;
        public const int AssetDescriptionMax = 500;
        public const int AssetMetadataMaxBytes = 10240;
        public const int AssetMetadataMaxPerKey = 128;
        public const int AssetMetadataMaxPerValue = 10240;
        public const int AssetMetadataMaxPairs = 256;
        public const int AssetSourceMax = 128;
        public const int AssetLabelsMax = 10;

        public const int TimeSeriesNameMax = 255;
        public const int TimeSeriesDescriptionMax = 1000;
        public const int TimeSeriesUnitMax = 32;
        public const int TimeSeriesMetadataMaxPerKey = 128;
        public const int TimeSeriesMetadataMaxPerValue = 10000;
        public const int TimeSeriesMetadataMaxBytes = 10000;
        public const int TimeSeriesMetadataMaxPairs = 256;

        public const int EventTypeMax = 64;
        public const int EventDescriptionMax = 500;
        public const int EventSourceMax = 128;
        public const int EventMetadataMaxPerKey = 128;
        public const int EventMetadataMaxPerValue = 128_000;
        public const int EventMetadataMaxPairs = 256;
        public const int EventmetadataMaxBytes = 200_000;
        public const int EventAssetIdsMax = 10_000;
        /// <summary>
        /// Reduce the length of given string to maxLength, if it is longer.
        /// </summary>
        /// <param name="str">String to be shortened</param>
        /// <param name="maxLength">Maximum length of final string</param>
        /// <returns>String which contains the first <paramref name="maxLength"/> characters of the passed string.</returns>
        public static string Truncate(this string str, int maxLength)
        {
            if (string.IsNullOrEmpty(str) || str.Length <= maxLength) return str;
            return str.Substring(0, maxLength);
        }

        private static bool CheckLength(this string str, int maxLength)
        {
            return string.IsNullOrEmpty(str) || str.Length <= maxLength;
        }

        /// <summary>
        /// Reduce the length of given CogniteExternalId to maxLength, if it is longer.
        /// </summary>
        /// <param name="id">CogniteExternalId to be shortened</param>
        /// <param name="maxLength">Maximum length of final string</param>
        /// <returns>CogniteExternalId which contains the first <paramref name="maxLength"/> characters of the passed value.</returns>
        public static CogniteExternalId Truncate(this CogniteExternalId id, int maxLength)
        {
            if (id == null) return id;
            var str = id.ExternalId;
            if (string.IsNullOrEmpty(str) || str.Length <= maxLength) return id;
            return new CogniteExternalId(str.Substring(0, maxLength));
        }

        /// <summary>
        /// Limit the maximum number of UTF8 bytes in the given string.
        /// </summary>
        /// <param name="str">String to truncate</param>
        /// <param name="n">Maximum number of UTF8 bytes in the final string</param>
        /// <returns>A truncated string, may be the same if no truncating was necessary</returns>
        public static string LimitUtf8ByteCount(this string str, int n)
        {
            if (string.IsNullOrEmpty(str) || Encoding.UTF8.GetByteCount(str) <= n) return str;

            var a = Encoding.UTF8.GetBytes(str);
            if (n > 0 && (a[n] & 0xC0) == 0x80)
            {
                // remove all bytes whose two highest bits are 10
                // and one more (start of multi-byte sequence - highest bits should be 11)
                while (--n > 0 && (a[n] & 0xC0) == 0x80) ;
            }
            // convert back to string (with the limit adjusted)
            return Encoding.UTF8.GetString(a, 0, n);
        }

        /// <summary>
        /// Transform an enumerable into a dictionary. Unlike the LINQ version,
        /// this simply uses the last value if there are duplicates, instead of throwing an error.
        /// </summary>
        /// <typeparam name="TInput">Input enumerable type</typeparam>
        /// <typeparam name="TKey">Output key type</typeparam>
        /// <typeparam name="TValue">Output value type</typeparam>
        /// <param name="input">Input enumerable</param>
        /// <param name="keySelector">Function to select key from input</param>
        /// <param name="valueSelector">Function to select value from input</param>
        /// <param name="comparer">IEqualityComparer to use for dictionary</param>
        /// <returns>A dictionary form <typeparamref name="TKey"/> to <typeparamref name="TValue"/></returns>
        public static Dictionary<TKey, TValue> ToDictionarySafe<TInput, TKey, TValue>(
            this IEnumerable<TInput> input,
            Func<TInput, TKey> keySelector,
            Func<TInput, TValue> valueSelector,
            IEqualityComparer<TKey> comparer = null)
        {
            if (input == null) throw new ArgumentNullException(nameof(input));
            var ret = new Dictionary<TKey, TValue>(comparer);
            foreach (var elem in input)
            {
                ret[keySelector(elem)] = valueSelector(elem);
            }
            return ret;
        }

        /// <summary>
        /// Sanitize a string, string metadata dictionary by limiting the number of UTF8 bytes per key,
        /// value and total, as well as the total number of key, value pairs.
        /// </summary>
        /// <param name="data">Metadata to limit</param>
        /// <param name="maxPerKey">Maximum number of bytes per key</param>
        /// <param name="maxKeys">Maximum number of key, value pairs</param>
        /// <param name="maxPerValue">Maximum number of bytes per value</param>
        /// <param name="maxBytes">Maximum number of total bytes</param>
        /// <returns>A sanitized dictionary</returns>
        public static Dictionary<string, string> SanitizeMetadata(this Dictionary<string, string> data,
            int maxPerKey,
            int maxKeys,
            int maxPerValue,
            int maxBytes)
        {
            if (data == null || !data.Any()) return data;
            int count = 0;
            int byteCount = 0;
            return data
                .Where(kvp => kvp.Key != null)
                .Select(kvp => (kvp.Key.LimitUtf8ByteCount(maxPerKey), kvp.Value.LimitUtf8ByteCount(maxPerValue)))
                .TakeWhile(pair =>
                {
                    count++;
                    byteCount += Encoding.UTF8.GetByteCount(pair.Item1) + Encoding.UTF8.GetByteCount(pair.Item2);
                    return count <= maxKeys && byteCount <= maxBytes;
                })
                .ToDictionarySafe(pair => pair.Item1, pair => pair.Item2);
        }

        public static bool VerifyMetadata(this Dictionary<string, string> data,
            int maxPerKey,
            int maxKeys,
            int maxPerValue,
            int maxBytes)
        {
            if (data == null || !data.Any()) return true;
            int count = 0;
            int byteCount = 0;
            foreach (var kvp in data)
            {
                var valueByteCount = string.IsNullOrEmpty(kvp.Value) ? 0 : Encoding.UTF8.GetByteCount(kvp.Value);
                if (valueByteCount > maxPerValue) return false;
                var keyByteCount = Encoding.UTF8.GetByteCount(kvp.Key);
                if (keyByteCount > maxPerKey) return false;
                byteCount += valueByteCount + keyByteCount;
                count++;
                if (byteCount > maxBytes || count > maxKeys) return false;
            }
            return true;
        }

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
            asset.Metadata = asset.Metadata?.SanitizeMetadata(AssetMetadataMaxPerKey, AssetMetadataMaxPairs, AssetMetadataMaxPerValue, AssetMetadataMaxBytes);
            asset.Source = asset.Source.Truncate(AssetSourceMax);
            asset.Labels = asset.Labels?
                .Where(label => label != null && label.ExternalId != null)
                .Select(label => label.Truncate(ExternalIdMax))
                .Take(10);
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
            if (!asset.Name.CheckLength(AssetNameMax)) return ResourceType.Name;
            if (asset.ParentId != null && asset.ParentId < 1) return ResourceType.ParentId;
            if (!asset.ParentExternalId.CheckLength(ExternalIdMax)) return ResourceType.ParentExternalId;
            if (!asset.Description.CheckLength(AssetDescriptionMax)) return ResourceType.Description;
            if (asset.DataSetId != null && asset.DataSetId < 1) return ResourceType.DataSetId;
            if (!asset.Metadata.VerifyMetadata(AssetMetadataMaxPerKey, AssetMetadataMaxPairs, AssetMetadataMaxPerValue, AssetMetadataMaxBytes))
                return ResourceType.Metadata;
            if (!asset.Source.CheckLength(AssetSourceMax)) return ResourceType.Source;
            if (asset.Labels != null && (asset.Labels.Count() > AssetLabelsMax || asset.Labels.Any(label => !label.ExternalId.CheckLength(ExternalIdMax))))
                return ResourceType.Labels;
            return null;
        }

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
                TimeSeriesMetadataMaxPerValue, TimeSeriesMetadataMaxBytes);
            ts.Unit = ts.Unit.Truncate(TimeSeriesUnitMax);
            ts.LegacyName = ts.LegacyName.Truncate(ExternalIdMax);
        }

        /// <summary>
        /// Check that given TimeSeriesCreate satisfies CDF limits.
        /// </summary>
        /// <param name="asset">Timeseries to check</param>
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
                TimeSeriesMetadataMaxPerValue, TimeSeriesMetadataMaxPerValue)) return ResourceType.Metadata;
            if (!ts.Unit.CheckLength(TimeSeriesUnitMax)) return ResourceType.Unit;
            if (!ts.LegacyName.CheckLength(ExternalIdMax)) return ResourceType.LegacyName;
            return null;
        }

        /// <summary>
        /// Sanitize a EventCreate object so that it can be safely sent to CDF.
        /// Requests may still fail due to conflicts or missing ids.
        /// </summary>
        /// <param name="evt">TimeSeries to sanitize</param>
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
            evt.Metadata = evt.Metadata.SanitizeMetadata(EventMetadataMaxPerKey, EventMetadataMaxPairs, EventMetadataMaxPerValue, EventmetadataMaxBytes);
        }

        /// <summary>
        /// Check that given EventCreate satisfies CDF limits.
        /// </summary>
        /// <param name="asset">Event to check</param>
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
            if (!evt.Metadata.VerifyMetadata(EventMetadataMaxPerKey, EventMetadataMaxPairs, EventMetadataMaxPerValue, EventmetadataMaxBytes))
                return ResourceType.Metadata;
            return null;
        }

        /// <summary>
        /// Clean list of AssetCreate objects, sanitizing each and removing any duplicates.
        /// The first encountered duplicate is kept.
        /// </summary>
        /// <param name="assets">AssetCreate request to clean</param>
        /// <param name="mode">The type of sanitation to apply</param>
        /// <returns>Cleaned create request and an optional error if any ids were duplicated</returns>
        public static (IEnumerable<AssetCreate>, IEnumerable<CogniteError>) CleanAssetRequest(IEnumerable<AssetCreate> assets, SanitationMode mode)
        {
            var result = new List<AssetCreate>();
            var errors = new List<CogniteError>();

            var ids = new HashSet<string>();
            var duplicated = new HashSet<string>();
            var bad = new List<(ResourceType, AssetCreate)>();

            foreach (var asset in assets)
            {
                bool toAdd = true;
                if (mode == SanitationMode.Remove)
                {
                    var failedField = asset.Verify();
                    if (failedField.HasValue)
                    {
                        bad.Add((failedField.Value, asset));
                        toAdd = false;
                    }
                }
                else if (mode == SanitationMode.Clean)
                {
                    asset.Sanitize();
                }
                if (asset.ExternalId != null)
                {
                    if (!ids.Add(asset.ExternalId))
                    {
                        duplicated.Add(asset.ExternalId);
                        toAdd = false;
                    }
                }
                if (toAdd)
                {
                    result.Add(asset);
                }
            }
            if (duplicated.Any())
            {
                errors.Add(new CogniteError
                {
                    Status = 409,
                    Message = "Duplicate external ids",
                    Resource = ResourceType.ExternalId,
                    Type = ErrorType.ItemDuplicated,
                    Values = duplicated.Select(item => Identity.Create(item)).ToArray()
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

        /// <summary>
        /// Clean list of TimeSeriesCreate objects, sanitizing each and removing any duplicates.
        /// The first encountered duplicate is kept.
        /// </summary>
        /// <param name="timeseries">TimeSeriesCreate request to clean</param>
        /// <param name="mode">The type of sanitation to apply</param>
        /// <returns>Cleaned create request and optional errors for duplicated ids and legacyNames</returns>
        public static (IEnumerable<TimeSeriesCreate>, IEnumerable<CogniteError>) CleanTimeSeriesRequest(IEnumerable<TimeSeriesCreate> timeseries,
            SanitationMode mode)
        {
            var result = new List<TimeSeriesCreate>();
            var errors = new List<CogniteError>();

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
                errors.Add(new CogniteError
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
                errors.Add(new CogniteError
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

        /// <summary>
        /// Clean list of EventCreate objects, sanitizing each and removing any duplicates.
        /// The first encountered duplicate is kept.
        /// </summary>
        /// <param name="events">EventCreate request to clean</param>
        /// <param name="mode">The type of sanitation to apply</param>
        /// <returns>Cleaned request and optional error if any ids were duplicated</returns>
        public static (IEnumerable<EventCreate>, IEnumerable<CogniteError>) CleanEventRequest(IEnumerable<EventCreate> events, SanitationMode mode)
        {
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
