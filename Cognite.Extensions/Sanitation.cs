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

        public const int TimeSeriesNameMax = 255;
        public const int TimeSeriesDescriptionMax = 1000;
        public const int TimeSeriesUnitMax = 32;
        public const int TimeSeriesMetadataMaxPerKey = 32;
        public const int TimeSeriesMetadataMaxPerValue = 512;
        public const int TimeSeriesMetadataMaxPairs = 16;

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

        /// <summary>
        /// Sanitize a string, string metadata dictionary by limiting the number of UTF8 bytes per key
        /// and value, as well as the total number of key, value pairs.
        /// </summary>
        /// <param name="data">Metadata to limit</param>
        /// <param name="maxPerKey">Maximum number of bytes per key</param>
        /// <param name="maxKeys">Maximum number of keys</param>
        /// <param name="maxPerValue">Maximum number of bytes per value</param>
        /// <returns></returns>
        public static Dictionary<string, string> SanitizeMetadata(this Dictionary<string, string> data,
            int maxPerKey,
            int maxKeys,
            int maxPerValue)
        {
            if (data == null || !data.Any()) return data;
            return data
                .Where(kvp => kvp.Key != null)
                .Select(kvp => (kvp.Key.LimitUtf8ByteCount(maxPerKey), kvp.Value.LimitUtf8ByteCount(maxPerValue)))
                .Take(maxKeys)
                .ToDictionarySafe(pair => pair.Item1, pair => pair.Item2);
        }

        /// <summary>
        /// Sanitize an AssetCreate so that it can be safely sent to CDF.
        /// Requests may still fail due to conflicts or missing ids.
        /// </summary>
        /// <param name="asset">Asset to sanitize</param>
        public static void Sanitize(this AssetCreate asset)
        {
            if (asset == null) throw new ArgumentNullException(nameof(asset));
            asset.ExternalId = asset.ExternalId?.Truncate(ExternalIdMax);
            asset.Name = asset.Name?.Truncate(AssetNameMax);
            if (asset.ParentId < 1) asset.ParentId = null;
            asset.ParentExternalId = asset.ParentExternalId?.Truncate(ExternalIdMax);
            asset.Description = asset.Description?.Truncate(AssetDescriptionMax);
            if (asset.DataSetId < 1) asset.DataSetId = null;
            asset.Metadata = asset.Metadata?.SanitizeMetadata(AssetMetadataMaxPerKey, AssetMetadataMaxPairs, AssetMetadataMaxPerValue, AssetMetadataMaxBytes);
            asset.Source = asset.Source.Truncate(AssetSourceMax);
            asset.Labels = asset.Labels?
                .Where(label => label != null && label.ExternalId != null)
                .Select(label => label.Truncate(ExternalIdMax))
                .Take(10);
        }

        /// <summary>
        /// Sanitize a TimeSeriesCreate object so that it can be safely sent to CDF.
        /// Requests may still fail due to conflicts or missing ids.
        /// </summary>
        /// <param name="ts">TimeSeries to sanitize</param>
        public static void Sanitize(this TimeSeriesCreate ts)
        {
            if (ts == null) throw new ArgumentNullException(nameof(ts));
            ts.ExternalId = ts.ExternalId?.Truncate(ExternalIdMax);
            ts.Name = ts.Name?.Truncate(TimeSeriesNameMax);
            if (ts.AssetId < 1) ts.AssetId = null;
            ts.Description = ts.Description?.Truncate(TimeSeriesDescriptionMax);
            if (ts.DataSetId < 1) ts.DataSetId = null;
            ts.Metadata = ts.Metadata?.SanitizeMetadata(TimeSeriesMetadataMaxPerKey, TimeSeriesMetadataMaxPairs, TimeSeriesMetadataMaxPerValue);
            ts.Unit = ts.Unit?.Truncate(TimeSeriesUnitMax);
            ts.LegacyName = ts.LegacyName?.Truncate(ExternalIdMax);
        }

        /// <summary>
        /// Sanitize a EventCreate object so that it can be safely sent to CDF.
        /// Requests may still fail due to conflicts or missing ids.
        /// </summary>
        /// <param name="evt">TimeSeries to sanitize</param>
        public static void Sanitize(this EventCreate evt)
        {
            if (evt == null) throw new ArgumentNullException(nameof(evt));
            evt.ExternalId = evt.ExternalId?.Truncate(ExternalIdMax);
            evt.Type = evt.Type?.Truncate(EventTypeMax);
            evt.Subtype = evt.Subtype?.Truncate(EventTypeMax);
            evt.Source = evt.Source?.Truncate(EventSourceMax);
            evt.Description = evt.Description?.Truncate(EventDescriptionMax);
            evt.AssetIds = evt.AssetIds?
                .Where(id => id > 0)
                .Take(EventAssetIdsMax);
            if (evt.StartTime < 0) evt.StartTime = 0;
            if (evt.EndTime < 0) evt.EndTime = 0;
            if (evt.StartTime > evt.EndTime) evt.EndTime = evt.StartTime; 
            if (evt.DataSetId < 1) evt.DataSetId = null;
            evt.Metadata = evt.Metadata?.SanitizeMetadata(EventMetadataMaxPerKey, EventMetadataMaxPairs, EventMetadataMaxPerValue, EventmetadataMaxBytes);
        }

        /// <summary>
        /// Clean list of AssetCreate objects, sanitizing each and removing any duplicates.
        /// </summary>
        /// <param name="assets">AssetCreate request to clean</param>
        /// <returns>Cleaned create request and an optional error if any ids were duplicated</returns>
        public static (IEnumerable<AssetCreate>, CogniteError) CleanAssetRequest(IEnumerable<AssetCreate> assets)
        {
            var result = new List<AssetCreate>();

            var ids = new HashSet<string>();
            var duplicated = new HashSet<string>();

            foreach (var asset in assets)
            {
                asset.Sanitize();
                if (asset.ExternalId != null)
                {
                    if (!ids.Add(asset.ExternalId))
                    {
                        duplicated.Add(asset.ExternalId);
                        continue;
                    }
                }
                result.Add(asset);
            }
            CogniteError error = null;
            if (duplicated.Any())
            {
                error = new CogniteError
                {
                    Status = 409,
                    Message = "Duplicate external ids",
                    Resource = ResourceType.ExternalId,
                    Type = ErrorType.ItemDuplicated,
                    Values = duplicated.Select(item => Identity.Create(item)).ToArray()
                };
            }
            return (result, error);
        }

        /// <summary>
        /// Clean list of TimeSeriesCreate objects, sanitizing each and removing any duplicates.
        /// </summary>
        /// <param name="timeseries">TimeSeriesCreate request to clean</param>
        /// <returns>Cleaned create request and optional errors for duplicated ids and legacyNames</returns>
        public static (IEnumerable<TimeSeriesCreate>, CogniteError idError, CogniteError nameError) CleanTimeSeriesRequest(IEnumerable<TimeSeriesCreate> timeseries)
        {
            var result = new List<TimeSeriesCreate>();

            var ids = new HashSet<string>();
            var duplicatedIds = new HashSet<string>();

            var names = new HashSet<string>();
            var duplicatedNames = new HashSet<string>();

            foreach (var ts in timeseries)
            {
                ts.Sanitize();
                if (ts.ExternalId != null)
                {
                    if (!ids.Add(ts.ExternalId))
                    {
                        duplicatedIds.Add(ts.ExternalId);
                        continue;
                    }
                }
                if (ts.LegacyName != null)
                {
                    if (!names.Add(ts.LegacyName))
                    {
                        duplicatedNames.Add(ts.LegacyName);
                        continue;
                    }
                }
                result.Add(ts);
            }
            CogniteError idError = null;
            if (duplicatedIds.Any())
            {
                idError = new CogniteError
                {
                    Status = 409,
                    Message = "Conflicting identifiers",
                    Resource = ResourceType.ExternalId,
                    Type = ErrorType.ItemDuplicated,
                    Values = duplicatedIds.Select(Identity.Create).ToArray()
                };
            }
            CogniteError nameError = null;
            if (duplicatedNames.Any())
            {
                nameError = new CogniteError
                {
                    Status = 409,
                    Message = "Duplicated metric names in request",
                    Resource = ResourceType.LegacyName,
                    Type = ErrorType.ItemDuplicated,
                    Values = duplicatedNames.Select(Identity.Create).ToArray()
                };
            }
            return (result, idError, nameError);
        }

        /// <summary>
        /// Clean list of EventCreate objects, sanitizing each and removing any duplicates.
        /// </summary>
        /// <param name="events">EventCreate request to clean</param>
        /// <returns>Cleaned request and optional error if any ids were duplicated</returns>
        public static (IEnumerable<EventCreate>, CogniteError) CleanEventRequest(IEnumerable<EventCreate> events)
        {
            var result = new List<EventCreate>();

            var ids = new HashSet<string>();
            var duplicated = new HashSet<string>();

            foreach (var evt in events)
            {
                evt.Sanitize();
                if (evt.ExternalId != null)
                {
                    if (!ids.Add(evt.ExternalId))
                    {
                        duplicated.Add(evt.ExternalId);
                        continue;
                    }
                }
                result.Add(evt);
            }
            CogniteError err = null;
            if (duplicated.Any())
            {
                err = new CogniteError
                {
                    Status = 409,
                    Message = "ExternalIds duplicated",
                    Resource = ResourceType.ExternalId,
                    Type = ErrorType.ItemDuplicated,
                    Values = duplicated.Select(Identity.Create).ToArray()
                };
            }
            return (result, err);
        }
    }
}
