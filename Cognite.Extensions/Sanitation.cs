using CogniteSdk;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Cognite.Extensions
{
    /// <summary>
    /// Collection of methods for cleaning and sanitizing objects used in
    /// requests to various CDF endpoints
    /// </summary>
    public static partial class Sanitation
    {
        /// <summary>
        /// Maximum length of External ID
        /// </summary>
        public const int ExternalIdMax = 255;

        /// <summary>
        /// Maximum length of DM Space ID
        /// </summary>
        public const int SpaceIdMax = 43;

        /// <summary>
        /// Maximum length of Instance ID
        /// </summary>
        public const int ExternalIdMaxBytes = 256;

        /// <summary>
        /// Reduce the length of given string to maxLength, if it is longer.
        /// </summary>
        /// <param name="str">String to be shortened</param>
        /// <param name="maxLength">Maximum length of final string</param>
        /// <returns>String which contains the first <paramref name="maxLength"/> characters of the passed string.</returns>
#if NETSTANDARD2_1_OR_GREATER
        [return: System.Diagnostics.CodeAnalysis.NotNullIfNotNull("str")]
#endif
        public static string? Truncate(this string? str, int maxLength)
        {
            if (string.IsNullOrEmpty(str) || str!.Length <= maxLength) return str;
            return str.Substring(0, maxLength);
        }

        /// <summary>
        /// Reduce the length of given string to maxLength of UTF-8 bytes, if it is longer.
        /// </summary>
        /// <param name="str">String to be shortened</param>
        /// <param name="maxLength">Maximum length of final string in bytes</param>
        /// <returns>String which contains the characters fitting in <paramref name="maxLength"/> bytes from the start of the passed string.</returns>
#if NETSTANDARD2_1_OR_GREATER
        [return: System.Diagnostics.CodeAnalysis.NotNullIfNotNull("str")]
#endif
        public static string? TruncateBytes(this string? str, int maxLength)
        {
            if (string.IsNullOrEmpty(str)) return str;
            return str.LimitUtf8ByteCount(maxLength);
        }

        internal static bool CheckLength(this string? str, int maxLength)
        {
            return string.IsNullOrEmpty(str) || str!.Length <= maxLength;
        }

        /// <summary>
        /// Reduce the length of given CogniteExternalId to maxLength, if it is longer.
        /// </summary>
        /// <param name="id">CogniteExternalId to be shortened</param>
        /// <param name="maxLength">Maximum length of final string</param>
        /// <returns>CogniteExternalId which contains the first <paramref name="maxLength"/> characters of the passed value.</returns>
#if NETSTANDARD2_1_OR_GREATER
        [return: System.Diagnostics.CodeAnalysis.NotNullIfNotNull("id")]
#endif
        public static CogniteExternalId? Truncate(this CogniteExternalId? id, int maxLength)
        {
            if (id == null) return id;
            var str = id.ExternalId;
            if (string.IsNullOrEmpty(str) || str.Length <= maxLength) return id;
            return new CogniteExternalId(str.Substring(0, maxLength));
        }

        /// <summary>
        /// Reduce the length of "set" on given update to maxLength, if it is longer.
        /// </summary>
        /// <param name="update">String update to be shortened</param>
        /// <param name="maxLength">Maximum length of final string</param>
        /// <returns>String update which contains the first <paramref name="maxLength"/> characters of the passed value.</returns>
#if NETSTANDARD2_1_OR_GREATER
        [return: System.Diagnostics.CodeAnalysis.NotNullIfNotNull("update")]
#endif
        public static Update<string>? Truncate(this Update<string>? update, int maxLength)
        {
            if (update == null) return null;
            if (string.IsNullOrEmpty(update.Set) || update.Set.Length <= maxLength) return update;
            return new Update<string>(update.Set.Substring(0, maxLength));
        }

        /// <summary>
        /// Reduce the length of "set" on given update to maxLength, if it is longer.
        /// </summary>
        /// <param name="update">String update to be shortened</param>
        /// <param name="maxLength">Maximum length of final string</param>
        /// <returns>String update which contains the first <paramref name="maxLength"/> characters of the passed value.</returns>
        public static UpdateNullable<string>? Truncate(this UpdateNullable<string>? update, int maxLength)
        {
            if (update == null) return null;
            if (update.SetNull ?? false) return update;
            if (update.Set.Length <= maxLength) return update;
            return new UpdateNullable<string>(update.Set.Substring(0, maxLength));
        }

        /// <summary>
        /// Limit the maximum number of UTF8 bytes in the given string.
        /// </summary>
        /// <param name="str">String to truncate</param>
        /// <param name="n">Maximum number of UTF8 bytes in the final string</param>
        /// <returns>A truncated string, may be the same if no truncating was necessary</returns>
#if NETSTANDARD2_1_OR_GREATER
        [return: System.Diagnostics.CodeAnalysis.NotNullIfNotNull("str")]
#endif
        public static string? LimitUtf8ByteCount(this string? str, int n)
        {
            if (SafeByteCount(str) <= n) return str;

            var a = Encoding.UTF8.GetBytes(str!);
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
            IEqualityComparer<TKey>? comparer = null) where TKey : notnull
        {
            if (input == null)
            {
                throw new ArgumentNullException(nameof(input));
            }
            if (keySelector == null)
            {
                throw new ArgumentNullException(nameof(keySelector));
            }
            if (valueSelector == null)
            {
                throw new ArgumentNullException(nameof(valueSelector));
            }
            var ret = new Dictionary<TKey, TValue>(comparer);
            foreach (var elem in input)
            {
                ret[keySelector(elem)] = valueSelector(elem);
            }
            return ret;
        }

        private static int SafeByteCount(string? str)
        {
            if (string.IsNullOrEmpty(str)) return 0;

            return Encoding.UTF8.GetByteCount(str);
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
        /// <param name="bytes">Total number of bytes in returned metadata</param>
        /// <returns>A sanitized dictionary</returns>
#if NETSTANDARD2_1_OR_GREATER
        [return: System.Diagnostics.CodeAnalysis.NotNullIfNotNull("data")]
#endif
        public static Dictionary<string, string>? SanitizeMetadata(this Dictionary<string, string>? data,
            int maxPerKey,
            int maxKeys,
            int maxPerValue,
            int maxBytes,
            out int bytes)
        {
            bytes = 0;
            if (data == null || !data.Any()) return data;
            int count = 0;
            int byteCount = 0;
            var result = data
                .Where(kvp => kvp.Key != null)
                .Select(kvp => (kvp.Key.LimitUtf8ByteCount(maxPerKey), kvp.Value.LimitUtf8ByteCount(maxPerValue) ?? ""))
                .TakeWhile(pair =>
                {
                    count++;
                    int numBytes = SafeByteCount(pair.Item1) + SafeByteCount(pair.Item2);
                    if (count <= maxKeys && byteCount + numBytes <= maxBytes)
                    {
                        byteCount += numBytes;
                        return true;
                    }
                    return false;
                })
                .ToDictionarySafe(pair => pair.Item1!, pair => pair.Item2);
            bytes = byteCount;
            return result;
        }

        /// <summary>
        /// Check that the given metadata dictionary satisfies the limits of UTF8 bytes per key,
        /// value and total, as well as the total number of key, value pairs, as specified
        /// in the parameters
        /// </summary>
        /// <param name="data">Metadata to verify</param>
        /// <param name="maxPerKey">Maximum number of bytes per key</param>
        /// <param name="maxKeys">Maximum number of key, value pairs</param>
        /// <param name="maxPerValue">Maximum number of bytes per value</param>
        /// <param name="maxBytes">Maximum number of total bytes</param>
        /// <param name="bytes">Total number of bytes in metadata</param>
        /// <returns>True if the limits are satisfied, false otherwise</returns>
        public static bool VerifyMetadata(this Dictionary<string, string>? data,
            int maxPerKey,
            int maxKeys,
            int maxPerValue,
            int maxBytes,
            out int bytes)
        {
            bytes = 0;
            if (data == null || !data.Any()) return true;
            int count = 0;
            foreach (var kvp in data)
            {
                if (kvp.Value == null) return false;
                var valueByteCount = SafeByteCount(kvp.Value);
                if (valueByteCount > maxPerValue) return false;
                var keyByteCount = SafeByteCount(kvp.Key);
                if (keyByteCount > maxPerKey) return false;
                int numBytes = valueByteCount + keyByteCount;
                count++;
                if (bytes + numBytes > maxBytes || count > maxKeys) return false;
                bytes += numBytes;
            }
            return true;
        }


        internal class DistinctResource<T>
        {
            public DistinctResource(string text, ResourceType resource, Func<T, Identity?> selector)
            {
                Text = text;
                Resource = resource;
                Selector = selector;
            }

            public string Text { get; }
            public ResourceType Resource { get; }
            public Func<T, Identity?> Selector { get; }
        }

        internal static (List<T>, List<CogniteError<T>>) CleanRequest<T>(
            DistinctResource<T>[] distinctResources,
            IEnumerable<T> items,
            Func<T, ResourceType?> verify,
            Action<T> sanitize,
            SanitationMode mode)
        {
            if (items == null) throw new ArgumentNullException(nameof(items));
            if (mode == SanitationMode.None) return (items.ToList(), new List<CogniteError<T>>());

            var result = new List<T>();
            var errors = new List<CogniteError<T>>();

            var existingResources = new HashSet<Identity>[distinctResources.Length];
            var duplicates = new List<(T Item, Identity Idt)>[distinctResources.Length];

            var bad = new List<(ResourceType Type, T Item)>();

            for (int i = 0; i < distinctResources.Length; i++)
            {
                existingResources[i] = new HashSet<Identity>();
                duplicates[i] = new List<(T Item, Identity Idt)>();
            }

            foreach (var item in items)
            {
                bool toAdd = true;
                if (mode == SanitationMode.Clean)
                {
                    sanitize(item);
                }
                var failedField = verify(item);
                if (failedField.HasValue)
                {
                    bad.Add((failedField.Value, item));
                    toAdd = false;
                }

                for (int i = 0; i < distinctResources.Length; i++)
                {
                    var value = distinctResources[i].Selector(item);
                    if (value != null && !existingResources[i].Add(value))
                    {
                        duplicates[i].Add((item, value));
                        toAdd = false;
                    }
                }

                if (toAdd)
                {
                    result.Add(item);
                }
            }

            for (int i = 0; i < duplicates.Length; i++)
            {
                if (duplicates[i].Any())
                {
                    errors.Add(new CogniteError<T>
                    {
                        Status = 409,
                        Message = distinctResources[i].Text,
                        Resource = distinctResources[i].Resource,
                        Type = ErrorType.ItemDuplicated,
                        Values = duplicates[i].Select(pair => pair.Idt).Distinct().ToArray(),
                        Skipped = duplicates[i].Select(pair => pair.Item)
                    });
                }
            }

            if (bad.Any())
            {
                errors.AddRange(bad.GroupBy(pair => pair.Type).Select(group => new CogniteError<T>
                {
                    Skipped = group.Select(pair => pair.Item).ToList(),
                    Resource = group.Key,
                    Type = ErrorType.SanitationFailed,
                    Status = 400
                }));
            }

            return (result, errors);
        }
    }
}
