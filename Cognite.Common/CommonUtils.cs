using System;
using System.Collections.Generic;
using System.Linq;

namespace Cognite.Extractor.Common
{
    /// <summary>
    /// Various utility functions
    /// </summary>
    public static class CommonUtils
    {
        /// <summary>
        /// Trim this string object to null.
        /// </summary>
        /// <returns>A string without leading or trailing whitespace, or null.</returns>
        public static string TrimToNull(this string @this)
        {
            string s = @this?.Trim() ?? null;
            return string.IsNullOrEmpty(s) ? null : s;
        }

        /// <summary>
        /// Try to get the value associated with the provided <paramref name="key"/>.
        /// If it is not found, returns <paramref name="defaultValue"/>
        /// </summary>
        /// <param name="dict">This dictionary</param>
        /// <param name="key">Key to search</param>
        /// <param name="defaultValue">Default value to return</param>
        /// <returns>Value or default</returns>
        public static TValue GetValue<TKey,TValue>(this IDictionary<TKey, TValue> dict, TKey key, TValue defaultValue)
        {
            return dict.TryGetValue(key, out TValue value) ? value : defaultValue;
        }

        /// <summary>
        /// Get the minimum and maximum timestamp in a list, using <paramref name="tsSelector"/> to get the timestamp for
        /// each item
        /// </summary>
        /// <typeparam name="T">Input element type</typeparam>
        /// <param name="items">Input enumerable</param>
        /// <param name="tsSelector">Function to get timestamp for each element</param>
        /// <returns>A tuple (Min, Max)</returns>
        public static (DateTime Min, DateTime Max) MinMax<T>(this IEnumerable<T> items, Func<T, DateTime> tsSelector)
        {
            DateTime min = DateTime.MaxValue;
            DateTime max = DateTime.MinValue;
            foreach (var item in items)
            {
                DateTime ts = tsSelector(item);
                if (ts < min) min = ts;
                if (ts > max) max = ts;
            }
            return (min, max);
        }

        /// <summary>
        /// Group input by some granularity, so that the result is a list of lists where
        /// each inner list contains only elements within <paramref name="granularity"/> time of each other.
        /// </summary>
        /// <typeparam name="T">Input element type</typeparam>
        /// <param name="items">Input enumerable</param>
        /// <param name="granularity">Granularity to chunk by</param>
        /// <param name="tsSelector">Function to get timestamp for each element</param>
        /// <param name="maxLength">Maximum number of elements in each inner list</param>
        /// <returns>A list of lists where each inner list contains only elements within <paramref name="granularity"/>
        /// time of each other.</returns>
        public static IEnumerable<IEnumerable<T>> GroupByTimeGranularity<T>(
            this IEnumerable<T> items,
            TimeSpan granularity,
            Func<T, DateTime> tsSelector,
            int maxLength)
        {
            return granularity == TimeSpan.Zero
                ? items.Select(item => new[] { item })
                : items.GroupBy(item => tsSelector(item).Ticks / granularity.Ticks)
                    .SelectMany(group => group.ChunkBy(maxLength));
        }

        /// <summary>
        /// Returns elements of the source enumerable, where all elments have distinct results of <paramref name="selector"/>.
        /// If there are duplicates, only the first occurence in the input enumerable will be used.
        /// </summary>
        /// <param name="source">Input enumerable</param>
        /// <param name="selector">Function to generate keys</param>
        /// <param name="comparer">Optional element comparer</param>
        /// <returns>Entries of the source enumerable, where all elements have distinct results of <paramref name="selector"/></returns>
        public static IEnumerable<TSource> DistinctBy<TSource, TKey>(this IEnumerable<TSource> source,
            Func<TSource, TKey> selector, IEqualityComparer<TKey> comparer = null)
        {
            if (source == null) throw new ArgumentNullException(nameof(source));
            HashSet<TKey> seenKeys = new HashSet<TKey>(comparer);
            foreach (var elem in source)
            {
                if (seenKeys.Add(selector(elem)))
                {
                    yield return elem;
                }
            }
        }
    }
}
