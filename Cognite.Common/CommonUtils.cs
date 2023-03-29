using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

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
        public static string? TrimToNull(this string? @this)
        {
            string? s = @this?.Trim() ?? null;
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
        /// Get the minimum and maximum value in a list, using <paramref name="selector"/> to get the value for
        /// each item. Throws an exception if no elements are present.
        /// </summary>
        /// <typeparam name="T">Input element type</typeparam>
        /// <typeparam name="R">Comparable element</typeparam>
        /// <param name="items">Input enumerable</param>
        /// <param name="selector">Function to get value for each element</param>
        /// <returns>A tuple (Min, Max)</returns>
        public static (R Min, R Max) MinMax<T, R>(this IEnumerable<T> items, Func<T, R> selector) where R : IComparable
        {
            if (selector == null)
            {
                throw new ArgumentNullException(nameof(selector));
            }
            R min = default;
            R max = default;
            bool hasValue = false;
            foreach (var item in items)
            {
                R val = selector(item);
                if (!hasValue)
                {
                    min = max = val;
                    hasValue = true;
                    continue;
                }
                else if (val.CompareTo(min) < 0) min = val;
                if (val.CompareTo(max) > 0) max = val;
            }
            if (!hasValue) throw new InvalidOperationException("Enumerable is empty");
            return (min!, max!);
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
            Func<TSource, TKey> selector, IEqualityComparer<TKey>? comparer = null)
        {
            if (selector == null)
            {
                throw new ArgumentNullException(nameof(selector));
            }
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

        /// <summary>
        /// Convenient method to efficiently wait for a wait handle and cancellation token with timeout
        /// asynchronously.
        /// </summary>
        /// <param name="handle">WaitHandle to wait for</param>
        /// <param name="timeout">Wait timeout</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>True if wait handle or cancellation token was triggered, false otherwise</returns>
        public static Task<bool> WaitAsync(WaitHandle handle, TimeSpan timeout, CancellationToken token)
        {
            var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var registeredHandle = ThreadPool.RegisterWaitForSingleObject(
                handle,
                (state, timedOut) => tcs.TrySetResult(!timedOut),
                null,
                timeout,
                true);
            var tokenRegistration = token.Register(
                state => ((TaskCompletionSource<bool>?)state)?.TrySetCanceled(),
                tcs);
            var task = tcs.Task;
            tcs.Task.ContinueWith(t =>
            {
                if (registeredHandle != null) registeredHandle.Unregister(null);
                tokenRegistration.Dispose();
            }, TaskScheduler.Current);
            return task;
        }
    }
}
