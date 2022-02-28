using Cognite.Extractor.Common;
using CogniteSdk;
using CogniteSdk.Types.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.Extensions
{
    /// <summary>
    /// Utility methods for the CogniteSDK.
    /// </summary>
    public static class ApiUtils
    {
        /// <summary>
        /// Return an async enumerator for the specified method, following the cursor to exhaustion.
        /// <paramref name="query"/> is reused, so a filter can be passed to it.
        /// </summary>
        /// <typeparam name="TResult">The actual result type</typeparam>
        /// <typeparam name="TQuery">Query type, must be a subtype of CursorQueryBase</typeparam>
        /// <param name="query">Initial query. If this is null, a default instance is created</param>
        /// <param name="method">Method to call to fetch the next chunk.</param>
        /// <param name="token">Optional cancellation token.</param>
        /// <returns>An asynchronous enumerable iterating over the full result collection.
        /// New queries to CDF will be made as the previous results are exhausted.</returns>
        public static async IAsyncEnumerable<TResult> FollowCursor<TResult, TQuery>(
            TQuery? query,
            Func<TQuery, CancellationToken, Task<IItemsWithCursor<TResult>>> method,
            [EnumeratorCancellation] CancellationToken token)
            where TQuery : CursorQueryBase
        {
            if (method == null) throw new ArgumentNullException(nameof(method));
            if (query == null) query = Activator.CreateInstance<TQuery>();

            string? cursor;
            do
            {
                var read = await method(query, token).ConfigureAwait(false);
                cursor = read.NextCursor;
                foreach (var item in read.Items)
                {
                    yield return item;
                }
                query.Cursor = cursor;
            } while (cursor != null && !token.IsCancellationRequested);
        }

        /// <summary>
        /// Utility method to convert an async enumerable to a synchronous collection.
        /// This will exhaust the enumerable.
        /// </summary>
        /// <typeparam name="T">Type of enumerable</typeparam>
        /// <param name="items">Items to exhaust.</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>The contents of the enumerable in a synchronous collection.</returns>
        public static async Task<IEnumerable<T>> ToListAsync<T>(this IAsyncEnumerable<T> items, CancellationToken token)
        {
            var results = new List<T>();
            await foreach (var item in items.WithCancellation(token).ConfigureAwait(false))
            {
                results.Add(item);
            }
            return results;
        }

        /// <summary>
        /// Retrieve all elements from a resource that allows reading with partitions.
        /// </summary>
        /// <typeparam name="TResult">Result type</typeparam>
        /// <typeparam name="TQuery">Type of query, must be both a CursorQueryBase and IPartitionedQuery</typeparam>
        /// <param name="queryGenerator">Function to return the query. Needs to return separate queries since we need
        /// one instance per parallel request, as they will be used at the same time.</param>
        /// <param name="method">SDK method accepting the query</param>
        /// <param name="numPartitions">Number of partitions. Must be a number greater than 0.</param>
        /// <param name="parallelism">Maximum number of parallel requests.</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>The full list of retrieved items, in no particular order</returns>
        public static async Task<IEnumerable<TResult>> ReadAllPartitioned<TResult, TQuery>(
            Func<TQuery> queryGenerator,
            Func<TQuery, CancellationToken, Task<IItemsWithCursor<TResult>>> method,
            int numPartitions,
            int parallelism,
            CancellationToken token)
            where TQuery : CursorQueryBase, IPartitionedQuery
        {
            if (numPartitions <= 0) throw new ArgumentOutOfRangeException(nameof(numPartitions));

            var results = new IEnumerable<TResult>[numPartitions];

            var generators = Enumerable.Range(1, numPartitions)
                .Select<int, Func<Task>>(partition => async () =>
                {
                    var query = queryGenerator();
                    query.Partition = $"{partition}/{numPartitions}";
                    var result = await FollowCursor(query, method, token).ToListAsync(token).ConfigureAwait(false);
                    results[partition - 1] = result;
                });

            await generators.RunThrottled(parallelism, token).ConfigureAwait(false);

            return results.SelectMany(res => res);
        }
    }
}
