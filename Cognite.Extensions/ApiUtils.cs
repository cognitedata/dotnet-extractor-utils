using CogniteSdk;
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.Extensions
{
    /// <summary>
    /// Utility methods for the CogniteSDK.
    /// </summary>
    public class ApiUtils
    {
        /// <summary>
        /// Return an async enumerator for the specified method, following the cursor to exhaustion.
        /// <paramref name="query"/> is reused, so a filter can be passed to it.
        /// </summary>
        /// <typeparam name="TResult">The actual result type</typeparam>
        /// <typeparam name="TResponse">The response type, a subtype of ItemsWithCursor</typeparam>
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
    }
}
