using CogniteSdk.Beta.DataModels;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Runtime.CompilerServices;
using CogniteSdk.Resources.Beta;
using System.Linq;
using System.Threading.Tasks;

namespace Cognite.Extensions.DataModels
{
    /// <summary>
    /// Extensions for Data Models.
    /// </summary>
    public static class DataModelExtensions
    {
        /// <summary>
        /// Load a list of instances of type <typeparamref name="T"/>, containing data of type <typeparamref name="R"/>.
        /// The filter <paramref name="filter"/> will be applied.
        /// 
        /// This follows cursors automatically.
        /// </summary>
        /// <typeparam name="T">Type of response, typically either nodes or edges, but it is possible to use BaseInstance from the SDK to retrieve either.</typeparam>
        /// <typeparam name="R">Type of data in the response. You can use JsonNode or JsonElement to retrieve anything.</typeparam>
        /// <param name="resource">Query resource</param>
        /// <param name="filter">Initial filter</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>Async iterator of returned instances.</returns>
        public static async IAsyncEnumerable<T> LoadInstances<T, R>(this DataModelsResource resource, InstancesFilter filter, [EnumeratorCancellation] CancellationToken token) where T : BaseInstance<R>
        {
            if (filter == null) throw new ArgumentNullException(nameof(filter));
            if (resource == null) throw new ArgumentNullException(nameof(resource));
            string? cursor = null;

            do
            {
                filter.Cursor = cursor;
                var res = await resource.FilterInstances<R>(filter, token).ConfigureAwait(false);

                foreach (var it in res.Items.OfType<T>())
                {
                    yield return it;
                }

                cursor = res.NextCursor;
            } while (cursor != null);
        }

        /// <summary>
        /// Return a list of nodes containing data of type <typeparamref name="T"/>.
        /// </summary>
        /// <typeparam name="T">Type of data in response. You can use JsonNode or JsonElement to retrieve anything.</typeparam>
        /// <param name="resource">Query resource</param>
        /// <param name="filter">Initial filter</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>Async iterator of returned nodes.</returns>
        public static IAsyncEnumerable<Node<T>> LoadNodes<T>(this DataModelsResource resource, InstancesFilter filter, CancellationToken token)
        {
            return LoadInstances<Node<T>, T>(resource, filter, token);
        }

        /// <summary>
        /// Return a list of edges containing data of type <typeparamref name="T"/>.
        /// </summary>
        /// <typeparam name="T">Type of data in response. You can use JsonNode or JsonElement to retrieve anything.</typeparam>
        /// <param name="resource">Query resource</param>
        /// <param name="filter">Initial filter</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>Async iterator of returned edges.</returns>
        public static IAsyncEnumerable<Edge<T>> LoadEdges<T>(this DataModelsResource resource, InstancesFilter filter, CancellationToken token)
        {
            return LoadInstances<Edge<T>, T>(resource, filter, token);
        }

        /// <summary>
        /// Query DMS and follow the returned cursors. This variant will only call DMS once.
        /// 
        /// DMS cursors are per-query, but since queries may be dependent on each other, it may be necessary to keep one cursor
        /// fixed while following another. This method handles that complicated iteration for queries that are one or more trees.
        /// This does not support queries that are more complex graphs.
        /// 
        /// The query may change between calls, but each new query must be stricter than any previous query.
        /// </summary>
        /// <typeparam name="T">Type of returned instance data</typeparam>
        /// <param name="resource">Query resource</param>
        /// <param name="query">Query to use.</param>
        /// <param name="cursor">Cursor</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>A result containing data and an optional new cursor.</returns>
        public static async Task<DMSQueryResult<T>> QueryPaginated<T>(this DataModelsResource resource, Query query, QueryCursor cursor, CancellationToken token)
        {
            if (resource == null) throw new ArgumentNullException(nameof(resource));
            if (query == null) throw new ArgumentNullException(nameof(query));
            if (cursor == null) throw new ArgumentNullException(nameof(cursor));
            return await resource.QueryPaginatedIter<T>(query, cursor, token).ConfigureAwait(false);
        }

        /// <summary>
        /// Query DMS and follow the returned cursors. This variant will follow the cursors until they are exhausted.
        /// 
        /// DMS cursors are per-query, but since queries may be dependent on each other, it may be necessary to keep one cursor
        /// fixed while following another. This method handles that complicated iteration for queries that are one or more trees.
        /// This does not support queries that are more complex graphs.
        /// </summary>
        /// <typeparam name="T">Type of returned instance data</typeparam>
        /// <param name="resource">Query resource</param>
        /// <param name="query">Query to use</param>
        /// <param name="queriesToNeverPaginate">List of queries that should never be paginated,
        /// because the caller knows that they will never return values more than once. This is a workaround for DMS always returning
        /// cursors, even if there are no more results.</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>A result dict mapping from query ID to list of instances.</returns>
        public static async Task<Dictionary<string, List<BaseInstance<T>>>> QueryPaginated<T>(this DataModelsResource resource, Query query, IEnumerable<string>? queriesToNeverPaginate, CancellationToken token)
        {
            var cursor = new QueryCursor(query, queriesToNeverPaginate);
            var result = new Dictionary<string, List<BaseInstance<T>>>();
            do
            {
                var res = await resource.QueryPaginated<T>(query, cursor, token).ConfigureAwait(false);
                cursor = res.Cursor;
                foreach (var kvp in res.Items)
                {
                    if (!result.TryGetValue(kvp.Key, out var items))
                    {
                        result[kvp.Key] = items = new List<BaseInstance<T>>();
                    }
                    items.AddRange(kvp.Value);
                }
            } while (cursor != null && !cursor.Finished);

            return result;
        }
    }
}
