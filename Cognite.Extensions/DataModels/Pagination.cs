using CogniteSdk;
using CogniteSdk.Beta.DataModels;
using CogniteSdk.Resources.Beta;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.Extensions.DataModels
{
    internal class QueryNode
    {
        public QueryNode? Parent { get; set; }
        public List<QueryNode> Children { get; } = new List<QueryNode>();
        public string ID { get; }

        public QueryNode(string id, QueryNode? parent)
        {
            ID = id;
            Parent = parent;
        }

        public IEnumerable<QueryNode> TraverseParents(bool includeSelf = false)
        {
            if (includeSelf) yield return this;
            if (Parent == null) yield break;
            foreach (var parent in Parent.TraverseParents(true))
            {
                yield return parent;
            }
        }

        public IEnumerable<QueryNode> TraverseChildren(bool includeSelf = false)
        {
            if (includeSelf) yield return this;
            foreach (var child in Children)
            {
                foreach (var childchild in child.TraverseChildren(true))
                {
                    yield return childchild;
                }
            }
        }
    }

    /// <summary>
    /// Composite cursor used when iterating 
    /// </summary>
    public class QueryCursor
    {
        internal Dictionary<string, QueryNode> Nodes { get; } = new Dictionary<string, QueryNode>();
        internal IEnumerable<QueryNode> Roots { get; }

        internal Dictionary<string, string>? LastCursors { get; set; }
        internal Dictionary<string, string>? Cursors { get; set; }

        internal HashSet<string> QueriesToNeverPaginate { get; }

        internal bool IsNew { get; }

        /// <summary>
        /// True if this cursor has finished reading.
        /// </summary>
        public bool Finished => !IsNew && (Cursors == null || Cursors.Count == 0);

        /// <summary>
        /// Create an empty query cursor from a query.
        /// </summary>
        /// <param name="query">DMS query to convert to a set of cursors.</param>
        /// <param name="queriesToNeverPaginate">Set of queries that shouldn't be paginated, for example if you know for sure that they
        /// will never return more results than will be returned in a single request.
        /// This is a workaround for DMS returning cursors even if there are no more results.</param>
        public QueryCursor(Query query, IEnumerable<string>? queriesToNeverPaginate)
        {
            if (query == null) throw new ArgumentNullException(nameof(query));

            IsNew = true;
            QueriesToNeverPaginate = new HashSet<string>(queriesToNeverPaginate ?? Enumerable.Empty<string>());

            var roots = new List<QueryNode>();
            foreach (var kvp in query.With)
            {
                var from = (kvp.Value as QueryNodeTableExpression)?.Nodes?.From ?? (kvp.Value as QueryEdgeTableExpression)?.Edges?.From;

                QueryNode? parent = null;
                if (from != null)
                {
                    if (!Nodes.TryGetValue(from, out parent))
                    {
                        Nodes[from] = parent = new QueryNode(from, null);
                    }
                }
                
                // The node may already be added if its child came earlier in the iteration
                if (Nodes.TryGetValue(kvp.Key, out var node))
                {
                    node.Parent = parent;
                }
                else
                {
                    Nodes[kvp.Key] = node = new QueryNode(kvp.Key, parent);
                }

                if (parent != null)
                {
                    parent.Children.Add(node);
                }
                else
                {
                    roots.Add(node);
                }
            }
            Roots = roots;
        }

        /// <summary>
        /// Construct a cursor from the set of returned cursors from a DMS query, and the QueryCursor used when retrieving
        /// those cursors.
        /// </summary>
        /// <param name="other"></param>
        /// <param name="cursors"></param>
        internal QueryCursor(QueryCursor other, Dictionary<string, string> cursors)
        {
            Nodes = other.Nodes;
            Roots = other.Roots;
            LastCursors = other.Cursors;
            QueriesToNeverPaginate = other.QueriesToNeverPaginate;
            Cursors = cursors;
        }

        /// <summary>
        /// Construct a cursor from another cursor, but without a set of returned cursors. This finalizes the cursor.
        /// </summary>
        /// <param name="other"></param>
        internal QueryCursor(QueryCursor other)
        {
            if (other == null) throw new ArgumentNullException(nameof(other));
            Nodes = other.Nodes;
            Roots = other.Roots;
            QueriesToNeverPaginate = other.QueriesToNeverPaginate;
        }

        /// <summary>
        /// Find all "leaves" in the query. This is done by traversing each root, finding all queries that
        /// have no children of their own that needs pagination.
        /// </summary>
        /// <param name="remainingQueries"></param>
        /// <returns></returns>
        internal IEnumerable<QueryNode> CollectCursorLeaves(HashSet<string> remainingQueries)
        {
            var res = new List<QueryNode>();
            foreach (var root in Roots)
            {
                FindCursorLeaves(root, remainingQueries, res);
            }
            return res;
        }

        private bool FindCursorLeaves(QueryNode node, HashSet<string> remainingQueries, List<QueryNode> results)
        {
            if (Cursors == null) throw new InvalidOperationException("Attempted to iterate using a finished cursor");
            if (!remainingQueries.Contains(node.ID)) return false;
            bool anyChild = false;

            foreach (var child in node.Children)
            {
                anyChild |= FindCursorLeaves(child, remainingQueries, results);
            }

            if (anyChild) return true;

            if (!QueriesToNeverPaginate.Contains(node.ID) && Cursors.ContainsKey(node.ID))
            {
                results.Add(node);
                return true;
            }

            return false;
        }
    }

    /// <summary>
    /// Result of an iteration of a DMS query
    /// </summary>
    /// <typeparam name="T">Type of node or edge properties</typeparam>
    public class DMSQueryResult<T>
    {
        /// <summary>
        /// Items returned by this iteration of the query, grouped by subquery.
        /// This may be empty, if there was no valid query to be made.
        /// </summary>
        public Dictionary<string, IEnumerable<BaseInstance<T>>> Items { get; }
        /// <summary>
        /// Cursor for pagination.
        /// </summary>
        public QueryCursor? Cursor { get; }

        internal DMSQueryResult(Dictionary<string, IEnumerable<BaseInstance<T>>> items, QueryCursor cursor)
        {
            Items = items;
            Cursor = cursor == null || cursor.Finished ? null : cursor;
        }
    }

    internal static class DataModelPagination
    {
        /*
        This is a tool for paginating over DMS queries, which is generally very hard to do.
        Essentially, what we are doing is ordinally iterating over the cursors returned from DMS.
        
        So, imagine we are making a query with four subqueries, [A, B, C, D]. C depends on B, B depends on A, A and D depend on nothing.

        The first query returns cursors for all four
        [A1, B1, C1, D1]

        If we were to simply make a query with [A1, B1, C1, D1] given as cursors, we would not get the result we expected. Since
        B depends on A, we would instead get the result from B, offset with B1, using filters based on A, offset by A1. Instead what we do
        is we step each "leaf" cursor forward one:

        [null, null, C1, D1] which returns [A1, B1, C2, null]. Note how we get the same results for A and B, which is inefficient, but hard
        to work around at the moment. There are maybe some ways we can do this in the future, but last I checked there were issues in DMS
        preventing it.

        Now, D is completely exhausted, so we no longer need it. The next query we make is [null, null, C2], leaving D out completely.
        This returns [A1, B1, null]. C is exhausted, so now we can step B forward, querying [null, B1, null]. Note how we are including C here,
        since it is a child of B. This will yield a different subset of C, since it depends on B.

        This returns [A1, B2, C1], so next we are iterating on C again, making the requests:
        [null, B1, C1] -> [A1, B2, null]
        [null, B2, null] -> [A1, null, C1]
        [null, B2, C1] -> [A1, null, null]
        [A1, null, null] -> [null, B1, C1]
        [A1, null, C1] -> [null, B1, null]
        [A1, B1, null] -> [null, null, null]

        We keep going until the cursors are completely exhausted.

        The optimization we might be able to make here is we could modify C so that the dependence on B is satisified through
        a filter on externalId, startNode, or endNode, or something along those lines.

        Do also note that this method is a bit limited, and only really handles `from` relations. The SDK doesn't currently support
        union/intersect type queries, but if you were using those this would get a _lot_ more complicated, since then this can be
        a lot more graphy instead of a pure tree.
         */
        public static async Task<DMSQueryResult<T>> QueryPaginatedIter<T>(this DataModelsResource resource, Query query, QueryCursor cursor, CancellationToken token)
        {
            var existingQueries = new HashSet<string>(query.With.Keys);

            Query innerQuery;
            if (cursor.IsNew)
            {
                innerQuery = query;
            }
            else
            {
                if (cursor.Finished) throw new InvalidOperationException("Attempted to query using a finished cursor");
                innerQuery = new Query
                {
                    Parameters = query.Parameters,
                    With = new Dictionary<string, IQueryTableExpression>(),
                    Select = new Dictionary<string, SelectExpression>(),
                    Cursors = new Dictionary<string, string>()
                };

                var leaves = cursor.CollectCursorLeaves(existingQueries);

                // If there are no leaves, it means we've reached the end of the query.
                // This can happen if the remaining cursors are all excluded.
                if (!leaves.Any()) return new DMSQueryResult<T>(new Dictionary<string, IEnumerable<BaseInstance<T>>>(), new QueryCursor(cursor));
                foreach (var leaf in leaves)
                {
                    innerQuery.Cursors[leaf.ID] = cursor.Cursors![leaf.ID];
                    innerQuery.With[leaf.ID] = query.With[leaf.ID];
                    innerQuery.Select[leaf.ID] = query.Select[leaf.ID];

                    // Include any child of the leaf, with no set cursor.
                    foreach (var child in leaf.TraverseChildren())
                    {
                        innerQuery.With[child.ID] = query.With[child.ID];
                        innerQuery.Select[child.ID] = query.Select[child.ID];
                    }

                    // Include parents, using the previous cursor if it exists, else null.
                    foreach (var parent in leaf.TraverseParents())
                    {
                        if (cursor.LastCursors != null && cursor.LastCursors.TryGetValue(parent.ID, out var pCursor))
                        {
                            innerQuery.Cursors[parent.ID] = pCursor;
                        }
                        innerQuery.With[parent.ID] = query.With[parent.ID];
                        innerQuery.Select[parent.ID] = query.Select[parent.ID];
                    }
                }
            }

            var result = await resource.QueryInstances<T>(innerQuery, token).ConfigureAwait(false);
            return new DMSQueryResult<T>(result.Items, new QueryCursor(cursor, result.NextCursor));
        }
    }
}
