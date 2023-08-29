using CogniteSdk.Beta.DataModels;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Cognite.Extensions.DataModels.QueryBuilder
{
    /// <summary>
    /// Builder item for edge queries.
    /// </summary>
    public class EdgeQueryBuilderItem : QueryBuilderItem
    {
        private IDMSFilter? _nodeFilter;
        private IDMSFilter? _edgeFilter;
        private IDMSFilter? _terminationFilter;
        private string? _from;
        private bool _outwards = true;
        private int _maxDistance;
        private int? _limit;

        /// <summary>
        /// Add a node filter. All nodes returned will match this filter.
        /// </summary>
        /// <param name="builder">Filter builder.</param>
        public EdgeQueryBuilderItem WithNodeFilter(IDMSFilter builder)
        {
            _nodeFilter = builder;
            return this;
        }

        /// <summary>
        /// Add an edge filter. All traversed edges will match this filter.
        /// </summary>
        /// <param name="builder">Filter builder.</param>
        public EdgeQueryBuilderItem WithEdgeFilter(IDMSFilter builder)
        {
            _edgeFilter = builder;
            return this;
        }

        /// <summary>
        /// Add a termination filter. When nodes match this filter, traversal will not continue on edges from those nodes.
        /// </summary>
        /// <param name="builder">Filter builder.</param>
        public EdgeQueryBuilderItem WithTerminationFilter(IDMSFilter builder)
        {
            _terminationFilter = builder;
            return this;
        }

        /// <summary>
        /// Chain this query from the query with name given by <paramref name="from"/>
        /// </summary>
        /// <param name="from">Query to chain this from</param>
        public EdgeQueryBuilderItem WithFrom(string from)
        {
            _from = from;
            return this;
        }

        /// <summary>
        /// Set whether this query goes outwards or inwards, default is true.
        /// </summary>
        /// <param name="outwards">Set whether this query goes inwards or outwards.</param>
        /// <returns></returns>
        public EdgeQueryBuilderItem Outwards(bool outwards)
        {
            _outwards = outwards;
            return this;
        }

        /// <summary>
        /// Set the maximum number of edges out from the start the query will maximally traverse.
        /// </summary>
        /// <param name="hops">Number of hops</param>
        public EdgeQueryBuilderItem WithMaxDistance(int hops)
        {
            _maxDistance = hops;
            return this;
        }

        /// <summary>
        /// Select a set of properties from a view.
        /// </summary>
        /// <param name="view">View identifier</param>
        /// <param name="properties">Properties from <paramref name="view"/> to select</param>
        public EdgeQueryBuilderItem WithSelectFrom(ViewIdentifier view, params string[] properties)
        {
            if (view == null) throw new ArgumentNullException(nameof(view));
            if (Selects.TryGetValue((view.Space, view.ExternalId, view.Version), out var oldSelect))
            {
                oldSelect.Properties = new HashSet<string>(oldSelect.Properties.Concat(properties));
                return this;
            }

            Selects.Add((view.Space, view.ExternalId, view.Version), new SelectSource
            {
                Source = view,
                Properties = properties
            });
            return this;
        }

        /// <summary>
        /// Add a limit to the maximum number of returned edges.
        /// </summary>
        /// <param name="limit">Maximum number of returned edges.</param>
        /// <returns></returns>
        public EdgeQueryBuilderItem WithLimit(int limit)
        {
            _limit = limit;
            return this;
        }

        /// <inheritdoc />
        public override IQueryTableExpression Build()
        {
            return new QueryEdgeTableExpression
            {
                Edges = new QueryEdges
                {
                    Filter = _edgeFilter,
                    NodeFilter = _nodeFilter,
                    From = _from,
                    TerminationFilter = _terminationFilter,
                    Direction = _outwards ? ConnectionDirection.outwards : ConnectionDirection.inwards,
                    MaxDistance = _maxDistance,
                },
                Limit = _limit
            };
        }
    }
}
