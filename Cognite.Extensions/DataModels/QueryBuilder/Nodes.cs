using CogniteSdk.DataModels;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Cognite.Extensions.DataModels.QueryBuilder
{
    /// <summary>
    /// Builder item for node queries.
    /// </summary>
    public class NodeQueryBuilderItem : QueryBuilderItem
    {
        private IDMSFilter? _filter;
        private QueryViewReference? _through;
        private string? _from;
        private int? _limit;

        /// <summary>
        /// Add a filter. All returned nodes will match this filter.
        /// </summary>
        /// <param name="filter"></param>
        /// <returns></returns>
        public NodeQueryBuilderItem WithFilter(IDMSFilter filter)
        {
            _filter = filter;
            return this;
        }

        /// <summary>
        /// Query through a direct relation in another query.
        /// </summary>
        /// <param name="through"></param>
        /// <returns></returns>
        public NodeQueryBuilderItem WithThrough(QueryViewReference through)
        {
            _through = through;
            return this;
        }

        /// <summary>
        /// Query from an edge query.
        /// </summary>
        /// <param name="from">Name of edge query to start from.</param>
        public NodeQueryBuilderItem WithFrom(string from)
        {
            _from = from;
            return this;
        }

        /// <summary>
        /// Select a set of properties from a view.
        /// </summary>
        /// <param name="view">View identifier</param>
        /// <param name="properties">Properties from <paramref name="view"/> to select</param>
        public NodeQueryBuilderItem WithSelectFrom(ViewIdentifier view, params string[] properties)
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
        /// Set a limit for the maximum number of retrieved nodes.
        /// </summary>
        /// <param name="limit">Maximum number of retrieved nodes.</param>
        public NodeQueryBuilderItem WithLimit(int limit)
        {
            _limit = limit;
            return this;
        }

        /// <inheritdoc />
        public override IQueryTableExpression Build()
        {
            return new QueryNodeTableExpression
            {
                Nodes = new QueryNodes
                {
                    Filter = _filter,
                    From = _from,
                    Through = _through,
                },
                Limit = _limit
            };
        }
    }
}
