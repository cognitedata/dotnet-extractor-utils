using CogniteSdk.Beta.DataModels;
using System;
using System.Collections.Generic;

namespace Cognite.Extensions.DataModels.QueryBuilder
{
    /// <summary>
    /// Utility for building DMS queries with a fluent API
    /// </summary>
    public class QueryBuilder
    {
        private readonly Dictionary<string, QueryBuilderItem> _queries = new Dictionary<string, QueryBuilderItem>();
        private readonly Dictionary<string, IDMSValue> _parameters = new Dictionary<string, IDMSValue>();

        /// <summary>
        /// Add the sub-query <paramref name="builder"/> to the query.
        /// </summary>
        /// <typeparam name="T">Type of builder, either <see cref="NodeQueryBuilderItem"/> or <see cref="EdgeQueryBuilderItem"/>.</typeparam>
        /// <param name="name">Name of the query</param>
        /// <param name="builder">Builder for sub-query to add.</param>
        public QueryBuilder WithQuery<T>(string name, T builder) where T : QueryBuilderItem
        {
            _queries.Add(name, builder);
            return this;
        }

        /// <summary>
        /// Build a DMS query
        /// </summary>
        /// <returns>A finished DMS query</returns>
        public Query Build()
        {
            var queries = new Dictionary<string, IQueryTableExpression>();
            var selects = new Dictionary<string, SelectExpression>();
            foreach (var kvp in _queries)
            {
                queries.Add(kvp.Key, kvp.Value.Build());
                selects.Add(kvp.Key, kvp.Value.BuildSelect());
            }
            return new Query
            {
                Select = selects,
                With = queries,
                Parameters = _parameters
            };
        }

        /// <summary>
        /// Add a parameter. Parameters are shared accross all sub-queries, and can make
        /// complex queries more efficient.
        /// </summary>
        /// <param name="name">Name of the parameter</param>
        /// <param name="value">Value of the parameter</param>
        public QueryBuilder WithParameter(string name, Value value)
        {
            if (value == null) throw new ArgumentNullException(nameof(value));
            _parameters.Add(name, value.Val);
            return this;
        }
    }
    /// <summary>
    /// Type of query.
    /// </summary>
    public enum QueryType
    {
        /// <summary>
        /// Query for nodes
        /// </summary>
        Nodes,
        /// <summary>
        /// Query for edges
        /// </summary>
        Edges
    }

    /// <summary>
    /// Wrapper around values in DMS queries.
    /// </summary>
    public class Value
    {
        private IDMSValue _inner;
        private Value(IDMSValue value) { _inner = value; }

        /// <summary>
        /// A raw value, using some primitive type, or a complex type which will be converted to JSON.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="item"></param>
        public static Value Raw<T>(T item)
        {
            return new Value(new RawPropertyValue<T>(item));
        }

        /// <summary>
        /// A reference to a parameter. This must be added using <see cref="QueryBuilder.WithParameter(string, Value)"/>
        /// </summary>
        /// <param name="param">Name of the parameter to reference</param>
        public static Value Parameter(string param)
        {
            return new Value(new ParameterizedPropertyValue { Parameter = param });
        }

        /// <summary>
        /// A reference to a different property.
        /// </summary>
        /// <param name="property">Property path, for example "node", "space"</param>
        /// <returns></returns>
        public static Value Reference(params string[] property)
        {
            return new Value(new ReferencedPropertyValue(property));
        }

        /// <summary>
        /// Implicitly create a value from a long
        /// </summary>
        /// <param name="v">Value</param>
        public static implicit operator Value(long v) => Raw(v);
        /// <summary>
        /// Implicitly create a value from an int
        /// </summary>
        /// <param name="v">Value</param>
        public static implicit operator Value(int v) => Raw(v);
        /// <summary>
        /// Implicitly create a value from a string
        /// </summary>
        /// <param name="v">Value</param>
        public static implicit operator Value(string v) => Raw(v);
        /// <summary>
        /// Implicitly create a value from a double
        /// </summary>
        /// <param name="v">Value</param>
        public static implicit operator Value(double v) => Raw(v);
        /// <summary>
        /// Implicitly create a value from a float
        /// </summary>
        /// <param name="v">Value</param>
        public static implicit operator Value(float v) => Raw(v);
        /// <summary>
        /// Implicitly create a value from a bool
        /// </summary>
        /// <param name="v">Value</param>
        public static implicit operator Value(bool v) => Raw(v);

        /// <summary>
        /// Inner value.
        /// </summary>
        public IDMSValue Val => _inner;
    }

    /// <summary>
    /// Base class for query builder items. Subclassed to either nodes or edges.
    /// </summary>
    public abstract class QueryBuilderItem
    {
        /// <summary>
        /// Collection of selects, a map from (space, view, property) to a select identifier.
        /// The dictionary is for uniqueness.
        /// </summary>
        protected Dictionary<(string, string, string), SelectSource> Selects { get; } = new Dictionary<(string, string, string), SelectSource>();

        /// <summary>
        /// Create a select expression from added selects.
        /// </summary>
        public SelectExpression BuildSelect()
        {
            return new SelectExpression
            {
                Sources = Selects.Values,
            };
        }

        /// <summary>
        /// Create a query expression from this.
        /// </summary>
        public abstract IQueryTableExpression Build();
    }
}
