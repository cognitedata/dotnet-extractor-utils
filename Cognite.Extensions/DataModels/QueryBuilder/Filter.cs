using CogniteSdk.Beta.DataModels;
using Oryx.Cognite;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Cognite.Extensions.DataModels.QueryBuilder
{
    /// <summary>
    /// Fluent API for building DMS filters.
    /// </summary>
    public static class Filter
    {
        /// <summary>
        /// And' a list of filters.
        /// </summary>
        /// <param name="filters">Filters to And'</param>
        /// <returns>The given list of filters And'ed together.</returns>
        public static IDMSFilter And(params IDMSFilter[] filters)
        {
            if (filters == null) throw new ArgumentNullException(nameof(filters));
            if (!filters.Any()) throw new ArgumentException("Must provide at least 1 filter");
            if (filters.Length == 1)
            {
                return filters[0];
            }
            return new AndFilter
            {
                And = filters
            };
        }

        /// <summary>
        /// Or' a list of filters.
        /// </summary>
        /// <param name="filters">Filters to Or'</param>
        /// <returns>The given list of filters Or'ed together.</returns>
        public static IDMSFilter Or(params IDMSFilter[] filters)
        {
            if (filters == null) throw new ArgumentNullException(nameof(filters));
            if (!filters.Any()) throw new ArgumentException("Must provide at least 1 filter");
            if (filters.Length == 1)
            {
                return filters[0];
            }
            return new OrFilter
            {
                Or = filters
            };
        }

        /// <summary>
        /// Return the inverse of <paramref name="filter"/>
        /// </summary>
        /// <param name="filter">Filter to invert.</param>
        /// <returns>The inverse of <paramref name="filter"/></returns>
        public static IDMSFilter Not(this IDMSFilter filter)
        {
            if (filter == null) throw new ArgumentNullException(nameof(filter));
            if (filter is NotFilter notFilter)
            {
                return notFilter.Not;
            }
            return new NotFilter { Not = filter };
        }

        /// <summary>
        /// Return a filter for <paramref name="value"/> == <paramref name="property"/>
        /// </summary>
        /// <param name="value">Value to compare</param>
        /// <param name="property">Property to compare</param>
        public static IDMSFilter Equal(Value value, params string[] property)
        {
            if (value == null) throw new ArgumentNullException(nameof(value));
            return new EqualsFilter
            {
                Property = property,
                Value = value.Val
            };
        }

        /// <summary>
        /// Return a filter for  <paramref name="property"/> IN <paramref name="values"/>
        /// </summary>
        /// <param name="values">Value to compare</param>
        /// <param name="property">Property to compare</param>
        public static IDMSFilter In(IEnumerable<Value> values, params string[] property)
        {
            if (values == null) throw new ArgumentNullException(nameof(values));
            return new InFilter
            {
                Property = property,
                Values = values.Select(v => v.Val)
            };
        }

        /// <summary>
        /// Return a filter for <paramref name="property"/> between <paramref name="gt"/> or <paramref name="gte"/> and <paramref name="lt"/> or <paramref name="lte"/>.
        /// </summary>
        /// <param name="gt">Property must be strictly greater than this</param>
        /// <param name="gte">Property must be greater than or equal to this</param>
        /// <param name="lt">Property must be strictly less than this</param>
        /// <param name="lte">Property must be less than or equal to this</param>
        /// <param name="property">Property to compare</param>
        public static IDMSFilter Range(Value? gt, Value? gte, Value? lt, Value? lte, params string[] property)
        {
            return new RangeFilter
            {
                Property = property,
                GreaterThan = gt?.Val,
                GreaterThanEqual = gte?.Val,
                LessThan = lt?.Val,
                LessThanEqual = lte?.Val,
            };
        }

        /// <summary>
        /// Return a filter for <paramref name="value"/> being a prefix of <paramref name="property"/>.
        /// This only works if <paramref name="property"/> is a single string value.
        /// </summary>
        /// <param name="value">Value to compare</param>
        /// <param name="property">Property to compare</param>
        public static IDMSFilter Prefix(Value value, params string[] property)
        {
            if (value == null) throw new ArgumentNullException(nameof(value));
            return new PrefixFilter
            {
                Property = property,
                Value = value.Val
            };
        }

        /// <summary>
        /// Return a filter for <paramref name="property"/> existing.
        /// </summary>
        /// <param name="property">Property to compare</param>
        public static IDMSFilter Exists(params string[] property)
        {
            return new ExistsFilter
            {
                Property = property,
            };
        }

        /// <summary>
        /// Return a filter for any value of <paramref name="property"/> being present in <paramref name="values"/>.
        /// </summary>
        /// <param name="values">Values to compare</param>
        /// <param name="property">Property to compare</param>
        public static IDMSFilter ContainsAny(IEnumerable<Value> values, params string[] property)
        {
            if (values == null) throw new ArgumentNullException(nameof(values));
            return new ContainsAnyFilter
            {
                Property = property,
                Values = values.Select(v => v.Val)
            };
        }

        /// <summary>
        /// Return a filter for all values of <paramref name="property"/> being present in <paramref name="values"/>.
        /// </summary>
        /// <param name="values">Values to compare</param>
        /// <param name="property">Property to compare</param>
        public static IDMSFilter ContainsAll(IEnumerable<Value> values, params string[] property)
        {
            if (values == null) throw new ArgumentNullException(nameof(values));
            return new ContainsAllFilter
            {
                Property = property,
                Values = values.Select(v => v.Val)
            };
        }

        /// <summary>
        /// Return the empty filter that matches everything.
        /// </summary>
        public static IDMSFilter MatchAll()
        {
            return new MatchAllFilter();
        }

        /// <summary>
        /// Return a filter on a different node joined through a direct reference.
        /// </summary>
        /// <param name="filter">The filter to apply.</param>
        /// <param name="scope">The direct reference property to query through.</param>
        public static IDMSFilter Nested(IDMSFilter filter, params string[] scope)
        {
            if (filter == null) throw new ArgumentNullException(nameof(filter));
            return new NestedFilter
            {
                Filter = filter,
                Scope = scope
            };
        }

        /// <summary>
        /// Return a filter requiring that the range given by the properties <paramref name="startProperty"/> and <paramref name="endProperty"/>
        /// overlap with the range given by <paramref name="gt"/>, <paramref name="gte"/>, <paramref name="lt"/>, <paramref name="lte"/>.
        /// </summary>
        /// <param name="gt">Exclusive lower bound for range</param>
        /// <param name="gte">Inclusive lower bound for range</param>
        /// <param name="lt">Exclusive upper bound for range</param>
        /// <param name="lte">Inclusive upper bound for range</param>
        /// <param name="startProperty">Start of range</param>
        /// <param name="endProperty">End of range</param>
        public static IDMSFilter Range(Value? gt, Value? gte, Value? lt, Value? lte, IEnumerable<string> startProperty, IEnumerable<string> endProperty)
        {
            if (startProperty == null) throw new ArgumentNullException(nameof(startProperty));
            if (endProperty == null) throw new ArgumentNullException(nameof(endProperty));
            return new OverlapsFilter
            {
                StartProperty = startProperty,
                EndProperty = endProperty,
                GreaterThan = gt?.Val,
                GreaterThanEqual = gte?.Val,
                LessThan = lt?.Val,
                LessThanEqual = lte?.Val,
            };
        }

        /// <summary>
        /// Return a filter requiring that the instances have properties in containers or views given by
        /// <paramref name="sources"/>.
        /// </summary>
        /// <param name="sources">Containers or views</param>
        public static IDMSFilter HasData(IEnumerable<SourceIdentifier> sources)
        {
            if (sources == null) throw new ArgumentNullException(nameof(sources));
            return new HasDataFilter
            {
                Models = sources
            };
        }

        /// <summary>
        /// Require that the instance is in <paramref name="space"/>.
        /// </summary>
        /// <param name="space">Space the instances must be in</param>
        /// <param name="forNode">Whether this filter is for nodes or edges.</param>
        public static IDMSFilter Space(string space, bool forNode = true)
        {
            if (space == null) throw new ArgumentNullException(nameof(space));
            return new EqualsFilter
            {
                Property = new[] { forNode ? "node" : "edge", "space" },
                Value = new RawPropertyValue<string>
                {
                    Value = space
                }
            };
        }

        /// <summary>
        /// Filter on node externalId.
        /// </summary>
        /// <param name="externalId">ExternalId to match</param>
        /// <param name="forNode">Whether this filter is for nodes or edges.</param>
        public static IDMSFilter ExternalId(string externalId, bool forNode = true)
        {
            if (externalId == null) throw new ArgumentNullException(nameof(externalId));
            return new EqualsFilter
            {
                Property = new[] { forNode ? "node" : "edge", "externalId" },
                Value = new RawPropertyValue<string>
                {
                    Value = externalId
                }
            };
        }

        /// <summary>
        /// Filter on node externalIds.
        /// </summary>
        /// <param name="externalIds">ExternalIds to match</param>
        public static IDMSFilter ExternalId(IEnumerable<string> externalIds)
        {
            if (externalIds == null) throw new ArgumentNullException(nameof(externalIds));
            return new InFilter
            {
                Property = new[] { "node", "externalId" },
                Values = externalIds.Select(e => new RawPropertyValue<string>
                {
                    Value = e
                }).ToArray()
            };
        }
    }
}
