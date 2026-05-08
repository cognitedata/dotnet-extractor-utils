using CogniteSdk;
using CogniteSdk.DataModels;
using CogniteSdk.DataModels.Core;
using CogniteSdk.Resources.DataModels;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.Extensions
{
    public static partial class ResultHandlers
    {
        private static void ParseInstancesException(ResponseException ex, CogniteError err)
        {
            if (ex.Message.StartsWith("Cannot update immutable property", StringComparison.InvariantCultureIgnoreCase))
            {
                err.Type = ErrorType.IllegalItem;
                err.Resource = ResourceType.InstanceProperty;
            }
        }

        private static bool IsAffected<T>(SourcedNodeWrite<T> ts, HashSet<Identity> badValues, CogniteError<SourcedNodeWrite<T>> error)
        {
            return error.Resource switch
            {
                ResourceType.InstanceId => badValues.ContainsIdentity(new InstanceIdentifier(ts.Space, ts.ExternalId)),
                _ => false,
            };
        }

        /// <summary>
        /// Clean list of SourceNodeWrite objects based on CogniteError
        /// </summary>
        /// <param name="error">Error that occured with a previous push</param>
        /// <param name="items">Nodes to clean</param>
        /// <param name="resource">Resource to query for underlying timeseries with, when T is a child of CogniteTimeSeries</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>Nodes that are not affected by the error</returns>
        public static async Task<IEnumerable<SourcedNodeWrite<T>>> CleanFromError<T, T2>(
            CogniteError<SourcedNodeWrite<T>> error,
            IEnumerable<SourcedNodeWrite<T>> items,
            T2 resource,
            CancellationToken token
            ) where T2 : BaseDataModelResource<T>
        {
            if (resource != null && error?.Type == ErrorType.IllegalItem)
            {
                if(typeof(CogniteTimeSeriesBase).IsAssignableFrom(typeof(T)))
                {
                    var fetched = await FetchItems(resource, items, token).ConfigureAwait(false);
                    if (error.Message!.Contains("'cdf_cdm.CogniteTimeSeries.type'"))
                    {
                        return CleanTypeImmutabilityError(items, fetched);
                    }
                }
                // else if(typeof(CogniteAssetBase).IsAssignableFrom(typeof(T))) { }
            }
            return CleanFromErrorCommon(error!, items, IsAffected,
                ts => ts.ExternalId == null || ts.Space == null ? null : Identity.Create(new InstanceIdentifier(ts.Space, ts.ExternalId)),
                CdfMetrics.TimeSeriesSkipped);
        }
    }
}
