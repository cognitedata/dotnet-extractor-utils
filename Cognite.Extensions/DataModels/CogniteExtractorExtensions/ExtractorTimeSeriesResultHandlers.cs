using CogniteSdk;
using CogniteSdk.DataModels;
using System.Collections.Generic;
using System.Linq;

namespace Cognite.Extensions
{
    public static partial class ResultHandlers
    {
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
        /// <param name="timeseries">Nodes to clean</param>
        /// <returns>Nodes that are not affected by the error</returns>
        public static IEnumerable<SourcedNodeWrite<T>> CleanFromError<T>(
            CogniteError<SourcedNodeWrite<T>> error,
            IEnumerable<SourcedNodeWrite<T>> timeseries)
        {
            return CleanFromErrorCommon(error, timeseries, IsAffected,
                ts => ts.ExternalId == null || ts.Space == null ? null : Identity.Create(new InstanceIdentifier(ts.Space, ts.ExternalId)),
                CdfMetrics.TimeSeriesSkipped);
        }
    }
}
