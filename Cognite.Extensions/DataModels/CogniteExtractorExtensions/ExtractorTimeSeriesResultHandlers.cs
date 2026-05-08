using CogniteSdk.DataModels;
using CogniteSdk.DataModels.Core;
using CogniteSdk.Resources.DataModels;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.Extensions
{
    public static partial class ResultHandlers
    {
        /// <summary>
        /// Fetches Cognite Time Series
        /// </summary>
        /// <param name="resource">CDF resource to fetch with</param>
        /// <param name="items">Items to fetch</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>Cognite Time Series</returns>
        public static async Task<IEnumerable<SourcedInstance<CogniteTimeSeriesBase>>> FetchItems<T2, T>(
            T2 resource,
            IEnumerable<SourcedNodeWrite<T>> items,
            CancellationToken token
        ) where T2 : BaseDataModelResource<T>
        {
            return await resource.RetrieveAsync<CogniteTimeSeriesBase>(
                items.Select(x => new InstanceIdentifierWithType(InstanceType.node, x.Space, x.ExternalId)),
                CoreTimeSeriesResource<CogniteTimeSeriesBase>.DefaultView,
                token
            ).ConfigureAwait(false);
        }

        /// <summary>
        /// Clean Cognite Time Series from Type Immutability Errors
        /// </summary>
        /// <param name="items">Items to clean</param>
        /// <param name="fetchedItems">Original items retrieved from CDF</param>
        /// <returns>Cognite Time Series without type conflicts and ones not present in CDF</returns>
        public static IEnumerable<SourcedNodeWrite<T>> CleanTypeImmutabilityError<T>(
            IEnumerable<SourcedNodeWrite<T>> items, IEnumerable<SourcedInstance<CogniteTimeSeriesBase>> fetchedItems)
        {
            var foundTypeDict = fetchedItems.ToDictionarySafe(x => new InstanceIdentifier(x.Space, x.ExternalId), x => x.Properties.Type);
            return items.Where(x => !foundTypeDict.ContainsKey(new InstanceIdentifier(x.Space, x.ExternalId)) || foundTypeDict[new InstanceIdentifier(x.Space, x.ExternalId)] == (x.Properties as CogniteTimeSeriesBase)?.Type);
        }
    }
}
