using Cognite.Extensions.DataModels.QueryBuilder;
using CogniteSdk;
using CogniteSdk.Beta.DataModels;
using CogniteSdk.Resources.Beta;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.Extensions.DataModels
{
    /// <summary>
    /// Utility methods for performing atomic updates against FDM.
    /// </summary>
    public static class AtomicUpdateUtils
    {
        /// <summary>
        /// Upsert a list of up to 1000 items atomically. 
        /// </summary>
        /// <typeparam name="T">Type of data to retrieve</typeparam>
        /// <param name="resource">Data models resouce</param>
        /// <param name="externalIds">ExternalIDs of instances to upsert</param>
        /// <param name="space">Space of given externalIds</param>
        /// <param name="instanceType">Instance type to upsert</param>
        /// <param name="sources">Which sources to retrieve data for.</param>
        /// <param name="updateAction">Action called once for each upsert attempt,
        /// should create a write by updating the given instances.
        /// May be called multiple times if the upsert fails with a conflict.</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>Updated instances</returns>
        public static async Task<IEnumerable<SlimInstance>> UpsertAtomic<T>(
            this DataModelsResource resource,
            IEnumerable<string> externalIds,
            string space,
            InstanceType instanceType,
            IEnumerable<InstanceSource> sources,
            Func<IEnumerable<BaseInstance<T>>, IEnumerable<BaseInstanceWrite>> updateAction,
            CancellationToken token)
        {
            if (updateAction == null) throw new ArgumentNullException(nameof(updateAction));
            if (externalIds.Count() > 1000 || !externalIds.Any())
            {
                throw new InvalidOperationException("Number of externalIds must be between 1 and 1000");
            }

            while (!token.IsCancellationRequested)
            {
                var data = await resource.FilterInstances<T>(new InstancesFilter
                {
                    InstanceType = instanceType,
                    Limit = 1000,
                    Filter = Filter.And(
                        Filter.ExternalId(externalIds),
                        Filter.Space(space, instanceType == InstanceType.node)),
                    Sources = sources
                }, token).ConfigureAwait(false);

                var upserts = updateAction(data.Items);

                var byId = upserts.ToDictionary(k => (k.Space, k.ExternalId));
                if (!byId.Any()) return Enumerable.Empty<SlimInstance>();

                foreach (var inst in data.Items)
                {
                    if (byId.TryGetValue((inst.Space, inst.ExternalId), out var upsert))
                    {
                        upsert.ExistingVersion = inst.Version;
                    }
                }

                try
                {
                    var res = await resource.UpsertInstances(new InstanceWriteRequest
                    {
                        SkipOnVersionConflict = false,
                        Items = upserts,
                        Replace = true,
                        AutoCreateEndNodes = true,
                        AutoCreateStartNodes = true,
                    }, token).ConfigureAwait(false);
                    return res;
                }
                catch (ResponseException ex)
                {
                    if (ex.Code == 409 && ex.Message == "A version conflict caused the ingest to fail.")
                    {
                        // Try again right away
                        continue;
                    }
                    throw;
                }
            }
            throw new OperationCanceledException();
        }
    }
}
