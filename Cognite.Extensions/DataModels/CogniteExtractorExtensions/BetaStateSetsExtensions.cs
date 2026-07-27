using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using CogniteSdk;
using CogniteSdk.DataModels;
using CogniteSdk.DataModels.Core;
using CogniteSdk.Resources.Beta;

namespace Cognite.Extensions.DataModels.CogniteExtractorExtensions
{
    /// <summary>
    /// Extension methods for creating and managing state sets through the beta CDM state set
    /// resource. State sets are currently only available through this beta API. These are thin
    /// wrappers around <see cref="BetaResourceExtensions"/>.
    /// </summary>
    public static class BetaStateSetsExtensions
    {
        /// <summary>
        /// Get or create the state sets with the provided <paramref name="instanceIds"/> if they exist in CDF.
        /// If one or more do not exist, use the <paramref name="buildStateSets"/> function to construct
        /// the missing state set objects and upload them to CDF using the chunking and throttling in
        /// <paramref name="options"/>.
        /// </summary>
        /// <param name="stateSets">CogniteSdk beta CDM StateSets resource</param>
        /// <param name="instanceIds">Instance Ids</param>
        /// <param name="buildStateSets">Function that builds CogniteSdk SourcedNodeWrite objects</param>
        /// <param name="options">Chunking, throttling, retry, and sanitation options</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>A <see cref="CogniteResult{TResult, TError}"/> containing errors that occured and a list of the created and found state sets</returns>
        public static Task<CogniteResult<SourcedNode<T>, SourcedNodeWrite<T>>> GetOrCreateStateSetsAsync<T>(
            this StateSetsResource stateSets,
            IEnumerable<InstanceIdentifier> instanceIds,
            Func<IEnumerable<InstanceIdentifier>, IEnumerable<SourcedNodeWrite<T>>> buildStateSets,
            BetaResourceParams options,
            CancellationToken token) where T : CogniteStateSet
        {
            return BetaResourceExtensions.GetOrCreateAsync(
                StateSetsResource.View,
                (ids, tok) => stateSets.RetrieveAsync<T>(ids, tok),
                (items, tok) => stateSets.UpsertAsync(items, null, tok),
                DataModelSanitation.CleanInstanceRequest,
                instanceIds, buildStateSets, options, token);
        }

        /// <summary>
        /// Get or create the state sets with the provided <paramref name="instanceIds"/> if they exist in CDF.
        /// If one or more do not exist, use the <paramref name="buildStateSets"/> function to construct
        /// the missing state set objects and upload them to CDF using the chunking and throttling in
        /// <paramref name="options"/>.
        /// </summary>
        /// <param name="stateSets">CogniteSdk beta CDM StateSets resource</param>
        /// <param name="instanceIds">Instance Ids</param>
        /// <param name="buildStateSets">Async function that builds CogniteSdk SourcedNodeWrite objects</param>
        /// <param name="options">Chunking, throttling, retry, and sanitation options</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>A <see cref="CogniteResult{TResult, TError}"/> containing errors that occured and a list of the created and found state sets</returns>
        public static Task<CogniteResult<SourcedNode<T>, SourcedNodeWrite<T>>> GetOrCreateStateSetsAsync<T>(
            this StateSetsResource stateSets,
            IEnumerable<InstanceIdentifier> instanceIds,
            Func<IEnumerable<InstanceIdentifier>, Task<IEnumerable<SourcedNodeWrite<T>>>> buildStateSets,
            BetaResourceParams options,
            CancellationToken token) where T : CogniteStateSet
        {
            if (options == null) throw new ArgumentNullException(nameof(options));
            return BetaResourceExtensions.GetOrCreateAsync(
                StateSetsResource.View,
                (ids, tok) => stateSets.RetrieveAsync<T>(ids, tok),
                (items, tok) => stateSets.UpsertAsync(items, null, tok),
                DataModelSanitation.CleanInstanceRequest,
                instanceIds, buildStateSets, options, token);
        }
    }
}
