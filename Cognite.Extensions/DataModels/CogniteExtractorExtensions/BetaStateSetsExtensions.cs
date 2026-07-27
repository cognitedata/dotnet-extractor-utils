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
            if (stateSets == null) throw new ArgumentNullException(nameof(stateSets));
            if (buildStateSets == null) throw new ArgumentNullException(nameof(buildStateSets));
            if (options == null) throw new ArgumentNullException(nameof(options));
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
            if (stateSets == null) throw new ArgumentNullException(nameof(stateSets));
            if (buildStateSets == null) throw new ArgumentNullException(nameof(buildStateSets));
            if (options == null) throw new ArgumentNullException(nameof(options));
            return BetaResourceExtensions.GetOrCreateAsync(
                StateSetsResource.View,
                (ids, tok) => stateSets.RetrieveAsync<T>(ids, tok),
                (items, tok) => stateSets.UpsertAsync(items, null, tok),
                DataModelSanitation.CleanInstanceRequest,
                instanceIds, buildStateSets, options, token);
        }

        /// <summary>
        /// Ensures that all state sets in <paramref name="stateSetsToEnsure"/> exists in CDF.
        /// Tries to create the state sets and returns when all are created or have been removed
        /// due to issues with the request.
        /// </summary>
        /// <param name="stateSets">CogniteSdk beta CDM StateSets resource</param>
        /// <param name="stateSetsToEnsure">List of CogniteSdk SourcedNodeWrite objects</param>
        /// <param name="options">Chunking, throttling, retry, and sanitation options. Keeping duplicates
        /// via the retry mode is not valid for this method.</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>A <see cref="CogniteResult{TResult, TError}"/> containing errors that occured and a list of the created state sets</returns>
        public static Task<CogniteResult<SourcedNode<T>, SourcedNodeWrite<T>>> EnsureStateSetsExistAsync<T>(
            this StateSetsResource stateSets,
            IEnumerable<SourcedNodeWrite<T>> stateSetsToEnsure,
            BetaResourceParams options,
            CancellationToken token) where T : CogniteStateSet
        {
            if (stateSets == null) throw new ArgumentNullException(nameof(stateSets));
            if (options == null) throw new ArgumentNullException(nameof(options));
            return BetaResourceExtensions.EnsureExistsAsync(
                StateSetsResource.View,
                (items, tok) => stateSets.UpsertAsync(items, null, tok),
                DataModelSanitation.CleanInstanceRequest,
                stateSetsToEnsure, options, token);
        }

        /// <summary>
        /// Get the state sets with the provided <paramref name="ids"/>. Ignore any unknown ids.
        /// </summary>
        /// <param name="stateSets">CogniteSdk beta CDM StateSets resource</param>
        /// <param name="ids">List of <see cref="Identity"/> objects</param>
        /// <param name="chunkSize">Chunk size</param>
        /// <param name="throttleSize">Throttle size</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>Retrieved state sets</returns>
        public static Task<IEnumerable<SourcedNode<T>>> GetStateSetsByIdsIgnoreErrors<T>(
            this StateSetsResource stateSets,
            IEnumerable<Identity> ids,
            int chunkSize,
            int throttleSize,
            CancellationToken token) where T : CogniteStateSet
        {
            if (stateSets == null) throw new ArgumentNullException(nameof(stateSets));
            return BetaResourceExtensions.GetByIdsIgnoreErrors(
                StateSetsResource.View,
                (chunkIds, tok) => stateSets.RetrieveAsync<T>(chunkIds, tok),
                ids, chunkSize, throttleSize, token);
        }

        /// <summary>
        /// Upsert state sets.
        /// If any items fail to be created due to duplicated instance ids, they can be removed before
        /// retrying by setting the retry mode in <paramref name="options"/>.
        /// </summary>
        /// <param name="resource">CogniteSdk beta CDM StateSets resource</param>
        /// <param name="items">List of state set updates</param>
        /// <param name="options">Chunking, throttling, retry, and sanitation options</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>A <see cref="CogniteResult{TResult, TError}"/> containing errors that occured and a list of the updated state sets</returns>
        public static Task<CogniteResult<SlimInstance, SourcedNodeWrite<T>>> UpsertAsync<T>(
            this StateSetsResource resource,
            IEnumerable<SourcedNodeWrite<T>> items,
            BetaResourceParams options,
            CancellationToken token) where T : CogniteStateSet
        {
            if (resource == null) throw new ArgumentNullException(nameof(resource));
            if (options == null) throw new ArgumentNullException(nameof(options));
            return BetaResourceExtensions.UpsertAsync(
                StateSetsResource.View,
                (chunkItems, tok) => resource.UpsertAsync(chunkItems, null, tok),
                DataModelSanitation.CleanInstanceRequest,
                items, options, token);
        }
    }
}
