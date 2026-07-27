using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extractor.Common;
using CogniteSdk;
using CogniteSdk.DataModels;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Prometheus;

namespace Cognite.Extensions.DataModels.CogniteExtractorExtensions
{
    /// <summary>
    /// Retrieves instances by id for the beta CDM resource wrapped by a <see cref="BetaResourceExtensions"/> method.
    /// </summary>
    internal delegate Task<IEnumerable<SourcedNode<T>>> RetrieveInstancesFunc<T>(IEnumerable<InstanceIdentifierWithType> ids, CancellationToken token);

    /// <summary>
    /// Upserts instances for the beta CDM resource wrapped by a <see cref="BetaResourceExtensions"/> method.
    /// </summary>
    internal delegate Task<IEnumerable<SlimInstance>> UpsertInstancesFunc<T>(IEnumerable<SourcedNodeWrite<T>> items, CancellationToken token);

    /// <summary>
    /// Sanitizes a batch of instance writes for the beta CDM resource wrapped by a <see cref="BetaResourceExtensions"/> method.
    /// </summary>
    internal delegate (IEnumerable<SourcedNodeWrite<T>>, IEnumerable<CogniteError<SourcedNodeWrite<T>>>) SanitizeInstancesFunc<T>(IEnumerable<SourcedNodeWrite<T>> items, SanitationMode mode);

    /// <summary>
    /// Shared get-or-create implementation for beta CDM resources
    /// (<see cref="CogniteSdk.Resources.Beta.StateSetsResource"/>, <see cref="CogniteSdk.Resources.Beta.TimeSeriesResource"/>).
    /// These resources do not implement <c>BaseDataModelResource&lt;T&gt;</c>, so they cannot use the
    /// generic implementation in <see cref="Cognite.Extensions.DataModels.DataModelUtils"/>; instead
    /// <see cref="BetaStateSetsExtensions"/> is a thin public wrapper that supplies its resource's
    /// retrieve/upsert/sanitize operations to the methods here.
    /// </summary>
    internal static class BetaResourceExtensions
    {
        private static ILogger _logger = new NullLogger<Client>();

        internal static void SetLogger(ILogger logger)
        {
            _logger = logger;
        }

        public static Task<CogniteResult<SourcedNode<T>, SourcedNodeWrite<T>>> GetOrCreateAsync<T>(
            ViewIdentifier view,
            RetrieveInstancesFunc<T> retrieve,
            UpsertInstancesFunc<T> upsert,
            SanitizeInstancesFunc<T> sanitize,
            IEnumerable<InstanceIdentifier> instanceIds,
            Func<IEnumerable<InstanceIdentifier>, IEnumerable<SourcedNodeWrite<T>>> buildItems,
            BetaResourceParams options,
            CancellationToken token)
        {
            Task<IEnumerable<SourcedNodeWrite<T>>> AsyncBuildItems(IEnumerable<InstanceIdentifier> ids)
            {
                return Task.FromResult(buildItems(ids));
            }
            return GetOrCreateAsync(view, retrieve, upsert, sanitize, instanceIds, AsyncBuildItems, options, token);
        }

        /// <summary>
        /// Retrieves instances by id for the beta CDM resource wrapped by a <see cref="BetaResourceExtensions"/> method. If any instances are missing, they will be created using the provided <see cref="buildItems"/> function.
        /// </summary>
        /// <typeparam name="T">The type of the instance properties.</typeparam>
        /// <param name="view">The view to retrieve instances from.</param>
        /// <param name="retrieve">The function to retrieve instances.</param>
        /// <param name="upsert">The function to upsert instances.</param>
        /// <param name="sanitize">The function to sanitize instances.</param>
        /// <param name="instanceIds">The ids of the instances to retrieve or create.</param>
        /// <param name="buildItems">The function to build instances to create.</param>
        /// <param name="options">The options for the get-or-create operation.</param>
        /// <param name="token">The cancellation token.</param>
        /// <returns>A <see cref="CogniteResult{TResult, TError}"/> containing the retrieved or created instances and any errors that occurred during the operation.</returns>
        public static async Task<CogniteResult<SourcedNode<T>, SourcedNodeWrite<T>>> GetOrCreateAsync<T>(
            ViewIdentifier view,
            RetrieveInstancesFunc<T> retrieve,
            UpsertInstancesFunc<T> upsert,
            SanitizeInstancesFunc<T> sanitize,
            IEnumerable<InstanceIdentifier> instanceIds,
            Func<IEnumerable<InstanceIdentifier>, Task<IEnumerable<SourcedNodeWrite<T>>>> buildItems,
            BetaResourceParams options,
            CancellationToken token)
        {
            var chunks = instanceIds
                .ChunkBy(options.ChunkSize)
                .ToList();
            if (!chunks.Any()) return new CogniteResult<SourcedNode<T>, SourcedNodeWrite<T>>(null, null);

            var results = new CogniteResult<SourcedNode<T>, SourcedNodeWrite<T>>[chunks.Count];

            var generators = chunks
                .Select<IEnumerable<InstanceIdentifier>, Func<Task>>(
                    (chunk, idx) => async () =>
                    {
                        results[idx] = await GetOrCreateChunk(view, retrieve, upsert, sanitize, chunk,
                            buildItems, 0, options, token).ConfigureAwait(false);
                    });

            await generators.RunThrottled(options.ThrottleSize, token).ConfigureAwait(false);

            return CogniteResult<SourcedNode<T>, SourcedNodeWrite<T>>.Merge(results);
        }

        private static async Task<CogniteResult<SourcedNode<T>, SourcedNodeWrite<T>>> GetOrCreateChunk<T>(
            ViewIdentifier view,
            RetrieveInstancesFunc<T> retrieve,
            UpsertInstancesFunc<T> upsert,
            SanitizeInstancesFunc<T> sanitize,
            IEnumerable<InstanceIdentifier> instanceIds,
            Func<IEnumerable<InstanceIdentifier>, Task<IEnumerable<SourcedNodeWrite<T>>>> buildItems,
            int backoff,
            BetaResourceParams options,
            CancellationToken token)
        {
            IEnumerable<SourcedNode<T>> found;
            using (CdfMetrics.Instances(view, "retrieve").NewTimer())
            {
                try
                {
                    found = await retrieve(
                        instanceIds.Select(x => new InstanceIdentifierWithType(InstanceType.node, x)), token).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    var err = ResultHandlers.ParseSimpleError<SourcedNodeWrite<T>>(ex, instanceIds?.Select(x => Identity.Create(x)), null);
                    return new CogniteResult<SourcedNode<T>, SourcedNodeWrite<T>>(new[] { err }, null);
                }
            }

            var missing = instanceIds.Except(found.Select(ts => new InstanceIdentifier(ts.Space, ts.ExternalId))).ToList();

            if (missing.Count == 0)
            {
                return new CogniteResult<SourcedNode<T>, SourcedNodeWrite<T>>(null, found);
            }

            var toCreate = await buildItems(missing).ConfigureAwait(false);

            IEnumerable<CogniteError<SourcedNodeWrite<T>>> errors;
            (toCreate, errors) = sanitize(toCreate, options.SanitationMode);

            var result = await CreateHandleErrors(view, upsert, toCreate, options.RetryMode, token).ConfigureAwait(false);
            result.Results = result.Results == null ? found : result.Results.Concat(found);

            if (errors.Any())
            {
                result.Errors = result.Errors == null ? errors : result.Errors.Concat(errors);
            }

            if (result.Errors == null || !result.Errors.Any()
                || options.RetryMode != RetryMode.OnErrorKeepDuplicates
                && options.RetryMode != RetryMode.OnFatalKeepDuplicates) return result;

            var duplicateErrors = (result.Errors ?? Enumerable.Empty<CogniteError>()).Where(err =>
                err.Resource == ResourceType.ExternalId
                && err.Type == ErrorType.ItemExists)
                .ToList();

            var duplicatedIds = new HashSet<InstanceIdentifier>();
            if (duplicateErrors.Any())
            {
                foreach (var error in duplicateErrors)
                {
                    if (error.Values == null || !error.Values.Any()) continue;
                    foreach (var idt in error.Values) duplicatedIds.Add(idt.InstanceId);
                }
            }

            if (!duplicatedIds.Any()) return result;
            if (backoff == 3)
            {
                // We should never reach here anyway. The duplicates are objects that were created between
                // retrieve and CreateHandleErrors call. If there are persistent duplicates, we will just return
                // the results as is.
                _logger.LogError("Failed to resolve {Count} duplicated instance ids in view {View} after {Backoff} retries, giving up",
                    duplicatedIds.Count, view.ExternalId, backoff);
                return result;
            }

            await Task.Delay(TimeSpan.FromSeconds(0.1 * Math.Pow(2, backoff)), token).ConfigureAwait(false);
            var nextResult = await GetOrCreateChunk(view, retrieve, upsert, sanitize, duplicatedIds,
                buildItems, backoff + 1, options, token)
                .ConfigureAwait(false);
            result = result.Merge(nextResult);

            return result;
        }

        private static async Task<CogniteResult<SourcedNode<T>, SourcedNodeWrite<T>>> CreateHandleErrors<T>(
            ViewIdentifier view,
            UpsertInstancesFunc<T> upsert,
            IEnumerable<SourcedNodeWrite<T>> toCreate,
            RetryMode retryMode,
            CancellationToken token)
        {
            var errors = new List<CogniteError<SourcedNodeWrite<T>>>();
            while (toCreate != null && toCreate.Any() && !token.IsCancellationRequested)
            {
                try
                {
                    IEnumerable<SlimInstance> newInstances;
                    using (CdfMetrics.Instances(view, "create").NewTimer())
                    {
                        newInstances = await upsert(toCreate, token).ConfigureAwait(false);
                    }

                    var toCreateDict = new Dictionary<InstanceIdentifier, T>();
                    foreach (var cr in toCreate)
                    {
                        toCreateDict[new InstanceIdentifier(cr.Space, cr.ExternalId)] = cr.Properties;
                    }

                    return new CogniteResult<SourcedNode<T>, SourcedNodeWrite<T>>(
                        errors,
                        newInstances.Select(x =>
                        {
                            var id = new InstanceIdentifier(x.Space, x.ExternalId);
                            toCreateDict.TryGetValue(id, out var props);
                            return new SourcedNode<T>(x, props);
                        }));
                }
                catch (Exception ex)
                {
                    var error = ResultHandlers.ParseException<SourcedNodeWrite<T>>(ex, RequestType.UpsertInstances);
                    if (error.Type == ErrorType.FatalFailure
                        && (retryMode == RetryMode.OnFatal
                            || retryMode == RetryMode.OnFatalKeepDuplicates))
                    {
                        await Task.Delay(1000, token).ConfigureAwait(false);
                    }
                    else if (retryMode == RetryMode.None)
                    {
                        errors.Add(error);
                        break;
                    }
                    else
                    {
                        errors.Add(error);
                        toCreate = await ResultHandlers.CleanFromError(error, toCreate, token).ConfigureAwait(false);
                    }
                }
            }
            return new CogniteResult<SourcedNode<T>, SourcedNodeWrite<T>>(errors, null);
        }
    }
}
