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
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

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
    /// Shared get-or-create/ensure-exists/get-by-ids/upsert implementation for beta CDM resources
    /// (<see cref="CogniteSdk.Resources.Beta.StateSetsResource"/>, <see cref="CogniteSdk.Resources.Beta.TimeSeriesResource"/>).
    /// <see cref="BetaStateSetsExtensions"/> is a thin public wrapper that supplies its resource's
    /// retrieve/upsert/sanitize operations to the methods here.
    /// These resources do not implement <c>BaseDataModelResource&lt;T&gt;</c>, so they cannot use the
    /// generic implementation in <see cref="Cognite.Extensions.DataModels.DataModelUtils"/>; instead
    /// </summary>
    internal static class BetaResourceExtensions
    {
        private static ILogger _logger = new NullLogger<Client>();

        internal static void SetLogger(ILogger logger)
        {
            _logger = logger;
        }

        /// <summary>
        /// Gets instances for the beta CDM resource. If any instances are missing,
        /// they will be created using the provided <see paramref="buildItems"/> function.
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
            if (buildItems == null) throw new ArgumentNullException(nameof(buildItems));
            Task<IEnumerable<SourcedNodeWrite<T>>> AsyncBuildItems(IEnumerable<InstanceIdentifier> ids)
            {
                return Task.FromResult(buildItems(ids));
            }
            return GetOrCreateAsync(view, retrieve, upsert, sanitize, instanceIds, AsyncBuildItems, options, token);
        }

        /// <summary>
        /// Retrieves instances by id for the beta CDM resource wrapped by a <see cref="BetaResourceExtensions"/> method. If any instances are missing,
        /// they will be created using the provided <see paramref="buildItems"/> function.
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
            if (view == null) throw new ArgumentNullException(nameof(view));
            if (retrieve == null) throw new ArgumentNullException(nameof(retrieve));
            if (upsert == null) throw new ArgumentNullException(nameof(upsert));
            if (sanitize == null) throw new ArgumentNullException(nameof(sanitize));
            if (buildItems == null) throw new ArgumentNullException(nameof(buildItems));
            if (options == null) throw new ArgumentNullException(nameof(options));

            if (instanceIds == null || !instanceIds.Any()) return new CogniteResult<SourcedNode<T>, SourcedNodeWrite<T>>(null, null);

            var chunks = instanceIds.ChunkBy(options.ChunkSize).ToList();

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
                    found ??= Enumerable.Empty<SourcedNode<T>>();

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
                    foreach (var idt in error.Values)
                    {
                        if (idt.InstanceId == null)
                        {
                            _logger.LogError("No instance id for view {View} with error {Error}", view.ExternalId, error);
                            continue;
                        }
                        duplicatedIds.Add(idt.InstanceId);
                    }
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
            return await HandleWriteErrors(
                toCreate,
                retryMode,
                token,
                async (currentItems, currentToken) =>
                {
                    IEnumerable<SlimInstance> newInstances;
                    using (CdfMetrics.Instances(view, "create").NewTimer())
                    {
                        newInstances = await upsert(currentItems, currentToken).ConfigureAwait(false);
                    }

                    var toCreateDict = new Dictionary<InstanceIdentifier, T>();
                    foreach (var cr in currentItems)
                    {
                        toCreateDict[new InstanceIdentifier(cr.Space, cr.ExternalId)] = cr.Properties;
                    }

                    return newInstances.Select(x =>
                    {
                        var id = new InstanceIdentifier(x.Space, x.ExternalId);
                        toCreateDict.TryGetValue(id, out var props);
                        return new SourcedNode<T>(x, props!);
                    });
                }).ConfigureAwait(false);
        }

        /// <summary>
        /// Upserts instances for the beta CDM resource wrapped by a <see cref="BetaResourceExtensions"/> method.
        /// </summary>
        /// <typeparam name="T">The type of the instance properties.</typeparam>
        /// <param name="view">The view to upsert instances to.</param>
        /// <param name="upsert">The function to upsert instances.</param>
        /// <param name="sanitize">The function to sanitize instances.</param>
        /// <param name="itemsToEnsure">The instances to upsert.</param>
        /// <param name="options">The options for the upsert operation.</param>
        /// <param name="token">The cancellation token.</param>
        /// <returns>A <see cref="CogniteResult{TResult, TError}"/> containing the upserted instances and any errors that occurred during the operation.</returns>
        public static async Task<CogniteResult<SourcedNode<T>, SourcedNodeWrite<T>>> EnsureExistsAsync<T>(
            ViewIdentifier view,
            UpsertInstancesFunc<T> upsert,
            SanitizeInstancesFunc<T> sanitize,
            IEnumerable<SourcedNodeWrite<T>> itemsToEnsure,
            BetaResourceParams options,
            CancellationToken token)
        {
            if (view == null) throw new ArgumentNullException(nameof(view));
            if (upsert == null) throw new ArgumentNullException(nameof(upsert));
            if (sanitize == null) throw new ArgumentNullException(nameof(sanitize));
            if (options == null) throw new ArgumentNullException(nameof(options));

            return await WriteChunked(
                sanitize,
                itemsToEnsure,
                options,
                token,
                chunk => CreateHandleErrors(view, upsert, chunk, options.RetryMode, token))
                .ConfigureAwait(false);
        }

        /// <summary>
        /// Retrieves instances by their IDs, ignoring any errors that occur during the process.
        /// Doesn't absorb exceptions, but instead returns a list of successfully retrieved instances
        /// and logs any errors encountered.
        /// </summary>
        /// <typeparam name="T">The type of the instances being retrieved.</typeparam>
        /// <param name="view">The view identifier.</param>
        /// <param name="retrieve">The function to retrieve instances.</param>
        /// <param name="ids">The list of instance identifiers.</param>
        /// <param name="chunkSize">The size of each chunk for processing.</param>
        /// <param name="throttleSize">The maximum number of concurrent tasks.</param>
        /// <param name="token">The cancellation token.</param>
        /// <returns>A list of successfully retrieved instances.</returns>
        /// <exception cref="ArgumentNullException">Thrown when any of the required parameters are null.</exception>
        public static async Task<IEnumerable<SourcedNode<T>>> GetByIdsIgnoreErrors<T>(
            ViewIdentifier view,
            RetrieveInstancesFunc<T> retrieve,
            IEnumerable<Identity> ids,
            int chunkSize,
            int throttleSize,
            CancellationToken token)
        {
            if (view == null) throw new ArgumentNullException(nameof(view));
            if (retrieve == null) throw new ArgumentNullException(nameof(retrieve));
            var result = new List<SourcedNode<T>>();
            object mutex = new object();

            if (ids == null || !ids.Any()) return result;
            if (ids.Any(x => x.InstanceId == null)) throw new ArgumentException("All identities must have an InstanceId specified.", nameof(ids));

            var chunks = ids.ChunkBy(chunkSize).ToList();

            var generators = chunks
                .Select((Func<IEnumerable<Identity>, Func<Task>>)(chunk => async () =>
                {
                    IEnumerable<SourcedNode<T>> found;
                    using (CdfMetrics.Instances(view, "retrieve").NewTimer())
                    {
                        found = await retrieve(chunk.Select(x => new InstanceIdentifierWithType(InstanceType.node, x.InstanceId)), token).ConfigureAwait(false);
                    }
                    if (found == null || !found.Any()) return;
                    lock (mutex)
                    {
                        result.AddRange(found);
                    }
                }));

            await generators.RunThrottled(throttleSize, token).ConfigureAwait(false);
            return result;
        }

        /// <summary>
        /// Upserts instances asynchronously, handling errors and sanitization.
        /// </summary>
        /// <typeparam name="T">The type of the instances being upserted.</typeparam>
        /// <param name="view">The view identifier.</param>
        /// <param name="upsert">The function to upsert instances.</param>
        /// <param name="sanitize">The function to sanitize instances.</param>
        /// <param name="items">The instances to upsert.</param>
        /// <param name="options">The options for the upsert operation.</param>
        /// <param name="token">The cancellation token.</param>
        /// <returns>A CogniteResult containing the upserted instances and any errors.</returns>
        /// <exception cref="ArgumentNullException">Thrown when any of the required parameters are null.</exception>
        public static async Task<CogniteResult<SlimInstance, SourcedNodeWrite<T>>> UpsertAsync<T>(
            ViewIdentifier view,
            UpsertInstancesFunc<T> upsert,
            SanitizeInstancesFunc<T> sanitize,
            IEnumerable<SourcedNodeWrite<T>> items,
            BetaResourceParams options,
            CancellationToken token)
        {
            if (view == null) throw new ArgumentNullException(nameof(view));
            if (upsert == null) throw new ArgumentNullException(nameof(upsert));
            if (sanitize == null) throw new ArgumentNullException(nameof(sanitize));
            if (options == null) throw new ArgumentNullException(nameof(options));

            return await WriteChunked(
                sanitize,
                items,
                options,
                token,
                chunk => UpsertHandleErrors(view, upsert, chunk, options.RetryMode, token))
                .ConfigureAwait(false);
        }

        private static async Task<CogniteResult<SlimInstance, SourcedNodeWrite<T>>> UpsertHandleErrors<T>(
            ViewIdentifier view,
            UpsertInstancesFunc<T> upsert,
            IEnumerable<SourcedNodeWrite<T>> items,
            RetryMode retryMode,
            CancellationToken token)
        {
            return await HandleWriteErrors(
                items,
                retryMode,
                token,
                async (currentItems, currentToken) =>
                {
                    using (CdfMetrics.Instances(view, "update").NewTimer())
                    {
                        return await upsert(currentItems, currentToken).ConfigureAwait(false);
                    }
                }).ConfigureAwait(false);
        }

        private static async Task<CogniteResult<TResult, SourcedNodeWrite<T>>> WriteChunked<T, TResult>(
            SanitizeInstancesFunc<T> sanitize,
            IEnumerable<SourcedNodeWrite<T>> items,
            BetaResourceParams options,
            CancellationToken token,
            Func<IEnumerable<SourcedNodeWrite<T>>, Task<CogniteResult<TResult, SourcedNodeWrite<T>>>> handleChunk)
        {
            if (items == null || !items.Any()) return new CogniteResult<TResult, SourcedNodeWrite<T>>(null, null);

            IEnumerable<CogniteError<SourcedNodeWrite<T>>> errors;
            (items, errors) = sanitize(items, options.SanitationMode);

            var chunks = items.ChunkBy(options.ChunkSize).ToList();
            int size = chunks.Count + (errors.Any() ? 1 : 0);

            if (size == 0) return new CogniteResult<TResult, SourcedNodeWrite<T>>(null, null);

            var results = new CogniteResult<TResult, SourcedNodeWrite<T>>[size];
            if (errors.Any())
            {
                results[size - 1] = new CogniteResult<TResult, SourcedNodeWrite<T>>(errors, null);
                if (size == 1) return results[size - 1];
            }

            var generators = chunks
                .Select<IEnumerable<SourcedNodeWrite<T>>, Func<Task>>(
                    (chunk, idx) => async () =>
                    {
                        results[idx] = await handleChunk(chunk).ConfigureAwait(false);
                    });

            await generators.RunThrottled(options.ThrottleSize, token).ConfigureAwait(false);
            return CogniteResult<TResult, SourcedNodeWrite<T>>.Merge(results);
        }

        private static async Task<CogniteResult<TResult, SourcedNodeWrite<T>>> HandleWriteErrors<T, TResult>(
            IEnumerable<SourcedNodeWrite<T>> items,
            RetryMode retryMode,
            CancellationToken token,
            Func<IEnumerable<SourcedNodeWrite<T>>, CancellationToken, Task<IEnumerable<TResult>>> write)
        {
            var errors = new List<CogniteError<SourcedNodeWrite<T>>>();
            while (items != null && items.Any() && !token.IsCancellationRequested)
            {
                try
                {
                    var updated = await write(items, token).ConfigureAwait(false);
                    return new CogniteResult<TResult, SourcedNodeWrite<T>>(errors, updated);
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
                        items = await ResultHandlers.CleanFromError(error, items, token).ConfigureAwait(false);
                    }
                }
            }
            return new CogniteResult<TResult, SourcedNodeWrite<T>>(errors, null);
        }
    }
}
