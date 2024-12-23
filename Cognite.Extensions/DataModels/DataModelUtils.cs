﻿using CogniteSdk;
using CogniteSdk.DataModels;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Text;
using Cognite.Extractor.Common;
using CogniteSdk.Resources.DataModels;
using Prometheus;
using System.Linq;
using System.Threading.Tasks;
using System.Threading;

namespace Cognite.Extensions.DataModels
{
    /// <summary>
    /// General utils for data models.
    /// </summary>
    public static class DataModelUtils
    {
        private static ILogger _logger = new NullLogger<Client>();

        internal static void SetLogger(ILogger logger)
        {
            _logger = logger;
        }

        /// <summary>
        /// Get or create the resources with the provided <paramref name="instanceIds"/> if they exist in CDF.
        /// If one or more do not exist, use the <paramref name="buildResources"/> function to construct
        /// the missing resource objects and upload them to CDF using the chunking of items and throttling
        /// passed as parameters
        /// If any items fail to be created due to missing asset, duplicated externalId, duplicated
        /// legacy name, or missing dataSetId, they can be removed before retrying by setting <paramref name="retryMode"/>
        /// </summary>
        /// <param name="resource">CogniteSdk CDM Asset resource</param>
        /// <param name="instanceIds">Instance Ids</param>
        /// <param name="buildResources">Function that builds CogniteSdk SourcedNodeWrite objects</param>
        /// <param name="sanitizationMethod">Function that sanitizes CogniteSdk SourcedNodeWrite objects</param>
        /// <param name="chunkSize">Chunk size</param>
        /// <param name="throttleSize">Throttle size</param>
        /// <param name="retryMode">How to handle failed requests</param>
        /// <param name="sanitationMode">The type of sanitation to apply to resources before creating</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>A <see cref="CogniteResult{TResult, TError}"/> containing errors that occured and a list of the created and found resources</returns>
        public static Task<CogniteResult<SourcedNode<T>, SourcedNodeWrite<T>>> GetOrCreateResourcesAsync<T, T2>(
            this T2 resource,
            IEnumerable<InstanceIdentifier> instanceIds,
            Func<IEnumerable<InstanceIdentifier>, IEnumerable<SourcedNodeWrite<T>>> buildResources,
            Func<IEnumerable<SourcedNodeWrite<T>>, SanitationMode, (IEnumerable<SourcedNodeWrite<T>>, IEnumerable<CogniteError<SourcedNodeWrite<T>>>)> sanitizationMethod,
            int chunkSize,
            int throttleSize,
            RetryMode retryMode,
            SanitationMode sanitationMode,
            CancellationToken token) where T2 : BaseDataModelResource<T>
        {
            Task<IEnumerable<SourcedNodeWrite<T>>> asyncBuildResource(IEnumerable<InstanceIdentifier> ids)
            {
                return Task.FromResult(buildResources(ids));
            }
            return GetOrCreateResourcesAsync(resource, instanceIds, asyncBuildResource, sanitizationMethod,
                chunkSize, throttleSize, retryMode, sanitationMode, token);
        }

        /// <summary>
        /// Get or create the resources with the provided <paramref name="instanceIds"/> if they exist in CDF.
        /// If one or more do not exist, use the <paramref name="buildResources"/> function to construct
        /// the missing resource objects and upload them to CDF using the chunking of items and throttling
        /// passed as parameters
        /// If any items fail to be created due to missing asset, duplicated externalId, duplicated
        /// legacy name, or missing dataSetId, they can be removed before retrying by setting <paramref name="retryMode"/>
        /// </summary>
        /// <param name="resource">CogniteSdk CDM Asset resource</param>
        /// <param name="instanceIds">External Ids</param>
        /// <param name="buildResources">Async function that builds CogniteSdk SourcedNodeWrite objects</param>
        /// <param name="sanitizationMethod">Function that sanitizes CogniteSdk SourcedNodeWrite objects</param>
        /// <param name="chunkSize">Chunk size</param>
        /// <param name="throttleSize">Throttle size</param>
        /// <param name="retryMode">How to handle failed requests</param>
        /// <param name="sanitationMode">The type of sanitation to apply to resources before creating</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>A <see cref="CogniteResult{TResult, TError}"/> containing errors that occured and a list of the created and found resources</returns>
        public static async Task<CogniteResult<SourcedNode<T>, SourcedNodeWrite<T>>> GetOrCreateResourcesAsync<T, T2>(
            T2 resource,
            IEnumerable<InstanceIdentifier> instanceIds,
            Func<IEnumerable<InstanceIdentifier>, Task<IEnumerable<SourcedNodeWrite<T>>>> buildResources,
            Func<IEnumerable<SourcedNodeWrite<T>>, SanitationMode, (IEnumerable<SourcedNodeWrite<T>>, IEnumerable<CogniteError<SourcedNodeWrite<T>>>)> sanitizationMethod,
            int chunkSize,
            int throttleSize,
            RetryMode retryMode,
            SanitationMode sanitationMode,
            CancellationToken token) where T2 : BaseDataModelResource<T>
        {
            var chunks = instanceIds
                .ChunkBy(chunkSize)
                .ToList();
            if (!chunks.Any()) return new CogniteResult<SourcedNode<T>, SourcedNodeWrite<T>>(null, null);

            var results = new CogniteResult<SourcedNode<T>, SourcedNodeWrite<T>>[chunks.Count];

            _logger.LogDebug("Getting or creating resource. Number of external ids: {Number}. Number of chunks: {Chunks}", instanceIds.Count(), chunks.Count);
            var generators = chunks
                .Select<IEnumerable<InstanceIdentifier>, Func<Task>>(
                    (chunk, idx) => async () =>
                    {
                        var result = await GetOrCreateResourcesChunk(resource, chunk,
                            buildResources, sanitizationMethod, 0, retryMode, sanitationMode, token).ConfigureAwait(false);
                        results[idx] = result;
                    });

            int taskNum = 0;
            await generators.RunThrottled(
                throttleSize,
                (_) =>
                {
                    if (chunks.Count > 1)
                        _logger.LogDebug("{MethodName} completed {NumDone}/{TotalNum} tasks",
                            nameof(GetOrCreateResourcesAsync), ++taskNum, chunks.Count);
                },
                token).ConfigureAwait(false);

            return CogniteResult<SourcedNode<T>, SourcedNodeWrite<T>>.Merge(results);
        }

        /// <summary>
        /// Ensures that all resources in <paramref name="resourcesToEnsure"/> exists in CDF.
        /// Tries to create resources and returns when all are created or have been removed
        /// due to issues with the request.
        /// If any items fail to be created due to missing asset, duplicated externalId, duplicated
        /// legacy name, or missing dataSetId, they can be removed before retrying by setting <paramref name="retryMode"/>
        /// Resources will be returned in the same order as given in <paramref name="resourcesToEnsure"/>
        /// </summary>
        /// <param name="resource">CogniteSdk CDM Asset resource</param>
        /// <param name="resourcesToEnsure">List of CogniteSdk SourcedNodeWrite objects</param>
        /// <param name="sanitizationMethod">Function that sanitizes CogniteSdk SourcedNodeWrite objects</param>
        /// <param name="chunkSize">Chunk size</param>
        /// <param name="throttleSize">Throttle size</param>
        /// <param name="retryMode">How to do retries. Keeping duplicates is not valid for
        /// this method.</param>
        /// <param name="sanitationMode">The type of sanitation to apply to resources before creating</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>A <see cref="CogniteResult{TResult, TError}"/> containing errors that occured and a list of the created resources</returns>
        public static async Task<CogniteResult<SourcedNode<T>, SourcedNodeWrite<T>>> EnsureResourcesExistsAsync<T, T2>(
            T2 resource,
            IEnumerable<SourcedNodeWrite<T>> resourcesToEnsure,
            Func<IEnumerable<SourcedNodeWrite<T>>, SanitationMode, (IEnumerable<SourcedNodeWrite<T>>, IEnumerable<CogniteError<SourcedNodeWrite<T>>>)> sanitizationMethod,
            int chunkSize,
            int throttleSize,
            RetryMode retryMode,
            SanitationMode sanitationMode,
            CancellationToken token) where T2 : BaseDataModelResource<T>
        {
            IEnumerable<CogniteError<SourcedNodeWrite<T>>> errors;
            if (sanitizationMethod == null)
            {
                throw new ArgumentNullException(nameof(sanitizationMethod));
            }
            (resourcesToEnsure, errors) = sanitizationMethod(resourcesToEnsure, sanitationMode);

            var chunks = resourcesToEnsure
                .ChunkBy(chunkSize)
                .ToList();

            int size = chunks.Count + (errors.Any() ? 1 : 0);
            var results = new CogniteResult<SourcedNode<T>, SourcedNodeWrite<T>>[size];

            if (errors.Any())
            {
                results[size - 1] = new CogniteResult<SourcedNode<T>, SourcedNodeWrite<T>>(errors, null);
                if (size == 1) return results[size - 1];
            }
            if (size == 0) return new CogniteResult<SourcedNode<T>, SourcedNodeWrite<T>>(null, null);

            _logger.LogDebug("Ensuring resources. Number of resources: {Number}. Number of chunks: {Chunks}", resourcesToEnsure.Count(), chunks.Count);
            var generators = chunks
                .Select<IEnumerable<SourcedNodeWrite<T>>, Func<Task>>(
                (chunk, idx) => async () =>
                {
                    var result = await CreateResourcesHandleErrors(resource, chunk, retryMode, token).ConfigureAwait(false);
                    results[idx] = result;
                });

            int taskNum = 0;
            await generators.RunThrottled(
                throttleSize,
                (_) =>
                {
                    if (chunks.Count > 1)
                        _logger.LogDebug("{MethodName} completed {NumDone}/{TotalNum} tasks",
                            nameof(EnsureResourcesExistsAsync), ++taskNum, chunks.Count);
                },
                token).ConfigureAwait(false);

            return CogniteResult<SourcedNode<T>, SourcedNodeWrite<T>>.Merge(results);
        }

        /// <summary>
        /// Get the resources with the provided <paramref name="ids"/>. Ignore any
        /// unknown ids
        /// </summary>
        /// <param name="resource">CogniteSdk CDM Asset resource</param>
        /// <param name="ids">List of <see cref="Identity"/> objects</param>
        /// <param name="chunkSize">Chunk size</param>
        /// <param name="throttleSize">Throttle size</param>
        /// <param name="token">Cancellation token</param>
        /// <returns></returns>
        public static async Task<IEnumerable<SourcedNode<T>>> GetResourcesByIdsIgnoreErrors<T, T2>(
            T2 resource,
            IEnumerable<Identity> ids,
            int chunkSize,
            int throttleSize,
            CancellationToken token) where T2 : BaseDataModelResource<T>
        {
            var result = new List<SourcedNode<T>>();
            object mutex = new object();

            var chunks = ids
                .ChunkBy(chunkSize)
                .ToList();

            var generators = chunks
                .Select((Func<IEnumerable<Identity>, Func<Task>>)(chunk => async () =>
                {
                    IEnumerable<SourcedInstance<T>> found;
                    using (CdfMetrics.Instances(resource.View, "retrieve").NewTimer())
                    {
                        found = await resource.RetrieveAsync(chunk.Select(x => new InstanceIdentifierWithType(InstanceType.node, x.InstanceId)), token).ConfigureAwait(false);
                    }
                    lock (mutex)
                    {
                        result.AddRange(found.Select(x => new SourcedNode<T>(x)));
                    }
                }));
            int numTasks = 0;
            await generators.RunThrottled(throttleSize, (_) =>
                _logger.LogDebug("{MethodName} completed {NumDone}/{TotalNum} tasks", nameof(GetResourcesByIdsIgnoreErrors), ++numTasks, chunks.Count),
                token).ConfigureAwait(false);
            return result;
        }

        private static async Task<CogniteResult<SourcedNode<T>, SourcedNodeWrite<T>>> GetOrCreateResourcesChunk<T, T2>(
            T2 resource,
            IEnumerable<InstanceIdentifier> instanceIds,
            Func<IEnumerable<InstanceIdentifier>, Task<IEnumerable<SourcedNodeWrite<T>>>> buildResources,
            Func<IEnumerable<SourcedNodeWrite<T>>, SanitationMode, (IEnumerable<SourcedNodeWrite<T>>, IEnumerable<CogniteError<SourcedNodeWrite<T>>>)> sanitizationMethod,
            int backoff,
            RetryMode retryMode,
            SanitationMode sanitationMode,
            CancellationToken token) where T2 : BaseDataModelResource<T>
        {
            IEnumerable<SourcedInstance<T>> found;
            using (CdfMetrics.Instances(resource.View, "retrieve").NewTimer())
            {
                var idts = instanceIds;

                try
                {
                    found = await resource.RetrieveAsync(idts.Select(x => new InstanceIdentifierWithType(InstanceType.node, x)), token).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    var err = ResultHandlers.ParseSimpleError<SourcedNodeWrite<T>>(ex, idts?.Select(x => Identity.Create(x)), null);
                    return new CogniteResult<SourcedNode<T>, SourcedNodeWrite<T>>(new[] { err }, null);
                }
            }
            _logger.LogDebug("Retrieved {Existing} times series from CDF", found.Count());

            var missing = instanceIds.Except(found.Select(ts => new InstanceIdentifier(ts.Space, ts.ExternalId))).ToList();

            if (missing.Count == 0)
            {
                return new CogniteResult<SourcedNode<T>, SourcedNodeWrite<T>>(null, found.Select(x => new SourcedNode<T>(x)));
            }

            _logger.LogDebug("Could not fetch {Missing} out of {Found} resources. Attempting to create the missing ones", missing.Count, instanceIds.Count());
            var toCreate = await buildResources(missing).ConfigureAwait(false);

            IEnumerable<CogniteError<SourcedNodeWrite<T>>> errors;
            (toCreate, errors) = sanitizationMethod(toCreate, sanitationMode);

            var result = await CreateResourcesHandleErrors(resource, toCreate, retryMode, token).ConfigureAwait(false);
            result.Results = (result.Results == null ? found : result.Results.Concat(found)).Select(x => new SourcedNode<T>(x));

            if (errors.Any())
            {
                result.Errors = result.Errors == null ? errors : result.Errors.Concat(errors);
            }

            if (!result.Errors?.Any() ?? false
                || retryMode != RetryMode.OnErrorKeepDuplicates
                && retryMode != RetryMode.OnFatalKeepDuplicates) return result;

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
            _logger.LogDebug("Found {cnt} duplicated resources, retrying", duplicatedIds.Count);

            await Task.Delay(TimeSpan.FromSeconds(0.1 * Math.Pow(2, backoff)), token).ConfigureAwait(false);
            var nextResult = await GetOrCreateResourcesChunk(resource, duplicatedIds,
                buildResources, sanitizationMethod, backoff + 1, retryMode, sanitationMode, token)
                .ConfigureAwait(false);
            result = result.Merge(nextResult);

            return result;
        }

        private static async Task<CogniteResult<SourcedNode<T>, SourcedNodeWrite<T>>> CreateResourcesHandleErrors<T, T2>(
            T2 resource,
            IEnumerable<SourcedNodeWrite<T>> toCreate,
            RetryMode retryMode,
            CancellationToken token) where T2 : BaseDataModelResource<T>
        {
            var errors = new List<CogniteError<SourcedNodeWrite<T>>>();
            while (toCreate != null && toCreate.Any() && !token.IsCancellationRequested)
            {
                try
                {
                    IEnumerable<SlimInstance> newResource;
                    using (CdfMetrics.Instances(resource.View, "create").NewTimer())
                    {
                        newResource = await resource.UpsertAsync(toCreate, null, token).ConfigureAwait(false);
                    }

                    _logger.LogDebug("Created {New} new resources in CDF", newResource.Count());
                    var toCreateDict = new Dictionary<Identity, T>();
                    foreach (var cr in toCreate)
                    {
                        toCreateDict[new Identity(new InstanceIdentifier(cr.Space, cr.ExternalId))] = cr.Properties;
                    }

                    return new CogniteResult<SourcedNode<T>, SourcedNodeWrite<T>>(errors, newResource.Select(x => new SourcedNode<T>(x, toCreateDict[new Identity(new InstanceIdentifier(x.Space, x.ExternalId))])));
                }
                catch (Exception ex)
                {
                    _logger.LogDebug("Failed to create {cnt} resources: {msg}",
                        toCreate.Count(), ex.Message);
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
                        toCreate = ResultHandlers.CleanFromError(error, toCreate);
                    }
                }
            }
            return new CogniteResult<SourcedNode<T>, SourcedNodeWrite<T>>(errors, null);
        }

        /// <summary>
        /// Upsert resources.
        /// If any items fail to be created due to duplicated instance ids, they can be removed before retrying by setting <paramref name="retryMode"/>
        /// Resources will be returned in the same order as given in <paramref name="items"/>
        /// </summary>
        /// <param name="resource">CogniteSdk CDM Asset resource</param>
        /// <param name="items">List of resource updates</param>
        /// <param name="sanitizationMethod">Function that sanitizes CogniteSdk SourcedNodeWrite objects</param>
        /// <param name="chunkSize">Maximum number of resources per request</param>
        /// <param name="throttleSize">Maximum number of parallel requests</param>
        /// <param name="retryMode">How to handle retries</param>
        /// <param name="sanitationMode">What kind of pre-request sanitation to perform</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>A <see cref="CogniteResult{TResult, TError}"/> containing errors that occured and a list of the updated resources</returns>
        public static async Task<CogniteResult<SlimInstance, SourcedNodeWrite<T>>> UpsertAsync<T, T2>(
            T2 resource,
            IEnumerable<SourcedNodeWrite<T>> items,
            Func<IEnumerable<SourcedNodeWrite<T>>, SanitationMode, (IEnumerable<SourcedNodeWrite<T>>, IEnumerable<CogniteError<SourcedNodeWrite<T>>>)> sanitizationMethod,
            int chunkSize,
            int throttleSize,
            RetryMode retryMode,
            SanitationMode sanitationMode,
            CancellationToken token) where T2 : BaseDataModelResource<T>
        {
            IEnumerable<CogniteError<SourcedNodeWrite<T>>> errors;
            if (sanitizationMethod == null)
            {
                throw new ArgumentNullException(nameof(sanitizationMethod));
            }
            (items, errors) = sanitizationMethod(items, sanitationMode);

            var chunks = items
                .ChunkBy(chunkSize)
                .ToList();

            int size = chunks.Count + (errors.Any() ? 1 : 0);
            var results = new CogniteResult<SlimInstance, SourcedNodeWrite<T>>[size];

            if (errors.Any())
            {
                results[size - 1] = new CogniteResult<SlimInstance, SourcedNodeWrite<T>>(errors, null);
                if (size == 1) return results[size - 1];
            }
            if (size == 0) return new CogniteResult<SlimInstance, SourcedNodeWrite<T>>(null, null);

            _logger.LogDebug("Updating resources. Number of resources: {Number}. Number of chunks: {Chunks}", items.Count(), chunks.Count);
            var generators = chunks
                .Select<IEnumerable<SourcedNodeWrite<T>>, Func<Task>>(
                (chunk, idx) => async () =>
                {
                    var result = await UpsertResourcesHandleErrors(resource, chunk, retryMode, token).ConfigureAwait(false);
                    results[idx] = result;
                });

            int taskNum = 0;
            await generators.RunThrottled(
                throttleSize,
                (_) =>
                {
                    if (chunks.Count > 1)
                        _logger.LogDebug("{MethodName} completed {NumDone}/{TotalNum} tasks",
                            nameof(UpsertAsync), ++taskNum, chunks.Count);
                },
                token).ConfigureAwait(false);

            return CogniteResult<SlimInstance, SourcedNodeWrite<T>>.Merge(results);
        }

        private static async Task<CogniteResult<SlimInstance, SourcedNodeWrite<T>>> UpsertResourcesHandleErrors<T, T2>(
            T2 resource,
            IEnumerable<SourcedNodeWrite<T>> items,
            RetryMode retryMode,
            CancellationToken token) where T2 : BaseDataModelResource<T>
        {
            var errors = new List<CogniteError<SourcedNodeWrite<T>>>();
            while (items != null && items.Any() && !token.IsCancellationRequested)
            {
                try
                {
                    var toUpdate = new List<SourcedNodeWrite<T>>();

                    IEnumerable<SlimInstance> updated;
                    using (CdfMetrics.Instances(resource.View, "update").NewTimer())
                    {
                        updated = await resource.UpsertAsync(items, null, token).ConfigureAwait(false);
                    }

                    _logger.LogDebug("Updated {Count} resources in CDF", updated.Count());
                    return new CogniteResult<SlimInstance, SourcedNodeWrite<T>>(errors, updated);
                }
                catch (Exception ex)
                {
                    _logger.LogDebug("Failed to create {Count} resources: {Message}",
                        items.Count(), ex.Message);
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
                        items = ResultHandlers.CleanFromError(error, items);
                    }
                }
            }

            return new CogniteResult<SlimInstance, SourcedNodeWrite<T>>(errors, null);
        }

        /// <summary>
        /// Create a view from this container, mapping over all properties.
        /// Note that direct relation properties constrained to a container
        /// will be mapped over to point to a view, so if a relation is constrained to the
        /// container (containerSpace, containerExternalId), it will point to
        /// the view given by (containerSpace, <paramref name="version"/>, containerExternalId)
        /// in the created view.
        /// 
        /// This method is convenient since you need views in order to query the data,
        /// so this can reduce boilerplate.
        /// 
        /// The new view will have the same name, description, externalId, and space
        /// as the container.
        /// </summary>
        /// <param name="container">Container to convert</param>
        /// <param name="version">Version of the created view</param>
        /// <param name="baseViews">List of views this view should implement</param>
        /// <returns>Mapped view</returns>
        public static ViewCreate ToView(this ContainerCreate container, string version, params ViewIdentifier[] baseViews)
        {
            if (container == null) throw new ArgumentNullException(nameof(container));

            var properties = new Dictionary<string, ICreateViewProperty>();
            foreach (var kvp in container.Properties)
            {
                if (kvp.Value == null) throw new InvalidOperationException("Property value is null");
                properties[kvp.Key] = new ViewPropertyCreate
                {
                    Container = new ContainerIdentifier(container.Space, container.ExternalId),
                    Description = kvp.Value.Description,
                    Name = kvp.Value.Name,
                    ContainerPropertyIdentifier = kvp.Key,
                    Source = kvp.Value.Type is DirectRelationPropertyType dt && dt.Container != null ?
                        new ViewIdentifier(container.Space, dt.Container.ExternalId, version) : null
                };
            }

            return new ViewCreate
            {
                Description = container.Description,
                ExternalId = container.ExternalId,
                Name = container.Name,
                Space = container.Space,
                Version = version,
                Properties = properties,
                Implements = baseViews
            };
        }

    }
}
