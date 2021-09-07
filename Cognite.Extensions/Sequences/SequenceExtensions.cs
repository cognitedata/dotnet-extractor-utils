using Cognite.Extractor.Common;
using CogniteSdk;
using CogniteSdk.Resources;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Prometheus;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.Extensions
{
    /// <summary>
    /// Extensions to sequences.
    /// </summary>
    public static class SequenceExtensions
    {
        private static ILogger _logger = new NullLogger<Client>();

        internal static void SetLogger(ILogger logger)
        {
            _logger = logger;
        }

        /// <summary>
        /// Get or create the sequences with the provided <paramref name="externalIds"/>.
        /// If one or more do not exist, use the <paramref name="buildSequences"/> function to construct
        /// the missing sequence objects and upload them to CDF using the chunking of items and throttling
        /// passed as parameters
        /// If any items fail to be created due to missing asset, duplicated externalId, issues with the columns,
        /// or missing dataSetId, they can be removed before retrying by setting <paramref name="retryMode"/>
        /// </summary>
        /// <param name="sequences">Cognite sequences resource</param>
        /// <param name="externalIds">External Ids</param>
        /// <param name="buildSequences">Function that builds CogniteSdk SequenceCreate objects</param>
        /// <param name="chunkSize">Chunk size</param>
        /// <param name="throttleSize">Throttle size</param>
        /// <param name="retryMode">How to handle failed requests</param>
        /// <param name="sanitationMode">The type of sanitation to apply to sequences before creating</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>A <see cref="CogniteResult"/> containing errors that occured and a list of the created and found sequences</returns>
        public static Task<CogniteResult<Sequence>> GetOrCreateAsync(
            this SequencesResource sequences,
            IEnumerable<string> externalIds,
            Func<IEnumerable<string>, IEnumerable<SequenceCreate>> buildSequences,
            int chunkSize,
            int throttleSize,
            RetryMode retryMode,
            SanitationMode sanitationMode,
            CancellationToken token)
        {
            Task<IEnumerable<SequenceCreate>> asyncBuildSequences(IEnumerable<string> ids)
            {
                return Task.FromResult(buildSequences(ids));
            }
            return sequences.GetOrCreateAsync(externalIds, asyncBuildSequences,
                chunkSize, throttleSize, retryMode, sanitationMode, token);
        }

        /// <summary>
        /// Get or create the sequences with the provided <paramref name="externalIds"/>.
        /// If one or more do not exist, use the <paramref name="buildSequences"/> function to construct
        /// the missing sequence objects and upload them to CDF using the chunking of items and throttling
        /// passed as parameters
        /// If any items fail to be created due to missing asset, duplicated externalId, issues with the columns,
        /// or missing dataSetId, they can be removed before retrying by setting <paramref name="retryMode"/>
        /// </summary>
        /// <param name="sequences">Cognite client</param>
        /// <param name="externalIds">External Ids</param>
        /// <param name="buildSequences">Async function that builds CogniteSdk SequenceCreate objects</param>
        /// <param name="chunkSize">Chunk size</param>
        /// <param name="throttleSize">Throttle size</param>
        /// <param name="retryMode">How to handle failed requests</param>
        /// <param name="sanitationMode">The type of sanitation to apply to sequences before creating</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>A <see cref="CogniteResult"/> containing errors that occured and a list of the created and found sequences</returns>
        public static async Task<CogniteResult<Sequence>> GetOrCreateAsync(
            this SequencesResource sequences,
            IEnumerable<string> externalIds,
            Func<IEnumerable<string>, Task<IEnumerable<SequenceCreate>>> buildSequences,
            int chunkSize,
            int throttleSize,
            RetryMode retryMode,
            SanitationMode sanitationMode,
            CancellationToken token)
        {
            var chunks = externalIds
                .ChunkBy(chunkSize)
                .ToList();
            if (!chunks.Any()) return new CogniteResult<Sequence>(null, null);

            var results = new CogniteResult<Sequence>[chunks.Count];

            _logger.LogDebug("Getting or creating sequences. Number of external ids: {Number}. Number of chunks: {Chunks}", externalIds.Count(), chunks.Count);
            var generators = chunks
                .Select<IEnumerable<string>, Func<Task>>(
                    (chunk, idx) => async () => {
                        var result = await GetOrCreateSequencesChunk(sequences, chunk,
                            buildSequences, 0, retryMode, sanitationMode, token).ConfigureAwait(false);
                        results[idx] = result;
                    });

            int taskNum = 0;
            await generators.RunThrottled(
                throttleSize,
                (_) => {
                    if (chunks.Count > 1)
                        _logger.LogDebug("{MethodName} completed {NumDone}/{TotalNum} tasks",
                            nameof(GetOrCreateAsync), ++taskNum, chunks.Count);
                },
                token).ConfigureAwait(false);

            return CogniteResult<Sequence>.Merge(results);
        }
        /// <summary>
        /// Ensures that all sequences in <paramref name="sequencesToEnsure"/> exist in CDF.
        /// Tries to create the sequences and returns when all are created or have been removed
        /// due to issues with the request.
        /// If any items fail to be created due to missing asset, duplicated externalId, issues with the columns,
        /// or missing dataSetId, they can be removed before retrying by setting <paramref name="retryMode"/>
        /// Sequences will be returned in the same order as given in <paramref name="sequencesToEnsure"/>
        /// </summary>
        /// <param name="sequences">Cognite client</param>
        /// <param name="sequencesToEnsure">List of CogniteSdk SequenceCreate objects</param>
        /// <param name="chunkSize">Chunk size</param>
        /// <param name="throttleSize">Throttle size</param>
        /// <param name="retryMode">How to do retries. Keeping duplicates is not valid for
        /// this method.</param>
        /// <param name="sanitationMode">The type of sanitation to apply to sequences before creating</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>A <see cref="CogniteResult"/> containing errors that occured and a list of the created sequences</returns>
        public static async Task<CogniteResult<Sequence>> EnsureExistsAsync(
            this SequencesResource sequences,
            IEnumerable<SequenceCreate> sequencesToEnsure,
            int chunkSize,
            int throttleSize,
            RetryMode retryMode,
            SanitationMode sanitationMode,
            CancellationToken token)
        {
            IEnumerable<CogniteError> errors;
            (sequencesToEnsure, errors) = Sanitation.CleanSequenceRequest(sequencesToEnsure, sanitationMode);

            var chunks = sequencesToEnsure
                .ChunkBy(chunkSize)
                .ToList();

            int size = chunks.Count + (errors.Any() ? 1 : 0);
            var results = new CogniteResult<Sequence>[size];

            if (errors.Any())
            {
                results[size - 1] = new CogniteResult<Sequence>(errors, null);
                if (size == 1) return results[size - 1];
            }
            if (size == 0) return new CogniteResult<Sequence>(null, null);

            _logger.LogDebug("Ensuring sequences. Number of sequences: {Number}. Number of chunks: {Chunks}", sequencesToEnsure.Count(), chunks.Count);
            var generators = chunks
                .Select<IEnumerable<SequenceCreate>, Func<Task>>(
                (chunk, idx) => async () => {
                    var result = await CreateSequencesHandleErrors(sequences, chunk, retryMode, token).ConfigureAwait(false);
                    results[idx] = result;
                });

            int taskNum = 0;
            await generators.RunThrottled(
                throttleSize,
                (_) => {
                    if (chunks.Count > 1)
                        _logger.LogDebug("{MethodName} completed {NumDone}/{TotalNum} tasks",
                            nameof(EnsureExistsAsync), ++taskNum, chunks.Count);
                },
                token).ConfigureAwait(false);

            return CogniteResult<Sequence>.Merge(results);
        }

        /// <summary>
        /// Get the sequences with the provided <paramref name="ids"/>. Ignore any
        /// unknown ids
        /// </summary>
        /// <param name="sequences">A CogniteSdk Sequences resource</param>
        /// <param name="ids">List of <see cref="Identity"/> objects</param>
        /// <param name="chunkSize">Chunk size</param>
        /// <param name="throttleSize">Throttle size</param>
        /// <param name="token">Cancellation token</param>
        /// <returns></returns>
        public static async Task<IEnumerable<Sequence>> GetByIdsIgnoreErrors(
            this SequencesResource sequences,
            IEnumerable<Identity> ids,
            int chunkSize,
            int throttleSize,
            CancellationToken token)
        {
            var result = new List<Sequence>();
            object mutex = new object();

            var chunks = ids
                .ChunkBy(chunkSize)
                .ToList();

            var generators = chunks
                .Select<IEnumerable<Identity>, Func<Task>>(
                chunk => async () => {
                    IEnumerable<Sequence> found;
                    using (CdfMetrics.Sequences.WithLabels("retrieve").NewTimer())
                    {
                        found = await sequences.RetrieveAsync(chunk, true, token).ConfigureAwait(false);
                    }
                    lock (mutex)
                    {
                        result.AddRange(found);
                    }
                });
            int numTasks = 0;
            await generators.RunThrottled(throttleSize, (_) =>
                _logger.LogDebug("{MethodName} completed {NumDone}/{TotalNum} tasks", nameof(GetByIdsIgnoreErrors), ++numTasks, chunks.Count),
                token).ConfigureAwait(false);
            return result;
        }

        private static async Task<CogniteResult<Sequence>> GetOrCreateSequencesChunk(
            SequencesResource client,
            IEnumerable<string> externalIds,
            Func<IEnumerable<string>, Task<IEnumerable<SequenceCreate>>> buildSequences,
            int backoff,
            RetryMode retryMode,
            SanitationMode sanitationMode,
            CancellationToken token)
        {
            IEnumerable<Sequence> found;
            using (CdfMetrics.Sequences.WithLabels("retrieve").NewTimer())
            {
                found = await client.RetrieveAsync(externalIds.Select(id => new Identity(id)), true, token).ConfigureAwait(false);
            }
            _logger.LogDebug("Retrieved {Existing} sequences from CDF", found.Count());

            var missing = externalIds.Except(found.Select(ts => ts.ExternalId)).ToList();

            if (!missing.Any())
            {
                return new CogniteResult<Sequence>(null, found);
            }

            _logger.LogDebug("Could not fetch {Missing} out of {Found} sequences. Attempting to create the missing ones", missing.Count, externalIds.Count());
            var toCreate = await buildSequences(missing).ConfigureAwait(false);

            IEnumerable<CogniteError> errors;
            (toCreate, errors) = Sanitation.CleanSequenceRequest(toCreate, sanitationMode);

            var result = await CreateSequencesHandleErrors(client, toCreate, retryMode, token).ConfigureAwait(false);
            result.Results = result.Results == null ? found : result.Results.Concat(found);

            if (errors.Any())
            {
                result.Errors = result.Errors == null ? errors : result.Errors.Concat(errors);
            }

            if (!result.Errors?.Any() ?? false
                || retryMode != RetryMode.OnErrorKeepDuplicates
                && retryMode != RetryMode.OnFatalKeepDuplicates) return result;

            var duplicateErrors = result.Errors.Where(err =>
                err.Resource == ResourceType.ExternalId
                && err.Type == ErrorType.ItemExists)
                .ToList();

            var duplicatedIds = new HashSet<string>();
            if (duplicateErrors.Any())
            {
                foreach (var error in duplicateErrors)
                {
                    if (!error.Values?.Any() ?? false) continue;
                    foreach (var idt in error.Values) duplicatedIds.Add(idt.ExternalId);
                }
            }

            if (!duplicatedIds.Any()) return result;
            _logger.LogDebug("Found {cnt} duplicated sequences, retrying", duplicatedIds.Count);

            await Task.Delay(TimeSpan.FromSeconds(0.1 * Math.Pow(2, backoff)), token).ConfigureAwait(false);
            var nextResult = await GetOrCreateSequencesChunk(client, duplicatedIds,
                buildSequences, backoff + 1, retryMode, sanitationMode, token)
                .ConfigureAwait(false);
            result = result.Merge(nextResult);

            return result;
        }

        private static async Task<CogniteResult<Sequence>> CreateSequencesHandleErrors(
            SequencesResource sequences,
            IEnumerable<SequenceCreate> toCreate,
            RetryMode retryMode,
            CancellationToken token)
        {
            var errors = new List<CogniteError>();
            while (toCreate != null && toCreate.Any() && !token.IsCancellationRequested)
            {
                try
                {
                    IEnumerable<Sequence> newSequences;
                    using (CdfMetrics.Sequences.WithLabels("create").NewTimer())
                    {
                        newSequences = await sequences.CreateAsync(toCreate, token).ConfigureAwait(false);
                    }

                    _logger.LogDebug("Created {New} new sequences in CDF", newSequences.Count());
                    return new CogniteResult<Sequence>(errors, newSequences);
                }
                catch (Exception ex)
                {
                    _logger.LogDebug("Failed to create {cnt} sequences: {msg}",
                        toCreate.Count(), ex.Message);
                    var error = ResultHandlers.ParseException(ex, RequestType.CreateSequences);
                    errors.Add(error);
                    if (error.Type == ErrorType.FatalFailure
                        && (retryMode == RetryMode.OnFatal
                            || retryMode == RetryMode.OnFatalKeepDuplicates))
                    {
                        await Task.Delay(1000, token).ConfigureAwait(false);
                    }
                    else if (retryMode == RetryMode.None) break;
                    else
                    {
                        toCreate = ResultHandlers.CleanFromError(error, toCreate);
                    }
                }
            }
            return new CogniteResult<Sequence>(errors, null);
        }

        /// <summary>
        /// Insert sequence rows into given list of sequences.
        /// Chunks by both number of sequences per request, and number of rows per sequence.
        /// Optionally sanitizes the request, and handles errors that occur while running.
        /// </summary>
        /// <param name="sequences">CogniteSdk sequence resource object</param>
        /// <param name="toCreate">List of sequences and rows to create</param>
        /// <param name="keyChunkSize">Maximum number of sequences in each request</param>
        /// <param name="valueChunkSize">Maximum number of sequence rows per sequence</param>
        /// <param name="sequencesChunk">Maximum number of sequences to read at a time if reading to handle errors</param>
        /// <param name="throttleSize">Maximum number of parallel requests</param>
        /// <param name="retryMode">How to handle errors</param>
        /// <param name="sanitationMode">How to sanitize the request before sending</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>Result containing optional errors if something went wrong</returns>
        public static async Task<CogniteResult> InsertAsync(
            this SequencesResource sequences,
            IEnumerable<SequenceDataCreate> toCreate,
            int keyChunkSize,
            int valueChunkSize,
            int sequencesChunk,
            int throttleSize,
            RetryMode retryMode,
            SanitationMode sanitationMode,
            CancellationToken token)
        {
            IEnumerable<CogniteError> errors;
            (toCreate, errors) = Sanitation.CleanSequenceDataRequest(toCreate, sanitationMode);

            var dict = toCreate.ToDictionary(create => create.Id.HasValue ? Identity.Create(create.Id.Value) : Identity.Create(create.ExternalId), new IdentityComparer());
            var chunks = dict
                .Select(kvp => (kvp.Key, kvp.Value.Rows))
                .ChunkBy(keyChunkSize, valueChunkSize)
                .Select(chunk => chunk
                    .Select(pair => new SequenceDataCreate
                    {
                        Columns = dict[pair.Key].Columns,
                        Id = pair.Key.Id,
                        ExternalId = pair.Key.ExternalId,
                        Rows = pair.Values
                    }))
                .ToList();

            int size = chunks.Count + (errors.Any() ? 1 : 0);
            var results = new CogniteResult[size];

            if (errors.Any())
            {
                results[size - 1] = new CogniteResult<Sequence>(errors, null);
                if (size == 1) return results[size - 1];
            }
            if (size == 0) return new CogniteResult<Sequence>(null, null);

            _logger.LogDebug("Inserting sequences rows. Number of sequences: {Number}. Number of chunks: {Chunks}", toCreate.Count(), chunks.Count);
            var generators = chunks
                .Select<IEnumerable<SequenceDataCreate>, Func<Task>>(
                (chunk, idx) => async () => {
                    var result = await InsertSequenceRowsHandleErrors(sequences, chunk, sequencesChunk, throttleSize, retryMode, token).ConfigureAwait(false);
                    results[idx] = result;
                });

            int taskNum = 0;
            await generators.RunThrottled(
                throttleSize,
                (_) => {
                    if (chunks.Count > 1)
                        _logger.LogDebug("{MethodName} completed {NumDone}/{TotalNum} tasks",
                            nameof(InsertAsync), ++taskNum, chunks.Count);
                },
                token).ConfigureAwait(false);

            return CogniteResult.Merge(results);
        }

        private static async Task<CogniteResult> InsertSequenceRowsHandleErrors(
            SequencesResource sequences,
            IEnumerable<SequenceDataCreate> toCreate,
            int sequencesChunk,
            int throttleSize,
            RetryMode retryMode,
            CancellationToken token)
        {
            var errors = new List<CogniteError>();
            while (toCreate != null && toCreate.Any() && !token.IsCancellationRequested)
            {
                try
                {
                    using (CdfMetrics.SequenceRows.WithLabels("create"))
                    {
                        await sequences.CreateRowsAsync(toCreate, token).ConfigureAwait(false);
                    }

                    _logger.LogDebug("Created {rows} rows for {seq} sequences in CDF", toCreate.Sum(seq => seq.Rows.Count()), toCreate.Count());
                    return new CogniteResult(errors);
                }
                catch (Exception ex)
                {
                    _logger.LogDebug("Failed to create rows for {seq} sequences", toCreate.Count());
                    var error = ResultHandlers.ParseException(ex, RequestType.CreateSequences);
                    errors.Add(error);
                    if (error.Type == ErrorType.FatalFailure
                        && (retryMode == RetryMode.OnFatal
                            || retryMode == RetryMode.OnFatalKeepDuplicates))
                    {
                        await Task.Delay(1000, token).ConfigureAwait(false);
                    }
                    else if (retryMode == RetryMode.None) break;
                    else
                    {
                        toCreate = await ResultHandlers.CleanFromError(sequences, error, toCreate, sequencesChunk, throttleSize, token).ConfigureAwait(false);
                    }
                }
            }

            return new CogniteResult(errors);
        }
    }
}
