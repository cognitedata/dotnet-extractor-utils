using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extractor.Common;
using CogniteSdk;
using CogniteSdk.Beta;
using CogniteSdk.Resources.Beta;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Cognite.Extensions
{
    /// <summary>
    /// Extension utility methods for the beta streamrecords resource.
    /// </summary>
    public static class StreamRecordExtensions
    {
        private static ILogger _logger = new NullLogger<Client>();

        internal static void SetLogger(ILogger logger)
        {
            _logger = logger;
        }

        /// <summary>
        /// Retrieve a stream, or create it if it does not exist.
        /// </summary>
        /// <param name="streams">Stream resource</param>
        /// <param name="stream">Stream to create</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>Created or retrieved stream.</returns>
        /// <exception cref="ArgumentNullException"></exception>
        public static async Task<Stream> GetOrCreateStreamAsync(
            this StreamRecordsResource streams,
            StreamWrite stream,
            CancellationToken token
        )
        {
            if (stream is null) throw new ArgumentNullException(nameof(stream));
            try
            {
                using (CdfMetrics.StreamRecords.WithLabels("retrieve_stream"))
                {
                    var res = await streams.RetrieveStreamAsync(stream.ExternalId, token: token).ConfigureAwait(false);
                    return res;
                }
            }
            catch (ResponseException ex) when (ex.Code == 404)
            {
            }
            using (CdfMetrics.StreamRecords.WithLabels("create_stream"))
            {
                _logger.LogInformation("Creating new stream with ID {Stream}", stream.ExternalId);
                return await streams.CreateStreamAsync(stream, token).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Insert the given stream records into <paramref name="stream"/>. The stream
        /// must exist.
        /// </summary>
        /// <param name="streams">Stream resource</param>
        /// <param name="stream">Stream to ingest into</param>
        /// <param name="records">Stream records to insert</param>
        /// <param name="chunkSize">Maximum number of records per request</param>
        /// <param name="throttleSize">Maximum number of parallel requests</param>
        /// <param name="token">Cancellation token</param>
        public static async Task InsertRecordsAsync(
            this StreamRecordsResource streams,
            string stream,
            ICollection<StreamRecordWrite> records,
            int chunkSize,
            int throttleSize,
            CancellationToken token
        )
        {
            var chunks = records.ChunkBy(chunkSize);

            var generators = chunks
                .Select<IEnumerable<StreamRecordWrite>, Func<Task>>(
                    chunk => async () =>
                    {
                        using (CdfMetrics.StreamRecords.WithLabels("ingest_instances"))
                        {
                            await streams.IngestAsync(stream, chunk, token).ConfigureAwait(false);
                        }
                    }
                );
            int numTasks = 0;
            await generators
                .RunThrottled(throttleSize, (_) =>
                    _logger.LogDebug("{MethodName} completed {Num}/{Total} tasks", nameof(InsertRecordsAsync), ++numTasks,
                        Math.Ceiling((double)records.Count / chunkSize)), token)
                .ConfigureAwait(false);
        }
    }
}