using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using CogniteSdk;
using Microsoft.Extensions.Logging;

namespace ExtractorUtils
{
    /// <summary>
    /// Class with utility methods supporting extraction of data into CDF.
    /// These methods complement the ones offered by the <see cref="Client"/> and use a
    /// <see cref="CogniteConfig"/> object to determine chunking of data and throttling of
    /// requests against the client
    /// </summary>
    public class CogniteDestination
    {
        private Client _client;
        private ILogger<CogniteDestination> _logger;
        private CogniteConfig _config;

        /// <summary>
        /// Initializes the Cognite destination with the provided parameters
        /// </summary>
        /// <param name="client"><see cref="Client"/> object</param>
        /// <param name="logger">Logger</param>
        /// <param name="config">Configuration object</param>
        public CogniteDestination(Client client, ILogger<CogniteDestination> logger, CogniteConfig config)
        {
            _client = client;
            _logger = logger;
            _config = config;
            CogniteClientExtensions.SetLogger(_logger);
        }

        /// <summary>
        /// Ensures the the time series with the provided <paramref name="externalIds"/> exist in CDF.
        /// If one or more do not exist, use the <paramref name="buildTimeSeries"/> function to construct
        /// the missing time series objects and upload them to CDF.
        /// This method uses the <see cref="CogniteConfig"/> object to determine chunking of items and throttling
        /// against CDF 
        /// </summary>
        /// <param name="externalIds">External Ids</param>
        /// <param name="buildTimeSeries">Function that builds <see cref="TimeSeriesCreate"/> objects</param>
        /// <param name="token">Cancellation token</param>
        /// <returns></returns>
        public async Task<IEnumerable<TimeSeries>> GetOrCreateTimeSeriesAsync(
            IEnumerable<string> externalIds,
            Func<IEnumerable<string>, IEnumerable<TimeSeriesCreate>> buildTimeSeries,
            CancellationToken token)
        {
            _logger.LogInformation("Getting or creating {Number} time series in CDF", externalIds.Count());
            return await _client.GetOrCreateTimeSeriesAsync(
                externalIds,
                buildTimeSeries,
                _config.CdfChunking.TimeSeries,
                _config.CdfThrottling.TimeSeries,
                token);
        }

        /// <summary>
        /// Ensures that all time series in <paramref name="timeSeries"/> exist in CDF.
        /// Tries to create the time series and returns when all are created or reported as 
        /// duplicates (already exist in CDF)
        /// </summary>
        /// <param name="timeSeries">List of <see cref="TimeSeriesCreate"/> objects</param>
        /// <param name="token">Cancellation token</param>
        /// <returns></returns>
        public async Task EnsureTimeSeriesExistsAsync(
            IEnumerable<TimeSeriesCreate> timeSeries, 
            CancellationToken token)
        {
            _logger.LogInformation("Ensuring that {Number} time series exist in CDF", timeSeries.Count());
            await _client.EnsureTimeSeriesExistsAsync(
                timeSeries,
                _config.CdfChunking.TimeSeries,
                _config.CdfThrottling.TimeSeries,
                token);
        }
    }

    /// <summary>
    /// Extension utility methods for <see cref="Client"/>
    /// </summary>
    public static class CogniteClientExtensions
    {
        private static ILogger _logger = Logging.GetDefault();

        internal static void SetLogger(ILogger logger) {
            _logger = logger;
        }

        /// <summary>
        /// Verifies that the <paramref name="client"/> configured according to <paramref name="config"/>
        /// can access Cognite Data Fusion
        /// </summary>
        /// <param name="client">Cognite SDK client</param>
        /// <param name="config">Configuration object</param>
        /// <param name="token">Cancellation token</param>
        /// <exception cref="CogniteUtilsException">Thrown when credentials are invalid
        /// or the client cannot be used to access CDF resources</exception>
        public async static Task TestCogniteConfig(this Client client, CogniteConfig config, CancellationToken token)
        {
            if (config == null) {
                throw new CogniteUtilsException("Cognite configuration missing");
            }
            
            if (config?.Project?.TrimToNull() == null)
            {
                throw new CogniteUtilsException("CDF project is not configured");
            }

            var loginStatus = await client.Login.StatusAsync(token);
            if (!loginStatus.LoggedIn)
            {
                throw new CogniteUtilsException("CDF credentials are invalid");
            }
            if (!loginStatus.Project.Equals(config.Project))
            {
                throw new CogniteUtilsException($"CDF credentials are not associated with project {config.Project}");
            }
        }

        /// <summary>
        /// Get or create the time series with the provided <paramref name="externalIds"/> exist in CDF.
        /// If one or more do not exist, use the <paramref name="buildTimeSeries"/> function to construct
        /// the missing time series objects and upload them to CDF using the chunking of items and throttling
        /// passed as parameters
        /// </summary>
        /// <param name="client">Cognite client</param>
        /// <param name="externalIds">External Ids</param>
        /// <param name="buildTimeSeries">Function that builds <see cref="TimeSeriesCreate"/> objects</param>
        /// <param name="chunkSize">Chunk size</param>
        /// <param name="throttleSize">Throttle size</param>
        /// <param name="token">Cancellation token</param>
        /// <returns></returns>
        public static async Task<IEnumerable<TimeSeries>> GetOrCreateTimeSeriesAsync(
            this Client client,
            IEnumerable<string> externalIds,
            Func<IEnumerable<string>, IEnumerable<TimeSeriesCreate>> buildTimeSeries,
            int chunkSize,
            int throttleSize,
            CancellationToken token)
        {
            var result = new List<TimeSeries>();
            var chunks = externalIds
                .ChunkBy(chunkSize);
            _logger.LogDebug("Getting or creating time series. Number of external ids: {Number}. Number of chunks: {Chunks}", externalIds.Count(), chunks.Count());
            var generators = chunks
                .Select<IEnumerable<string>, Func<Task>>(
                    chunk => async () => {
                        var existing = await GetOrCreateTimeSeriesChunk(client, chunk, buildTimeSeries, 0, token);
                        result.AddRange(existing);
                    });
            await generators.RunThrottled(throttleSize, token);
            return result;
        }

        /// <summary>
        /// Ensures that all time series in <paramref name="timeSeries"/> exist in CDF.
        /// Tries to create the time series and returns when all are created or reported as 
        /// duplicates (already exist in CDF)
        /// </summary>
        /// <param name="client">Cognite client</param>
        /// <param name="timeSeries">List of <see cref="TimeSeriesCreate"/> objects</param>
        /// <param name="chunkSize">Chunk size</param>
        /// <param name="throttleSize">Throttle size</param>
        /// <param name="token">Cancellation token</param>
        public static async Task EnsureTimeSeriesExistsAsync(
            this Client client,
            IEnumerable<TimeSeriesCreate> timeSeries, 
            int chunkSize, 
            int throttleSize,
            CancellationToken token)
        {
            var chunks = timeSeries
                .ChunkBy(chunkSize);
            _logger.LogDebug("Ensuring time series. Number of time series: {Number}. Number of chunks: {Chunks}", timeSeries.Count(), chunks.Count());
            var generators = chunks
                .Select<IEnumerable<TimeSeriesCreate>, Func<Task>>(
                chunk => async () => {
                    await EnsureTimeSeriesChunk(client, chunk, token);
                });
            await generators.RunThrottled(throttleSize, token);
        }


        private static async Task<IEnumerable<TimeSeries>> GetOrCreateTimeSeriesChunk(
            Client client,
            IEnumerable<string> externalIds,
            Func<IEnumerable<string>, IEnumerable<TimeSeriesCreate>> buildTimeSeries,
            int backoff,
            CancellationToken token)
        {
            var missing = new HashSet<string>();
            try
            {
                var existingTs = await client.TimeSeries.RetrieveAsync(externalIds.Select(id => new Identity(id)), token);
                _logger.LogDebug("Retrieved {Existing} times series from CDF", existingTs.Count());
                return existingTs;
            }
            catch (ResponseException e) when (e.Code == 400 && e.Missing.Any()){
                foreach (var ts in e.Missing)
                {
                    if (ts.TryGetValue("externalId", out MultiValue value))
                    {
                        missing.Add(value.ToString());
                    }
                }
                
            }

            _logger.LogDebug("Could not fetch {Missing} out of {Found} time series. Attempting to create the missing ones", missing.Count, externalIds.Count());
            var toGet = new HashSet<string>(externalIds
                .Where(e => !missing.Contains(e)));
            var created = new List<TimeSeries>();

            try
            {
                var newTs = await client.TimeSeries.CreateAsync(buildTimeSeries(missing), token);
                created.AddRange(newTs);
                _logger.LogDebug("Created {New} new time series in CDF", newTs.Count());
            }
            catch (ResponseException e) when (e.Code == 409 && e.Duplicated.Any())
            {
                if (backoff > 10) // ~3.5 min total backoff time
                {
                    throw;
                }
                _logger.LogDebug("Found {NumDuplicated} duplicates, during the creation of {NumTimeSeries} time series", e.Duplicated.Count(), missing.Count);
                toGet.UnionWith(missing);
            }
            if (toGet.Any()) {
                await Task.Delay(TimeSpan.FromSeconds(0.1 * Math.Pow(2, backoff)));
                var ensured = await GetOrCreateTimeSeriesChunk(client, toGet, buildTimeSeries, backoff + 1, token);
                created.AddRange(ensured);
            }
            return created;
        }

        private static async Task EnsureTimeSeriesChunk(
            Client client,
            IEnumerable<TimeSeriesCreate> timeSeries,
            CancellationToken token)
        {
            var create = timeSeries;
            while (!token.IsCancellationRequested && create.Any())
            {
                try
                {
                    var newTs = await client.TimeSeries.CreateAsync(create, token);
                    _logger.LogDebug("Created {New} new time series in CDF", newTs.Count());
                    return;
                }
                catch (ResponseException e) when (e.Code == 409 && e.Duplicated.Any())
                {
                    // Remove duplicates - already exists
                    // also a case for legacyName...
                    var duplicated = new HashSet<string>(e.Duplicated
                        .Select(d => d.GetValue("externalId", null))
                        .Where(mv => mv != null)
                        .Select(mv => mv.ToString()));
                    create = timeSeries.Where(ts => !duplicated.Contains(ts.ExternalId));
                    await Task.Delay(TimeSpan.FromMilliseconds(100), token);
                    _logger.LogDebug("Found {NumDuplicated} duplicates, during the creation of {NumTimeSeries} time series", 
                        e.Duplicated.Count(), create.Count());
                    continue;
                }
#pragma warning disable CA1031 // Do not catch general exception types
                catch (Exception e)
                {
                    _logger.LogWarning("CDF create timeseries failed: {Message} - Retrying in 1 second", e.Message);
                }
#pragma warning restore CA1031 // Do not catch general exception types
                await Task.Delay(TimeSpan.FromSeconds(1), token);
            }
        }
    }
}