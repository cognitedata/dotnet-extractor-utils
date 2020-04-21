using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using CogniteSdk;

namespace ExtractorUtils
{
    /// <summary>
    /// Extension utility methods for <see cref="Client"/>
    /// </summary>
    public static class CogniteClientExtensions
    {
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

        public static async Task<IEnumerable<TimeSeries>> EnsureTimeSeries(
            this Client client,
            IEnumerable<string> externalIds,
            Func<IEnumerable<string>, IEnumerable<TimeSeriesCreate>> buildTimeSeries,
            int chunkSize,
            int throttleSize,
            CancellationToken token)
        {
            var result = new List<TimeSeries>();
            var generators = externalIds
                .ChunkBy(chunkSize)
                .Select<IEnumerable<string>, Func<Task>>(
                    chunk => async () => {
                        var existing = await EnsureTimeSeriesChunk(client, chunk, buildTimeSeries, 0, token);
                        result.AddRange(existing);
                    });
            await generators.RunThrottled(throttleSize, token);
            return result;
        }

        private static async Task<IEnumerable<TimeSeries>> EnsureTimeSeriesChunk(
            Client client,
            IEnumerable<string> externalIds,
            Func<IEnumerable<string>, IEnumerable<TimeSeriesCreate>> buildTimeSeries,
            int backoff,
            CancellationToken token)
        {
            var missing = new HashSet<string>();
            try
            {
                return await client.TimeSeries.RetrieveAsync(externalIds.Select(id => new Identity(id)), token);
            }
            catch (ResponseException e) {
                if (e.Code != 400 || !e.Missing.Any())
                {
                    throw;
                }
                foreach (var ts in e.Missing)
                {
                    if (ts.TryGetValue("externalId", out MultiValue value))
                    {
                        missing.Add(value.ToString());
                    }
                }
                
            }

            var toGet = new HashSet<string>(externalIds
                .Where(e => !missing.Contains(e)));
            var created = new List<TimeSeries>();

            try
            {
                var newTs = await client.TimeSeries.CreateAsync(buildTimeSeries(missing), token);
                created.AddRange(newTs);
            }
            catch (ResponseException e) 
            {
                if (e.Code != 409 || !e.Duplicated.Any())
                {
                    throw;
                }
                if (backoff > 10) // ~3.5 min total backoff time
                {
                    throw;
                }
                toGet.UnionWith(missing);
            }
            if (toGet.Any()) {
                await Task.Delay(TimeSpan.FromSeconds(0.1 * Math.Pow(2, backoff)));
                var ensured = await EnsureTimeSeriesChunk(client, toGet, buildTimeSeries, backoff + 1, token);
                created.AddRange(ensured);
            }
            return created;
        }
    }
}