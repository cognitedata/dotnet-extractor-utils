using Cognite.Extractor.Configuration;
using Cognite.Extractor.Metrics;
using Cognite.Extractor.Utils;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.Extractor.Utils
{
    /// <summary>
    /// Contains utilities for running an extractor based on BaseExtractor
    /// </summary>
    public static class ExtractorRunner
    {
        /// <summary>
        /// Configure and run an extractor with config of type <typeparamref name="T"/>
        /// </summary>
        /// <typeparam name="T">Type of configuration</typeparam>
        /// <param name="configPath">Path to yml config file</param>
        /// <param name="acceptedConfigVersions">List of accepted config versions, null accepts all</param>
        /// <param name="appId">AppId to append to requests to CDF</param>
        /// <param name="userAgent">User agent on form Product/Version</param>
        /// <param name="addStateStore">True if the extractor uses a state store</param>
        /// <param name="addLogger">True to add logging</param>
        /// <param name="addMetrics">True to add metrics</param>
        /// <param name="restart">True to restart extractor if it crashes or terminates, using exponential backoff</param>
        /// <param name="builder">Method constructing a new extractor using the service provider</param>
        /// <param name="token">Optional cancellation token from external cancellation source</param>
        /// <param name="services">Optional pre-configured service collection</param>
        /// <returns>Task which completes when the extractor has run</returns>
        public static async Task Run<T>(
            string configPath,
            int[] acceptedConfigVersions,
            string appId,
            string userAgent,
            bool addStateStore,
            bool addLogger,
            bool addMetrics,
            bool restart,
            Func<IServiceProvider, CancellationToken, BaseExtractor> builder,
            CancellationToken token,
            ServiceCollection services = null)
            where T : VersionedConfig
        {
            if (builder == null) throw new ArgumentNullException(nameof(builder));

            int waitRepeats = 1;

            if (services == null) services = new ServiceCollection();
            using (var source = CancellationTokenSource.CreateLinkedTokenSource(token))
            {
                Console.CancelKeyPress += (sender, eArgs) =>
                {
                    eArgs.Cancel = true;
                    source?.Cancel();
                };
                while (!source.IsCancellationRequested)
                {
                    services.AddExtractorDependencies<T>(configPath, acceptedConfigVersions,
                        appId, userAgent, addStateStore, addLogger, addMetrics);
                    var provider = services.BuildServiceProvider();
                    if (addMetrics)
                    {
                        var metrics = provider.GetRequiredService<MetricsService>();
                    }
                    ILogger<BaseExtractor> log = new NullLogger<BaseExtractor>();
                    if (addLogger)
                    {
                        log = provider.GetRequiredService<ILogger<BaseExtractor>>();
                    }
                    var extractor = builder(provider, source.Token);
                    DateTime startTime = DateTime.UtcNow;
                    try
                    {
                        await extractor.Start().ConfigureAwait(false);
                    }
                    catch (TaskCanceledException) when (source.IsCancellationRequested)
                    {
                        log.LogWarning("Extractor stopped manually");
                    }
                    catch (Exception ex)
                    {
                        log.LogError(ex, "Extractor crashed unexpectedly");
                    }


                    if (source.IsCancellationRequested || !restart)
                    {
                        log.LogInformation("Quitting extractor");
                        break;
                    }

                    if (startTime > DateTime.UtcNow - TimeSpan.FromSeconds(600))
                    {
                        waitRepeats++;
                    }
                    else
                    {
                        waitRepeats = 1;
                    }

                    try
                    {
                        var sleepTime = TimeSpan.FromSeconds(Math.Pow(2, Math.Min(waitRepeats, 9)));
                        log.LogInformation("Sleeping for {time}", sleepTime);
                        Task.Delay(sleepTime, source.Token).Wait();
                    }
                    catch (Exception)
                    {
                        log.LogWarning("Extractor stopped manually");
                        break;
                    }
                }
            }         
        }


    }
}
