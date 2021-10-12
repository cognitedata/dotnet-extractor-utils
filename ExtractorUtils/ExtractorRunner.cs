using Cognite.Extractor.Configuration;
using Cognite.Extractor.Logging;
using Cognite.Extractor.Metrics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using System;
using System.Linq;
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
        /// Configure and run an extractor with config of type <typeparamref name="TConfig"/>
        /// and extractor of type <typeparamref name="TExtractor"/>
        /// </summary>
        /// <typeparam name="TConfig">Type of configuration</typeparam>
        /// <typeparam name="TExtractor">Type of extractor</typeparam>
        /// <param name="configPath">Path to yml config file. Can be null to not load config, in this case
        /// <paramref name="config" /> must be set, or the config must be added to <paramref name="extServices"/></param>
        /// <param name="acceptedConfigVersions">List of accepted config versions, null accepts all</param>
        /// <param name="appId">AppId to append to requests to CDF</param>
        /// <param name="userAgent">User agent on form Product/Version</param>
        /// <param name="addStateStore">True if the extractor uses a state store</param>
        /// <param name="addLogger">True to add logging</param>
        /// <param name="addMetrics">True to add metrics</param>
        /// <param name="restart">True to restart extractor if it crashes or terminates, using exponential backoff</param>
        /// <param name="token">Optional cancellation token from external cancellation source</param>
        /// <param name="onCreateExtractor">Method called when the extractor is created,
        /// used to retrieve the extractor and destination objects</param>
        /// <param name="configCallback">Called after config has been created, used to manually set config parameters,
        /// for example from command line options. Can also throw a <see cref="ConfigurationException"/> if the contents are
        /// invalid</param>
        /// <param name="extServices">Optional pre-configured service collection</param>
        /// <param name="startupLogger">Optional logger to use before config has been loaded, to report configuration issues</param>
        /// <param name="config">Optional pre-existing config object, can be used instead of config path.</param>
        /// <param name="requireDestination">Default true, whether to fail if a destination cannot be configured</param>
        /// <returns>Task which completes when the extractor has run</returns>
        public static async Task Run<TConfig, TExtractor>(
            string configPath,
            int[] acceptedConfigVersions,
            string appId,
            string userAgent,
            bool addStateStore,
            bool addLogger,
            bool addMetrics,
            bool restart,
            CancellationToken token,
            Action<CogniteDestination, TExtractor> onCreateExtractor = null,
            Action<TConfig> configCallback = null,
            ServiceCollection extServices = null,
            ILogger startupLogger = null,
            TConfig config = null,
            bool requireDestination = true)
            where TConfig : VersionedConfig
            where TExtractor : BaseExtractor
        {
            int waitRepeats = 1;

            using var source = CancellationTokenSource.CreateLinkedTokenSource(token);
            void CancelKeyPressHandler(object sender, ConsoleCancelEventArgs eArgs)
            {
                eArgs.Cancel = true;
                try
                {
                    source?.Cancel();
                }
                catch { }
            }

            Console.CancelKeyPress += CancelKeyPressHandler;
            while (!source.IsCancellationRequested)
            {
                var services = new ServiceCollection();

                if (extServices != null)
                {
                    services.Add(extServices);
                }

                ConfigurationException exception = null;
                try
                {
                    config = services.AddExtractorDependencies(configPath, acceptedConfigVersions,
                        appId, userAgent, addStateStore, addLogger, addMetrics, requireDestination, config);
                    configCallback?.Invoke(config);
                }
                catch (AggregateException ex)
                {
                    exception = ex.Flatten().InnerExceptions.OfType<ConfigurationException>().First();
                }
                catch (ConfigurationException ex)
                {
                    exception = ex;
                }

                if (exception != null)
                {
                    if (startupLogger != null)
                    {
                        startupLogger.LogError("Invalid configuration file: {msg}", exception.Message);
                        if (!restart) startupLogger.LogInformation("Sleeping for 30 seconds");
                    }
                    else
                    {
                        Serilog.Log.Logger = LoggingUtils.GetSerilogDefault();
                        Serilog.Log.Error("Invalid configuration file: " + exception.Message);
                        if (!restart) Serilog.Log.Information("Sleeping for 30 seconds");
                    }
                    if (!restart) break;
                    try
                    {
                        await Task.Delay(30_000, source.Token).ConfigureAwait(false);
                    }
                    catch { }
                    continue;
                }

                services.AddSingleton<TExtractor>();
                services.AddSingleton<BaseExtractor>(prov => prov.GetRequiredService<TExtractor>());
                DateTime startTime = DateTime.UtcNow;
                ILogger<BaseExtractor> log;

                var provider = services.BuildServiceProvider();
                await using (provider.ConfigureAwait(false))
                {
                    log = new NullLogger<BaseExtractor>();
                    TExtractor extractor = null;
                    try
                    {
                        if (addMetrics)
                        {
                            var metrics = provider.GetRequiredService<MetricsService>();
                            metrics.Start();
                        }
                        if (addLogger)
                        {
                            log = provider.GetRequiredService<ILogger<BaseExtractor>>();
                            Serilog.Log.Logger = provider.GetRequiredService<Serilog.ILogger>();
                        }
                        extractor = provider.GetRequiredService<TExtractor>();
                        if (onCreateExtractor != null)
                        {
                            var destination = provider.GetRequiredService<CogniteDestination>();
                            onCreateExtractor(destination, extractor);
                        }
                    }
                    catch (Exception ex)
                    {
                        log.LogError("Failed to build extractor: {msg}", ex.Message);
                    }

                    if (extractor != null)
                    {
                        try
                        {
                            await extractor.Start(source.Token).ConfigureAwait(false);
                        }
                        catch (TaskCanceledException) when (source.IsCancellationRequested)
                        {
                            log.LogWarning("Extractor stopped manually");
                        }
                        catch (Exception ex)
                        {
                            // Make the stack trace a little cleaner. We generally don't need the whole task stack.
                            if (ex is AggregateException aex) ex = aex.Flatten().InnerExceptions.First();

                            if (source.IsCancellationRequested)
                            {
                                log.LogWarning("Extractor stopped manually");
                            }
                            else
                            {
                                log.LogError(ex, "Extractor crashed unexpectedly");
                            }
                        }
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
            Console.CancelKeyPress -= CancelKeyPressHandler;
        }


    }
}
