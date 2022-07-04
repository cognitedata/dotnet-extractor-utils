using Cognite.Extractor.Common;
using Cognite.Extractor.Configuration;
using Cognite.Extractor.Logging;
using Cognite.Extractor.Metrics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.Extractor.Utils
{
    /// <summary>
    /// Parameters for launching an extractor. Can be modified
    /// after config is loaded by defining a ConfigCallback.
    /// </summary>
    /// <typeparam name="TConfig">Type of config object</typeparam>
    /// <typeparam name="TExtractor">Type of extractor object</typeparam>
    public class ExtractorRunnerParams<TConfig, TExtractor>
        where TConfig : VersionedConfig
        where TExtractor : BaseExtractor<TConfig>
    {
        /// <summary>
        /// The extractor index
        /// </summary>
        public int Index { get; set; }
        /// <summary>
        /// Path to config file
        /// </summary>
        public string? ConfigPath { get; set; }
        /// <summary>
        /// List of accepted config versions.
        /// Can be set to null to ignore.
        /// </summary>
        public int[]? AcceptedConfigVersions { get; set; }
        /// <summary>
        /// AppId to use if CDF destination is defined
        /// </summary>
        public string? AppId { get; set; }
        /// <summary>
        /// User agent to use if CDF destination is defined
        /// </summary>
        public string? UserAgent { get; set; }
        /// <summary>
        /// True if the extractor uses a state store
        /// </summary>
        public bool AddStateStore { get; set; }
        /// <summary>
        /// True to add logging
        /// </summary>
        public bool AddLogger { get; set; } = true;
        /// <summary>
        /// True to add metrics
        /// </summary>
        public bool AddMetrics { get; set; }
        /// <summary>
        /// True to restart if the extractor fails.
        /// </summary>
        public bool Restart { get; set; }
        /// <summary>
        /// Called when the extractor has been built.
        /// </summary>
        public Action<CogniteDestination?, TExtractor>? OnCreateExtractor { get; set; }
        /// <summary>
        /// Called after config has been read. Can be used to modify the runner params and config object based on
        /// external parameters. New services can also be registered here based on the configuration object.
        /// </summary>
        public Action<TConfig, ExtractorRunnerParams<TConfig, TExtractor>, ServiceCollection>? ConfigCallback { get; set; }
        /// <summary>
        /// Predefined list of services.
        /// </summary>
        public ServiceCollection? ExtServices { get; set; }
        /// <summary>
        /// Logger to use before config has been loaded.
        /// </summary>
        public ILogger? StartupLogger { get; set; }
        /// <summary>
        /// Predefined config object, used instead of defining a config path.
        /// </summary>
        public TConfig? Config { get; set; }
        /// <summary>
        /// True to require a CogniteDestination to be set.
        /// </summary>
        public bool RequireDestination { get; set; }
        /// <summary>
        /// Method to log exceptions. Default is just a simple log message with the exception.
        /// </summary>
        public Action<ILogger, Exception, string>? LogException { get; set; }
        /// <summary>
        /// Method to build logger from config. Defaults to <see cref="LoggingUtils.GetConfiguredLogger(LoggerConfig)"/>
        /// </summary>
        public Func<LoggerConfig, Serilog.ILogger>? BuildLogger { get; set; }
        /// <summary>
        /// Wait for config to be loaded, even if Restart is set to false.
        /// </summary>
        public bool WaitForConfig { get; set; } = true;
        /// <summary>
        /// List of configuration types that should be registered if they are present on <typeparamref name="TConfig"/>.
        /// </summary>
        public IEnumerable<Type>? ConfigTypes { get; set; }
        /// <summary>
        /// Let this method return true if the exception is fatal and the extractor should terminate.
        /// </summary>
        public Func<Exception, bool>? IsFatalException { get; set; }

    }


    /// <summary>
    /// Contains utilities for running an extractor based on BaseExtractor
    /// </summary>
    public static class ExtractorRunner
    {
        private static void LogException(ILogger log, Exception ex, string message)
        {
            log.LogError(ex, "{msg}: {exMsg}", message, ex.Message);
        }


        /// <summary>
        /// Configure and run an extractor with config of type <typeparamref name="TConfig"/>
        /// and extractor of type <typeparamref name="TExtractor"/>
        /// </summary>
        /// <typeparam name="TConfig">Type of configuration</typeparam>
        /// <typeparam name="TExtractor">Type of extractor</typeparam>
        /// <param name="index">The extractor index</param>
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
        /// <param name="logException">Method called to log exceptions. Useful if special handling is desired.</param>
        /// <returns>Task which completes when the extractor has run</returns>
        public static async Task Run<TConfig, TExtractor>(
            int index,
            string configPath,
            int[] acceptedConfigVersions,
            string appId,
            string userAgent,
            bool addStateStore,
            bool addLogger,
            bool addMetrics,
            bool restart,
            CancellationToken token,
            Action<CogniteDestination?, TExtractor>? onCreateExtractor = null,
            Action<TConfig, ExtractorRunnerParams<TConfig, TExtractor>, ServiceCollection>? configCallback = null,
            ServiceCollection? extServices = null,
            ILogger? startupLogger = null,
            TConfig? config = null,
            bool requireDestination = true,
            Action<ILogger, Exception, string>? logException = null)
            where TConfig : VersionedConfig
            where TExtractor : BaseExtractor<TConfig>
        {
            await Run(new ExtractorRunnerParams<TConfig, TExtractor>
            {
                Index = index,
                ConfigPath = configPath,
                AcceptedConfigVersions = acceptedConfigVersions,
                AppId = appId,
                UserAgent = userAgent,
                AddStateStore = addStateStore,
                AddLogger = addLogger,
                AddMetrics = addMetrics,
                Restart = restart,
                OnCreateExtractor = onCreateExtractor,
                ConfigCallback = configCallback,
                ExtServices = extServices,
                StartupLogger = startupLogger,
                Config = config,
                RequireDestination = requireDestination,
                LogException = logException,
                }, token).ConfigureAwait(false);
        }

        /// <summary>
        /// Configure and run an extractor with config of type <typeparamref name="TConfig"/>
        /// and extractor of type <typeparamref name="TExtractor"/>
        /// </summary>
        /// <typeparam name="TConfig">Type of configuration</typeparam>
        /// <typeparam name="TExtractor">Type of extractor</typeparam>
        /// <param name="options">Parameter object</param>
        /// <param name="token">Cancellation token</param>
        /// <exception cref="ArgumentNullException">If options is not set</exception>
        public static async Task Run<TConfig, TExtractor>(
            ExtractorRunnerParams<TConfig, TExtractor> options,
            CancellationToken token)
            where TConfig : VersionedConfig
            where TExtractor : BaseExtractor<TConfig>
        {
            if (options == null) throw new ArgumentNullException(nameof(options));
            if (options.LogException == null) options.LogException = LogException;

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

                if (options.ExtServices != null)
                {
                    services.Add(options.ExtServices);
                }

                ConfigurationException? exception = null;
                try
                {
                    options.Config = services.AddExtractorDependencies(
                        options.ConfigPath,
                        options.AcceptedConfigVersions,
                        options.AppId,
                        options.UserAgent,
                        options.AddStateStore,
                        options.AddLogger,
                        options.AddMetrics, 
                        options.RequireDestination,
                        options.Config,
                        options.BuildLogger,
                        options.ConfigTypes);
                    options.ConfigCallback?.Invoke(options.Config, options, services);
                }
                catch (TargetInvocationException ex)
                {
                    if (ex.InnerException is ConfigurationException cex) exception = cex;
                    else if (ex.InnerException is AggregateException aex)
                    {
                        exception = aex.Flatten().InnerExceptions.OfType<ConfigurationException>().FirstOrDefault();
                    }
                    if (exception == null)
                    {
                        exception = new ConfigurationException("Failed to load config file: ", ex);
                    }
                }
                catch (AggregateException ex)
                {
                    exception = ex.Flatten().InnerExceptions.OfType<ConfigurationException>().FirstOrDefault();
                    if (exception == null)
                    {
                        exception = new ConfigurationException("Failed to load config file: ", ex);
                    }
                }
                catch (ConfigurationException ex)
                {
                    exception = ex;
                }
                catch (Exception ex)
                {
                    exception = new ConfigurationException("Failed to load config file: ", ex);
                }

                if (exception != null)
                {
                    if (options.StartupLogger != null)
                    {
                        options.StartupLogger.LogError("Invalid configuration file: {msg}", exception.Message);
                        if (options.WaitForConfig || options.Restart) options.StartupLogger.LogInformation("Sleeping for 30 seconds");
                    }
                    else
                    {
                        Serilog.Log.Logger = LoggingUtils.GetSerilogDefault();
                        Serilog.Log.Error("Invalid configuration file: " + exception.Message);
                        if (options.WaitForConfig || options.Restart) Serilog.Log.Information("Sleeping for 30 seconds");
                    }
                    if (!options.WaitForConfig && !options.Restart) break;
                    try
                    {
                        await Task.Delay(30_000, source.Token).ConfigureAwait(false);
                    }
                    catch { }
                    continue;
                }

                services.AddSingleton<TExtractor>();
                services.AddSingleton<BaseExtractor<TConfig>>(prov => prov.GetRequiredService<TExtractor>());
                DateTime startTime = DateTime.UtcNow;
                ILogger<BaseExtractor<TConfig>> log;

                var provider = services.BuildServiceProvider();
                await using (provider.ConfigureAwait(false))
                {
                    log = new NullLogger<BaseExtractor<TConfig>>();
                    TExtractor? extractor = null;
                    try
                    {
                        if (options.AddMetrics)
                        {
                            var metrics = provider.GetRequiredService<MetricsService>();
                            metrics.Start();
                        }
                        if (options.AddLogger)
                        {
                            log = provider.GetRequiredService<ILogger<BaseExtractor<TConfig>>>();
                            Serilog.Log.Logger = provider.GetRequiredService<Serilog.ILogger>();
                        }
                        extractor = provider.GetRequiredService<TExtractor>();
                        
                        if (options.OnCreateExtractor != null)
                        {
                            var destination = provider.GetService<CogniteDestination>();
                            options.OnCreateExtractor(destination, extractor);
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
                            await extractor.Start(options.Index, source.Token).ConfigureAwait(false);
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
                                options.LogException(log, ex, "Extractor crashed unexpectedly");
                            }

                            if (options.IsFatalException?.Invoke(ex) ?? false)
                            {
                                log.LogInformation("Fatal exception encountered, quitting extractor");
                                break;
                            }
                        }
                    }


                    if (source.IsCancellationRequested || !options.Restart)
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
                        await Task.Delay(sleepTime, source.Token).ConfigureAwait(false);
                    }
                    catch (Exception)
                    {
                        log.LogWarning("Extractor stopped manually");
                        break;
                    }
                }


            }
            _ = Task.Run(() => Console.CancelKeyPress -= CancelKeyPressHandler, CancellationToken.None).ConfigureAwait(false);
        }


    }
}
