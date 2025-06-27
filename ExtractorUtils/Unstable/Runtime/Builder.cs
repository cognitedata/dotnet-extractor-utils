using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extractor.Common;
using Cognite.Extractor.Configuration;
using Cognite.Extractor.Logging;
using Cognite.Extractor.Utils.Unstable.Configuration;
using CogniteSdk;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Cognite.Extractor.Utils.Unstable.Runtime
{
    /// <summary>
    /// Configured source of the config file.
    /// </summary>
    public enum ConfigSourceType
    {
        /// <summary>
        /// Load a local config file.
        /// </summary>
        Local,
        /// <summary>
        /// Load the config file from CDF (default).
        /// </summary>
        Remote
    }

    /// <summary>
    /// Policy for when to restart the extractor.
    /// </summary>
    public enum ExtractorRestartPolicy
    {
        /// <summary>
        /// Never restart.
        /// </summary>
        Never,
        /// <summary>
        /// Restart if the extractor fails.
        /// </summary>
        OnError,
        /// <summary>
        /// Always restart the extractor when it completes.
        /// </summary>
        Always,
    }


    /// <summary>
    /// Builder for the extractor runtime.
    /// </summary>
    /// <typeparam name="TConfig">Type of configuration file.</typeparam>
    /// <typeparam name="TExtractor">Type of extractor.</typeparam>
    public class ExtractorRuntimeBuilder<TConfig, TExtractor>
        where TConfig : VersionedConfig
        where TExtractor : BaseExtractor<TConfig>
    {
        /// <summary>
        /// Default folder to look for config files, both `config.yaml` and `connection.yaml`.
        /// </summary>
        public string ConfigFolder { get; set; } = "config";
        /// <summary>
        /// List of accepted config versions. Null to ignore the version field.
        /// </summary>
        public int[]? AcceptedConfigVersions { get; set; }
        /// <summary>
        /// AppId to use if CDF destination is defined
        /// </summary>
        public string AppId { get; }
        /// <summary>
        /// User agent to use if CDF destination is defined
        /// </summary>
        public string UserAgent { get; }

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
        /// Whether the runtime should set up the HTTP client, or if
        /// the caller should provide an external one. This is useful
        /// for testing, where the caller might want to provide a mock
        /// HTTP client.
        /// </summary>
        public bool SetupHttpClient { get; set; } = true;

        /// <summary>
        /// Full path to a configuration file, takes precedence over the <see cref="ConfigFolder"/> setting.
        /// </summary>
        public string? OverrideConfigFile { get; set; }
        /// <summary>
        /// Full path to a connection file, takes precedence over the <see cref="ConfigFolder"/> setting.
        /// </summary>
        public string? OverrideConnectionConfig { get; set; }

        /// <summary>
        /// Called when the extractor has been built.
        /// </summary>
        public Action<CogniteDestination?, TExtractor>? OnCreateExtractor { get; set; }
        /// <summary>
        /// Called after config has been read. Can be used to modify the runner params and config object based on
        /// external parameters. New services can also be registered here based on the configuration object.
        /// </summary>
        public Action<TConfig, ExtractorRuntimeBuilder<TConfig, TExtractor>, ServiceCollection>? OnConfigure { get; set; }
        /// <summary>
        /// Predefined list of services, added to the list of services defined by the runtime.
        /// </summary>
        public IServiceCollection? ExternalServices { get; set; }
        /// <summary>
        /// Logger to use before config has been loaded.
        /// </summary>
        public ILogger StartupLogger { get; set; } = LoggingUtils.GetDefault();
        /// <summary>
        /// Method to log exceptions. Default is just a simple log message with the exception.
        /// </summary>
        public Action<ILogger, Exception, string> LogException { get; set; } = LogExceptionDefault;
        /// <summary>
        /// Method to build logger from config. Defaults to <see cref="LoggingUtils.GetConfiguredLogger(LoggerConfig)"/>
        /// </summary>
        public Func<LoggerConfig, Serilog.ILogger>? BuildLogger { get; set; }
        /// <summary>
        /// Policy describing when the extractor should be restarted.
        /// </summary>
        public ExtractorRestartPolicy RestartPolicy { get; set; } = ExtractorRestartPolicy.Always;

        /// <summary>
        /// Whether to retry sending the startup request. Only applicable if connection config is used.
        /// </summary>
        public bool RetryStartupRequest { get; set; }
        /// <summary>
        /// List of configuration types that should be registered if they are present on <typeparamref name="TConfig"/>.
        /// </summary>
        public IEnumerable<Type>? ConfigTypes { get; set; }
        /// <summary>
        /// True to buffer config if it is fetched from remote. Requires a config path to be set.
        /// Defaults to true.
        /// </summary>
        public bool BufferRemoteConfig { get; set; } = true;

        /// <summary>
        /// Base backoff in milliseconds for restarting the extractor
        /// after it failed to start. On a normal restart, for example
        /// due to a config change, the backoff is not used unless
        /// two restarts come in very quick succession.
        /// 
        /// The true backoff is calculated as
        /// 
        /// Min(MaxBackoff, BackoffBase * 2^n) where n is the number of
        /// times the extractor has been restarted in a row.
        /// </summary>
        public int BackoffBase { get; set; } = 5000;

        /// <summary>
        /// Maximum backoff in milliseconds for restarting the extractor.
        /// See `BackoffBase` for how the backoff is calculated.
        /// </summary>
        public int MaxBackoff { get; set; } = 60000;


        /// <summary>
        /// Static config file instead of reading from file as part of startup.
        /// </summary>
        public TConfig? ExternalConfig { get; set; }
        /// <summary>
        /// Static connection config instead of reading from file as part of startup.
        /// </summary>
        public ConnectionConfig? ExternalConnectionConfig { get; set; }

        /// <summary>
        /// True to not load any connection config, and not create any CDF destination.
        /// </summary>
        public bool NoConnection { get; set; }

        /// <summary>
        /// Configured source of the config file. Defaults to <see cref="ConfigSourceType.Remote"/>.
        /// Typically set from the command line, or to a constant.
        /// </summary>
        public ConfigSourceType ConfigSource { get; set; } = ConfigSourceType.Remote;


        /// <summary>
        /// Resolved path to connection config file.
        /// </summary>
        public string ConnectionConfigPath => OverrideConnectionConfig ?? Path.Combine(ConfigFolder, "connection.yml");
        /// <summary>
        /// Resolved path to config file.
        /// </summary>
        public string ConfigPath => OverrideConfigFile ?? Path.Combine(ConfigFolder, "config.yml");

        private static void LogExceptionDefault(ILogger log, Exception ex, string message)
        {
            log.LogError(ex, "{msg}: {exMsg}", message, ex.Message);
        }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="appId">AppId to use if CDF destination is defined.</param>
        /// <param name="userAgent">User agent to use if CDF destination is defined.</param>
        public ExtractorRuntimeBuilder(string appId, string userAgent)
        {
            AppId = appId;
            UserAgent = userAgent;
        }

        private ConfigSource<TConfig> GetConfigSource(ConnectionConfig? connectionConfig)
        {
            if (ExternalConfig != null)
            {
                return new StaticConfigSource<TConfig>(ExternalConfig);
            }

            // Create a local service collection containing any predefined services, just for this stage.


            if (ConfigSource == ConfigSourceType.Local)
            {
                return new LocalConfigSource<TConfig>(StartupLogger, ConfigPath);
            }
            else
            {
                var services = new ServiceCollection();
                if (ExternalServices != null)
                {
                    services.Add(ExternalServices);
                }
                if (connectionConfig == null)
                {
                    throw new ConfigurationException("Cannot use remote config without a connection config.");
                }

                if (connectionConfig.Integration?.ExternalId == null)
                {
                    throw new ConfigurationException("Cannot use remote config without an integration.");
                }

                services.AddConfig(connectionConfig, typeof(ConnectionConfig));
                services.AddCogniteClient(AppId, UserAgent, AddLogger, AddMetrics, SetupHttpClient);

                var provider = services.BuildServiceProvider();


                var configSource = new RemoteConfigSource<TConfig>(provider.GetRequiredService<Client>(), StartupLogger, connectionConfig.Integration.ExternalId, ConfigPath, BufferRemoteConfig);
                return configSource;
            }
        }

        private async Task<(ConfigSource<TConfig>, ConnectionConfig?)> InitConfigSource(CancellationToken token)
        {
            // Attempt to load the config source in a loop. This can fail,
            // in which case we will retry until the token is cancelled.
            // The reason why we do this and not just cancel the extractor is to
            // simplify setup when restarting the extractor manually is a difficult
            // process, but replacing the config file is easy.
            while (true)
            {
                token.ThrowIfCancellationRequested();
                // Create a new service collection for each iteration,
                // to avoid running into conflicts between iterations.
                var services = new ServiceCollection();
                if (ExternalServices != null)
                {
                    services.Add(ExternalServices);
                }

                try
                {
                    ConnectionConfig? connectionConfig = null;
                    if (!NoConnection)
                    {
                        connectionConfig = ConfigurationUtils.TryReadConfigFromFile<ConnectionConfig>(ConnectionConfigPath);
                    }

                    var configSource = GetConfigSource(connectionConfig);
                    return (configSource, connectionConfig);
                }
                catch (Exception ex)
                {
                    LogException(StartupLogger, ex, "Failed to setup config source, retrying after 20 seconds.");
                    token.ThrowIfCancellationRequested();
                    await Task.Delay(TimeSpan.FromSeconds(20), token).ConfigureAwait(false);
                    continue;
                }
            }
        }

        /// <summary>
        /// Create a new runtime instance by reading the connection config and
        /// creating a new config source. It also sets up a console cancel handler.
        /// </summary>
        /// <param name="token"></param>
        /// <returns></returns>
        public async Task<ExtractorRuntime<TConfig, TExtractor>> MakeRuntime(CancellationToken token)
        {
            var source = CancellationTokenSource.CreateLinkedTokenSource(token);
            void CancelKeyPressHandler(object? sender, ConsoleCancelEventArgs eArgs)
            {
                eArgs.Cancel = true;
                try
                {
                    source?.Cancel();
                }
                catch { }
            }

            Console.CancelKeyPress += CancelKeyPressHandler;
            var (configSource, connectionConfig) = await InitConfigSource(token).ConfigureAwait(false);

            return new ExtractorRuntime<TConfig, TExtractor>(this, configSource, connectionConfig, source);
        }
    }
}