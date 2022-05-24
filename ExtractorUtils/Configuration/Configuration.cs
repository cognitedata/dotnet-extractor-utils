using Microsoft.Extensions.DependencyInjection;
using Cognite.Extractor.Configuration;
using Cognite.Extractor.Logging;
using Cognite.Extractor.Metrics;
using Cognite.Extractor.StateStorage;
using Microsoft.Extensions.Logging;
using System;
using Cognite.Extractor.Common;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.IO;
using System.Threading;

namespace Cognite.Extractor.Utils
{

    /// <summary>
    /// Extension utilities for configuration.
    /// </summary>
    public static class ConfigurationExtensions
    {

        /// <summary>
        /// Read the config of type <typeparamref name="T"/> from the yaml file in <paramref name="path"/>
        /// and adds it as a singleton to the service collection <paramref name="services"/>
        /// Also adds <see cref="CogniteConfig"/>, <see cref="LoggerConfig"/> and <see cref="MetricsConfig"/> configuration
        /// objects as singletons, if they are present in the configuration
        /// </summary>
        /// <param name="services">The service collection</param>
        /// <param name="path">Path to the file</param>
        /// <param name="acceptedConfigVersions">Accepted versions</param>
        /// <typeparam name="T">A type that inherits from <see cref="VersionedConfig"/></typeparam>
        /// <exception cref="ConfigurationException">Thrown when the version is not valid, 
        /// the yaml file is not found or in case of yaml parsing error</exception>
        /// <returns>An instance of the configuration object</returns>
        public static T AddConfig<T>(this IServiceCollection services,
                                        string path,
                                        params int[]? acceptedConfigVersions) where T : VersionedConfig
        {
            var config = ConfigurationUtils.TryReadConfigFromFile<T>(path, acceptedConfigVersions);
            services.AddSingleton<T>(config);
            services.AddConfig<T>(config,
                typeof(CogniteConfig),
                typeof(LoggerConfig),
                typeof(MetricsConfig),
                typeof(StateStoreConfig),
                typeof(BaseConfig));
            return config;
        }

        /// <summary>
        /// Read the config of type <typeparamref name="T"/> from the yaml file in <paramref name="path"/>
        /// and adds it as a singleton to the service collection <paramref name="services"/>
        /// Also adds <see cref="CogniteConfig"/>, <see cref="LoggerConfig"/> and <see cref="MetricsConfig"/> configuration
        /// objects as singletons, if they are present in the configuration
        /// </summary>
        /// <param name="services">The service collection</param>
        /// <param name="path">Path to the file</param>
        /// <param name="acceptedConfigVersions">Accepted versions</param>
        /// <param name="types">Types to look for as properties on <typeparamref name="T"/></param>
        /// <typeparam name="T">A type that inherits from <see cref="VersionedConfig"/></typeparam>
        /// <exception cref="ConfigurationException">Thrown when the version is not valid, 
        /// the yaml file is not found or in case of yaml parsing error</exception>
        /// <returns>An instance of the configuration object</returns>
        public static T AddConfig<T>(this IServiceCollection services,
                                        string path,
                                        Type[] types,
                                        params int[]? acceptedConfigVersions) where T : VersionedConfig
        {
            var config = ConfigurationUtils.TryReadConfigFromFile<T>(path, acceptedConfigVersions);
            services.AddSingleton<T>(config);
            services.AddConfig<T>(config, types);
            return config;
        }

        /// <summary>
        /// Read the config of type <typeparamref name="T"/> from the yaml file in <paramref name="path"/>
        /// and adds it as a singleton to the service collection <paramref name="services"/>
        /// Also adds <see cref="CogniteConfig"/>, <see cref="LoggerConfig"/> and <see cref="MetricsConfig"/> configuration
        /// objects as singletons, if they are present in the configuration.
        /// The config file will either be read locally or fetched from CDF depending on the "type" field.
        /// </summary>
        /// <param name="services">The service collection</param>
        /// <param name="path">Path to the file</param>
        /// <param name="acceptedConfigVersions">Accepted versions</param>
        /// <param name="types">Types to look for as properties on <typeparamref name="T"/></param>
        /// <param name="setHttpClient">Set http client for the sdk, or assume one is provided.</param>
        /// <param name="setLogger">Set logger for the sdk.</param>
        /// <param name="setMetrics">Set metrics for the sdk.</param>
        /// <param name="remoteConfig">Pre-existing config object, set to null to read from <paramref name="path"/> instead. Path is still required if <paramref name="bufferConfigFile"/> is set.</param>
        /// <param name="bufferConfigFile">True to store the configuration file locally so that the extractor may start even without a connection to CDF.
        /// Stored in the same directory as the config file, with name equal to _temp_[name-of-config-file]</param>
        /// <param name="token">Optional cancellation token.</param>
        /// <typeparam name="T">A type that inherits from <see cref="VersionedConfig"/></typeparam>
        /// <exception cref="ConfigurationException">Thrown when the version is not valid, 
        /// the yaml file is not found or in case of yaml parsing error</exception>
        /// <returns>An instance of the configuration object</returns>
        public static async Task<T> AddRemoteConfig<T>(this IServiceCollection services,
                                        string path,
                                        Type[] types,
                                        bool setHttpClient,
                                        bool setLogger,
                                        bool setMetrics,
                                        bool bufferConfigFile,
                                        RemoteConfig? remoteConfig,
                                        CancellationToken token,
                                        params int[]? acceptedConfigVersions) where T : VersionedConfig
        {
            if (remoteConfig == null)
            {
                ConfigurationUtils.IgnoreUnmatchedProperties();
                remoteConfig = ConfigurationUtils.TryReadConfigFromFile<RemoteConfig>(path, null);
                ConfigurationUtils.DisallowUnmatchedProperties();

                if (remoteConfig.Type == ConfigurationMode.Local)
                {
                    return services.AddConfig<T>(path, types, acceptedConfigVersions);
                }

                // Try read config again, to get checking of unmatched properties.
                remoteConfig = ConfigurationUtils.TryReadConfigFromFile<RemoteConfig>(path, null);
            }
            else if (remoteConfig.Type == ConfigurationMode.Local)
            {
                return services.AddConfig<T>(path, types, acceptedConfigVersions);
            }

            if (remoteConfig.CogniteConfig.ExtractionPipeline?.PipelineId == null) throw new ConfigurationException("Extraction pipeline id required for remote config");

            services.AddCogniteClient("dotnet-utils-remote-config", null, setHttpClient, setLogger, setMetrics, true);
            services.AddSingleton(remoteConfig.CogniteConfig);
            services.AddSingleton(provider => new RemoteConfigManager<T>(
                provider.GetRequiredService<CogniteDestination>(),
                provider.GetService<ILogger<RemoteConfigManager<T>>>(),
                path,
                bufferConfigFile,
                acceptedConfigVersions,
                remoteConfig.CogniteConfig.ExtractionPipeline.PipelineId));
            var manager = services.BuildServiceProvider().GetRequiredService<RemoteConfigManager<T>>();

            var config = await manager.FetchLatestThrowOnFailure(token).ConfigureAwait(false);

            services.AddSingleton(config);
            services.AddConfig(config, types);

            return config;
        }

        /// <summary>
        /// Configure dependencies for the BaseExtractor, adding metrics, logging, state store and cognite client.
        /// Short for AddConfig, AddCogniteClient, AddStateStore, AddMetrics and AddLogger.
        /// </summary>
        /// <typeparam name="T">Type of config object</typeparam>
        /// <param name="services">Servicecollection to add to</param>
        /// <param name="configPath">Path to config file</param>
        /// <param name="acceptedConfigVersions">Valid config versions. Can be null to allow all.</param>
        /// <param name="appId">AppId added to requests to CDF</param>
        /// <param name="userAgent">User agent on form Product/Version</param>
        /// <param name="addStateStore">True to add state store, used if extractor reads history</param>
        /// <param name="addLogger">True to add logger</param>
        /// <param name="addMetrics">True to add metrics</param>
        /// <param name="requireDestination">True to fail if a destination cannot be configured</param>
        /// <param name="config">Optional pre-defined config object to use instead of reading from file</param>
        /// <param name="buildLogger">Optional method to build logger.
        /// <param name="types">Types to look for as properties on <typeparamref name="T"/></param>
        /// Defaults to <see cref="LoggingUtils.GetConfiguredLogger(LoggerConfig)"/> </param>
        /// <exception cref="ConfigurationException">Thrown when the version is not valid, 
        /// the yaml file is not found or in case of yaml parsing error</exception>
        /// <returns>Configuration object</returns>
        public static T AddExtractorDependencies<T>(
            this IServiceCollection services,
            string? configPath,
            int[]? acceptedConfigVersions,
            string? appId,
            string? userAgent,
            bool addStateStore,
            bool addLogger = true,
            bool addMetrics = true,
            bool requireDestination = true,
            T? config = null,
            Func<LoggerConfig, Serilog.ILogger>? buildLogger = null,
            IEnumerable<Type>? types = null) where T : VersionedConfig
        {
            var configTypes = new[]
                {
                    typeof(CogniteConfig),
                    typeof(LoggerConfig),
                    typeof(MetricsConfig),
                    typeof(StateStoreConfig),
                    typeof(BaseConfig)
                };

            if (types != null) configTypes = configTypes.Concat(types).Distinct().ToArray();

            if (config != null)
            {
                services.AddSingleton(config);
                services.AddConfig(config, configTypes);
            }
            else if (configPath != null)
            {
                config = services.AddConfig<T>(configPath, configTypes, acceptedConfigVersions);
            }
            else
            {
                throw new ConfigurationException("No configuration path specified");
            }
            services.AddCogniteClient(appId, userAgent, addLogger, addMetrics, true, requireDestination);
            if (addStateStore) services.AddStateStore();
            if (addLogger) services.AddLogger(buildLogger);
            if (addMetrics) services.AddMetrics();
            services.AddExtractionRun(addLogger);
            return config;
        }

        /// <summary>
        /// Add an <see cref="ExtractionRun"/> object as singleton dependency.
        /// </summary>
        /// <param name="services">Service collection to add to</param>
        /// <param name="setLogger">True to set a logger</param>
        public static void AddExtractionRun(this IServiceCollection services, bool setLogger)
        {
            services.AddSingleton(provider =>
            {
                var logger = setLogger ?
                    provider.GetRequiredService<ILogger<ExtractionRun>>() : null;
                var destination = provider.GetService<CogniteDestination>();
                var config = provider.GetService<CogniteConfig>();
                if (config == null || destination == null) return null!;
                if (config?.ExtractionPipeline == null || config.ExtractionPipeline.PipelineId == null) return null!;
                return new ExtractionRun(config.ExtractionPipeline, destination, logger);
            });
        }
    }
}

