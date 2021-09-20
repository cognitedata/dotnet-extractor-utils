using Microsoft.Extensions.DependencyInjection;
using Cognite.Extractor.Configuration;
using Cognite.Extractor.Logging;
using Cognite.Extractor.Metrics;
using Cognite.Extractor.StateStorage;

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
                                        params int[] acceptedConfigVersions) where T : VersionedConfig
        {
            var config = ConfigurationUtils.TryReadConfigFromFile<T>(path, acceptedConfigVersions);
            services.AddSingleton<T>(config);
            services.AddConfig<T>(config,
                typeof(CogniteConfig), typeof(LoggerConfig), typeof(MetricsConfig), typeof(StateStoreConfig), typeof(BaseConfig));
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
        /// <exception cref="ConfigurationException">Thrown when the version is not valid, 
        /// the yaml file is not found or in case of yaml parsing error</exception>
        /// <returns>Configuration object</returns>
        public static T AddExtractorDependencies<T>(
            this IServiceCollection services,
            string configPath,
            int[] acceptedConfigVersions,
            string appId,
            string userAgent,
            bool addStateStore,
            bool addLogger = true,
            bool addMetrics = true) where T : VersionedConfig
        {
            var config = services.AddConfig<T>(configPath, acceptedConfigVersions);
            services.AddCogniteClient(appId, userAgent, addLogger, addMetrics);
            if (addStateStore) services.AddStateStore();
            if (addLogger) services.AddLogger();
            if (addMetrics) services.AddMetrics();
            return config;
        }
    }
}

