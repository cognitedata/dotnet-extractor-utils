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
            services.AddConfig<T>(config, typeof(CogniteConfig), typeof(LoggerConfig), typeof(MetricsConfig), typeof(StateStoreConfig));
            return config;
        }
    }

}
