using Cognite.Extractor.Common;
using Cognite.Extractor.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.Extractor.Utils
{
    /// <summary>
    /// Whether configuration is read from CDF or stored locally.
    /// </summary>
    public enum ConfigurationMode
    {
        /// <summary>
        /// Config file is read locally.
        /// </summary>
        Local,
        /// <summary>
        /// Config file is remote.
        /// </summary>
        Remote
    }

    /// <summary>
    /// Configuration object used locally when config files are read from CDF.
    /// </summary>
    public class RemoteConfig : VersionedConfig
    {
        /// <summary>
        /// Configuration type
        /// </summary>
        public ConfigurationMode Type { get; set; } = ConfigurationMode.Local;

        /// <summary>
        /// Cognite configuration object.
        /// </summary>
        public CogniteConfig CogniteConfig { get; set; } = null!;

        /// <inheritdoc />
        public override void GenerateDefaults()
        {
            CogniteConfig = new CogniteConfig();
        }
    }


    /// <summary>
    /// State for remote config manager, which is reused between initialization and normal operation.
    /// This lets the state be created after services are initialized, giving it logger, etc.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class RemoteConfigState<T> where T : VersionedConfig
    {
        /// <summary>
        /// Current revision in use.
        /// </summary>
        public int CurrentRevision { get; set; }
        /// <summary>
        /// Current config object.
        /// </summary>
        public T? Config { get; set; }
    }

    /// <summary>
    /// Class to handle fetching of remote config objects.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class RemoteConfigManager<T> where T : VersionedConfig
    {
        private readonly RemoteConfigState<T> _state;

        private readonly CogniteDestination _destination;
        private readonly string? _bufferFilePath;
        private readonly int[]? _acceptedConfigVersions;
        private readonly string _pipelineId;
        private readonly ILogger<RemoteConfigManager<T>> _logger;
        private readonly RemoteConfig _remoteConfig;
        /// <summary>
        /// Current configuration object, if fetched.
        /// </summary>
        public T? Config => _state.Config;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="destination">Cognite destination to use to fetch data from CDF.</param>
        /// <param name="configFilePath">Path to local config file, used for buffering.</param>
        /// <param name="remoteConfig">Remote config object</param>
        /// <param name="state">Shared remote config manager state</param>
        /// <param name="bufferConfigFile">True to buffer the config file.</param>
        /// <param name="acceptedConfigVersions">List of accepted values of the "version" parameter, or null.</param>
        /// <param name="logger">Logger to use</param>
        public RemoteConfigManager(
            CogniteDestination destination,
            ILogger<RemoteConfigManager<T>>? logger,
            RemoteConfig remoteConfig,
            RemoteConfigState<T> state,
            string? configFilePath,
            bool bufferConfigFile,
            int[]? acceptedConfigVersions)
        {
            _destination = destination;
            _bufferFilePath = bufferConfigFile ? $"{Path.GetDirectoryName(configFilePath)}/_temp_{Path.GetFileName(configFilePath)}" : null;
            _acceptedConfigVersions = acceptedConfigVersions;
            _pipelineId = remoteConfig?.CogniteConfig?.ExtractionPipeline?.PipelineId ?? throw new ConfigurationException("Extraction pipeline id may not be null");
            _logger = logger ?? new NullLogger<RemoteConfigManager<T>>();
            _state = state;
            _remoteConfig = remoteConfig;
        }

        private async Task<(T config, int revision)> FetchLatestInternal(CancellationToken token)
        {
            try
            {
                var rawConfig = await _destination.CogniteClient.Playground.ExtPipeConfigs.GetCurrentConfig(_pipelineId, token).ConfigureAwait(false);
                var config = ConfigurationUtils.TryReadConfigFromString<T>(rawConfig.Config, _acceptedConfigVersions);

                if (_bufferFilePath != null)
                {
                    File.WriteAllText(_bufferFilePath, rawConfig.Config);
                }

                if (config is CogniteConfig cogniteConfig)
                {
                    cogniteConfig.IdpAuthentication = _remoteConfig.CogniteConfig.IdpAuthentication;
                    cogniteConfig.Project = _remoteConfig.CogniteConfig.Project;
                    cogniteConfig.ApiKey = _remoteConfig.CogniteConfig.ApiKey;
                    cogniteConfig.ExtractionPipeline = _remoteConfig.CogniteConfig.ExtractionPipeline;
                }

                _state.Config = config;
                

                return (config, rawConfig.Revision);
            }
            catch (Exception ex)
            {
                if (Config == null && _bufferFilePath != null)
                {
                    if (!File.Exists(_bufferFilePath)) throw new ConfigurationException($"Could not retrieve remote configuration, and local buffer does not exist: {ex.Message}", ex);
                    return (ConfigurationUtils.TryReadConfigFromFile<T>(_bufferFilePath, _acceptedConfigVersions), 0);
                }
                else if (Config != null)
                {
                    return (Config, _state.CurrentRevision);
                }
                throw new ConfigurationException($"Could not retrieve remote configuration: {ex.Message}", ex);
            }
        }

        internal async Task<T> FetchLatestThrowOnFailure(CancellationToken token)
        {
            var (config, revision) = await FetchLatestInternal(token).ConfigureAwait(false);
            _state.CurrentRevision = revision;
            return config;
        }

        /// <summary>
        /// Fetch latest configuration file from CDF.
        /// </summary>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The new configuration object if one was found. If none was found or the revision number was unchanged, this returns null.</returns>
        public async Task<T?> FetchLatest(CancellationToken token)
        {
            try
            {
                var (config, revision) = await FetchLatestInternal(token).ConfigureAwait(false);
                if (revision == _state.CurrentRevision || revision == 0)
                {
                    return null;
                }
                _state.CurrentRevision = revision;
                return config;
            }
            catch (Exception ex)
            {
                _logger.LogWarning("Failed to fetch latest configuration file from CDF: {Message}", ex.Message);
                return null;
            }
        }
    }
}
