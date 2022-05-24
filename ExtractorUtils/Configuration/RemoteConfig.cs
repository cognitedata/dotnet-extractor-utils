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
    /// Class to handle fetching of remote config objects.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class RemoteConfigManager<T> where T : VersionedConfig
    {
        private int _currentRevision;
        private CogniteDestination _destination;
        private string? _bufferFilePath;
        private int[]? _acceptedConfigVersions;
        private string _pipelineId;
        private ILogger<RemoteConfigManager<T>> _logger;
        /// <summary>
        /// Current configuration object, if fetched.
        /// </summary>
        public T? Config { get; private set; }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="destination">Cognite destination to use to fetch data from CDF.</param>
        /// <param name="configFilePath">Path to local config file, used for buffering.</param>
        /// <param name="bufferConfigFile">True to buffer the config file.</param>
        /// <param name="acceptedConfigVersions">List of accepted values of the "version" parameter, or null.</param>
        /// <param name="logger">Logger to use</param>
        /// <param name="pipelineId">Extraction pipeline id</param>
        public RemoteConfigManager(CogniteDestination destination, ILogger<RemoteConfigManager<T>>? logger, string configFilePath, bool bufferConfigFile, int[]? acceptedConfigVersions, string pipelineId)
        {
            _destination = destination;
            _bufferFilePath = bufferConfigFile ? $"{Path.GetDirectoryName(configFilePath)}/_temp_{Path.GetFileName(configFilePath)}" : null;
            _acceptedConfigVersions = acceptedConfigVersions;
            _pipelineId = pipelineId;
            _logger = logger ?? new NullLogger<RemoteConfigManager<T>>();
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

                Config = config;

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
                    return (Config, _currentRevision);
                }
                throw new ConfigurationException($"Could not retrieve remote configuration: {ex.Message}", ex);
            }
        }

        internal async Task<T> FetchLatestThrowOnFailure(CancellationToken token)
        {
            var (config, revision) = await FetchLatestInternal(token).ConfigureAwait(false);
            _currentRevision = revision;
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
                if (revision == _currentRevision || revision == 0)
                {
                    return null;
                }
                _currentRevision = revision;
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
