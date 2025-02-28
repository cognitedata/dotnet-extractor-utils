using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extractor.Common;
using Cognite.Extractor.Configuration;
using Cognite.ExtractorUtils.Unstable.Tasks;
using CogniteSdk;
using CogniteSdk.Alpha;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Cognite.ExtractorUtils.Unstable.Configuration
{
    class ConfigState<T> where T : VersionedConfig
    {
        /// <summary>
        /// Current revision in use.
        /// </summary>
        public int? CurrentRevision { get; set; }

        /// <summary>
        /// Type of config file.
        /// </summary>
        public ConfigMode Mode { get; set; }

        /// <summary>
        /// Current config object.
        /// </summary>
        public T? Config { get; set; }

        public ConfigState(ConfigMode mode = ConfigMode.None)
        {
            Mode = mode;
        }
    }

    /// <summary>
    /// Where to read configuration from.
    /// </summary>
    enum ConfigMode
    {
        /// <summary>
        /// Read from a local config file.
        /// </summary>
        Local,
        /// <summary>
        /// Read from a remote config file.
        /// </summary>
        Remote,
        /// <summary>
        /// Not yet read.
        /// </summary>
        None
    }

    /// <summary>
    /// Configuration source that reads configuration from CDF.
    /// </summary>
    /// <typeparam name="T">Config type</typeparam>
    public class ConfigSource<T> where T : VersionedConfig
    {
        private readonly Client _client;
        private readonly ILogger _logger;
        private readonly ConfigState<T> _state;
        private readonly string? _integrationId;
        private readonly string? _bufferFilePath;
        private readonly string _configFilePath;

        /// <summary>
        /// Configured local config file path.
        /// </summary>
        public string ConfigFilePath => _configFilePath;
        private string? _lastErrorMsg;


        private int? _lastAttemptedRevision;

        /// <summary>
        /// Current configuration object, if fetched.
        /// </summary>
        public T? Config => _state.Config;

        /// <summary>
        /// Current revision, null if a local revision is active.
        /// </summary>
        public int? Revision => _state.CurrentRevision;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="client">Cognite client used to read config from integrations API.</param>
        /// <param name="logger">Logger.</param>
        /// <param name="integrationId">ID of integration to read from.</param>
        /// <param name="configFilePath">Path to config file. This folder will also be used to
        /// store the buffered config file if that is enabled. This config file will only be read
        /// when using local config, but the folder is still needed for buffering remote configs.</param>
        /// <param name="bufferConfigFile">Whether to store a local copy of config files
        /// if reading from CDF fails.</param>
        public ConfigSource(
            Client client,
            ILogger? logger,
            string? integrationId,
            string configFilePath,
            bool bufferConfigFile)
        {
            _client = client;
            _logger = logger ?? new NullLogger<ConfigSource<T>>();
            _state = new ConfigState<T>();
            _integrationId = integrationId;
            _configFilePath = configFilePath;
            if (bufferConfigFile)
            {
                var dir = Path.GetDirectoryName(configFilePath);
                if (dir != null)
                {
                    _bufferFilePath = $"{dir}/_temp_{Path.GetFileName(configFilePath)}";
                }
                else
                {
                    _bufferFilePath = $"_temp_{Path.GetFileName(configFilePath)}";
                }
            }
        }

        private DateTime _lastLocalConfigModifyTime = DateTime.MinValue;

        private bool ShouldLoadNewConfig(int? targetRevision, ConfigMode mode)
        {
            if (mode != _state.Mode || _state.Config == null) return true;

            return targetRevision == null || targetRevision != _state.CurrentRevision;
        }

        /// <summary>
        /// Load configuration from a local file.
        /// Returns whether we have loaded a new config.
        /// </summary>
        /// <param name="reporter">Error reporter.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The new config and true/false depending on whether a new config file was loaded.</returns>
        public async Task<bool> ResolveLocalConfig(BaseErrorReporter reporter, CancellationToken token)
        {
            if (reporter == null) throw new ArgumentNullException(nameof(reporter));
            DateTime lastTime;
            try
            {
                lastTime = new FileInfo(_configFilePath).LastWriteTimeUtc;
            }
            catch
            {
                lastTime = DateTime.MinValue;
            }
            if (lastTime <= _lastLocalConfigModifyTime && _state.Mode == ConfigMode.Local && _state.Config != null)
            {
                return false;
            }

            // Avoid reporting a fatal error if the file has not changed.
            bool isNewConfig = lastTime > _lastLocalConfigModifyTime;
            _lastLocalConfigModifyTime = lastTime;


            string rawConfig;
            try
            {
                rawConfig = await ReadLocalFile(_configFilePath).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                var msg = ex.Message;
                if (_lastErrorMsg != msg)
                {
                    reporter.Fatal($"Fatally failed to load configuration file from {_configFilePath}: {ex.Message}");
                }
                _lastErrorMsg = msg;
                throw;
            }

            try
            {
                var config = ConfigurationUtils.TryReadConfigFromString<T>(rawConfig);
                _state.CurrentRevision = null;
                _state.Config = config;
                _state.Mode = ConfigMode.Local;
            }
            catch (Exception ex)
            {
                if (isNewConfig)
                {
                    reporter.Fatal($"Failed to parse configuration file from {_configFilePath}: {ex.Message}");
                }
                throw;
            }
            return true;
        }

        /// <summary>
        /// Load configuration from CDF.
        /// Returns whether we have loaded a new config file.
        /// </summary>
        /// <param name="targetRevision"></param>
        /// <param name="reporter"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        public async Task<bool> ResolveRemoteConfig(int? targetRevision, BaseErrorReporter reporter, CancellationToken token)
        {
            if (reporter == null) throw new ArgumentNullException(nameof(reporter));
            if (!ShouldLoadNewConfig(targetRevision, ConfigMode.Remote)) return false;

            string rawConfig;
            int? revision;
            try
            {
                (rawConfig, revision) = await ReadRemoteConfigInternal(targetRevision, token).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                var msg = ex.Message;
                if (_lastErrorMsg != msg)
                {
                    reporter.Fatal($"Fatally failed to load configuration file from CDF: {msg}");
                }
                _lastErrorMsg = msg;
                throw;
            }

            bool isNewConfig = _lastAttemptedRevision != revision;

            try
            {
                _lastAttemptedRevision = revision;
                var config = ConfigurationUtils.TryReadConfigFromString<T>(rawConfig);
                _state.CurrentRevision = revision;
                _state.Config = config;
                _state.Mode = ConfigMode.Remote;
            }
            catch (Exception ex)
            {
                if (isNewConfig)
                {
                    reporter.Fatal($"Failed to parse configuration file from CDF: {ex.Message}");
                }
                throw;
            }
            return true;
        }

        private async Task<string> ReadLocalFile(string path)
        {
            // if (_configFilePath == null) throw new InvalidOperationException("Attempt to read local config file when no local config file is configured");
            using var reader = new StreamReader(path);

            return await reader.ReadToEndAsync().ConfigureAwait(false);
        }

        private async Task<(string, int?)> ReadRemoteConfigInternal(int? targetRevision, CancellationToken token)
        {
            if (_integrationId == null) throw new InvalidOperationException("Attempt to fetch remote config when no integration is configured");

            ConfigRevision rawConfig;

            try
            {
                rawConfig = await _client.Alpha.Integrations.GetConfigRevisionAsync(_integrationId, targetRevision, token).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogError("Failed to retrieve configuration from integration {Int} file: {Message}", _integrationId, ex.Message);
                if (_bufferFilePath != null)
                {
                    if (!System.IO.File.Exists(_bufferFilePath)) throw new ConfigurationException($"Could not retrieve remote configuration, and local buffer does not exist: {ex.Message}", ex);
                    var bufferText = await ReadLocalFile(_bufferFilePath).ConfigureAwait(false);

                    _logger.LogWarning("Loaded configuration from local config file buffer.");

                    return (bufferText, null);
                }
                throw new ConfigurationException($"Could not retrieve remote configuration: {ex.Message}", ex);
            }

            if (_bufferFilePath != null)
            {
                try
                {
                    System.IO.File.WriteAllText(_bufferFilePath, rawConfig.Config);
                }
                catch (Exception write_ex)
                {
                    _logger.LogWarning("Failed to write remote config to local config file buffer, disabling local config buffer: {}", write_ex.Message);
                    if (System.IO.File.Exists(_bufferFilePath))
                    {
                        throw new ConfigurationException($"Failed to write to buffer file, but it already exists: {write_ex.Message}. This is a fatal error, as it may cause configuration to be unexpectedly out of sync in the future. Either delete the local buffer file ({_bufferFilePath}), or ensure the extractor has write access to it.");
                    }
                }
            }

            return (rawConfig.Config, rawConfig.Revision);
        }
    }
}