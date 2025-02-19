using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extractor.Common;
using Cognite.Extractor.Configuration;
using Cognite.ExtractorUtils.Unstable.Tasks;
using CogniteSdk;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Cognite.ExtractorUtils.Unstable.Configuration
{
    class RemoteConfigState<T> where T : VersionedConfig
    {
        /// <summary>
        /// Current revision in use.
        /// </summary>
        public int? CurrentRevision { get; set; }

        /// <summary>
        /// Type of config file.
        /// </summary>
        public ConfigMode? Mode { get; set; }

        /// <summary>
        /// Current config object.
        /// </summary>
        public T? Config { get; set; }
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
        Remote
    }

    /// <summary>
    /// Configuration source that reads configuration from CDF.
    /// </summary>
    /// <typeparam name="T">Config type</typeparam>
    public class ConfigSource<T> where T : VersionedConfig
    {
        private readonly Client _client;
        private readonly ILogger _logger;
        private readonly RemoteConfigState<T> _state;
        private readonly string? _integrationId;
        private readonly string? _bufferFilePath;
        private readonly string _configFilePath;
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
        /// store the buffered config file if that is enabled.</param>
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
            _state = new RemoteConfigState<T>();
            _integrationId = integrationId;
            _configFilePath = configFilePath;
            if (bufferConfigFile)
            {
                if (Path.GetDirectoryName(configFilePath) != null)
                {
                    _bufferFilePath = $"{Path.GetDirectoryName(configFilePath)}/_temp_{Path.GetFileName(configFilePath)}";
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
        public async Task<(T, bool)> ResolveLocalConfig(BaseErrorReporter reporter, CancellationToken token)
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
                return (_state.Config!, false);
            }

            bool isNewConfig = lastTime > _lastLocalConfigModifyTime;
            _lastLocalConfigModifyTime = lastTime;


            string rawConfig;
            try
            {
                rawConfig = await ReadLocalConfigFile(token).ConfigureAwait(false);
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
            return (_state.Config, true);
        }

        /// <summary>
        /// Load configuration from CDF.
        /// Returns whether we have loaded a new config file.
        /// </summary>
        /// <param name="targetRevision"></param>
        /// <param name="reporter"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        public async Task<(T, bool)> ResolveRemoteConfig(int? targetRevision, BaseErrorReporter reporter, CancellationToken token)
        {
            if (reporter == null) throw new ArgumentNullException(nameof(reporter));
            if (!ShouldLoadNewConfig(targetRevision, ConfigMode.Remote)) return (_state.Config!, false);

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

            bool isNewConfig = _lastAttemptedRevision == revision;

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
            return (_state.Config, true);
        }

        private async Task<string> ReadLocalConfigFile(CancellationToken token)
        {
            if (_configFilePath == null) throw new InvalidOperationException("Attempt to read local config file when no local config file is configured");
            using var reader = new StreamReader(_configFilePath);

#pragma warning disable CA2016 // Forward the 'CancellationToken' parameter to methods. Not in .NET standard 2.0
            return await reader.ReadToEndAsync().ConfigureAwait(false);
#pragma warning restore CA2016 // Forward the 'CancellationToken' parameter to methods
        }

        private async Task<(string, int?)> ReadRemoteConfigInternal(int? targetRevision, CancellationToken token)
        {
            if (_integrationId == null) throw new InvalidOperationException("Attempt to fetch remote config when no integration is configured");

            try
            {
                var rawConfig = await _client.Alpha.Integrations.GetConfigRevisionAsync(_integrationId, targetRevision, token).ConfigureAwait(false);

                if (_bufferFilePath != null)
                {
                    try
                    {
                        System.IO.File.WriteAllText(_bufferFilePath, rawConfig.Config);
                    }
                    catch (Exception write_ex)
                    {
                        _logger.LogWarning("Failed to write remote config to local config file buffer, disabling local config buffer: {}", write_ex.Message);
                    }
                }

                return (rawConfig.Config, rawConfig.Revision);
            }
            catch (Exception ex)
            {
                _logger.LogError("Failed to retrieve configuration from integration {Int} file: {Message}", _integrationId, ex.Message);
                if (_bufferFilePath != null)
                {
                    if (!System.IO.File.Exists(_bufferFilePath)) throw new ConfigurationException($"Could not retrieve remote configuration, and local buffer does not exist: {ex.Message}", ex);
                    using var reader = new StreamReader(_configFilePath);
#pragma warning disable CA2016 // Forward the 'CancellationToken' parameter to methods. Not in .NET standard 2.0
                    var text = await reader.ReadToEndAsync().ConfigureAwait(false);
#pragma warning restore CA2016 // Forward the 'CancellationToken' parameter to methods

                    _logger.LogWarning("Loaded configuration from local config file buffer.");

                    return (text, null);
                }
                throw new ConfigurationException($"Could not retrieve remote configuration: {ex.Message}", ex);
            }
        }
    }
}