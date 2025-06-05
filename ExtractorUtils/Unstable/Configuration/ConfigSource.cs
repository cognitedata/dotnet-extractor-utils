using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extractor.Common;
using Cognite.Extractor.Configuration;
using Cognite.Extractor.Utils.Unstable.Tasks;
using CogniteSdk;
using CogniteSdk.Alpha;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Cognite.Extractor.Utils.Unstable.Configuration
{
    /// <summary>
    /// Wrapper around a config file with
    /// extra information about the active revision.
    /// </summary>
    /// <typeparam name="TConfig"></typeparam>
    public class ConfigWrapper<TConfig>
    {
        /// <summary>
        /// Configuration object.
        /// </summary>
        public TConfig Config { get; }
        /// <summary>
        /// Revision number or null to mean
        /// local config.
        /// </summary>
        public int? Revision { get; }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="config">Configuration object.</param>
        /// <param name="revision">Revision info.</param>
        public ConfigWrapper(TConfig config, int? revision)
        {
            Config = config;
            Revision = revision;
        }
    }

    /// <summary>
    /// Abstract configuration source.
    /// </summary>
    /// <typeparam name="T">Type of config object to load.</typeparam>
    public abstract class ConfigSource<T> where T : VersionedConfig
    {
        /// <summary>
        /// Current configuration object, if fetched.
        /// </summary>
        public T? Config { get; protected set; }

        /// <summary>
        /// Current revision, null if a local revision is active.
        /// </summary>
        public int? Revision { get; protected set; }

        /// <summary>
        /// Read a file from the local filesystem to a string.
        /// </summary>
        /// <param name="path">Path to the local file</param>
        /// <returns>File as a string</returns>
        protected async Task<string> ReadLocalFile(string path)
        {
            using var reader = new StreamReader(path);

            return await reader.ReadToEndAsync().ConfigureAwait(false);
        }

        /// <summary>
        /// Get a wrapper around configuration with information about the remote revision
        /// if applicable.
        /// This will fail if a config has not been loaded.
        /// </summary>
        /// <returns></returns>
        public abstract ConfigWrapper<T> GetConfigWrapper();

        /// <summary>
        /// Load configuration, either from a local file or from CDF.
        /// </summary>
        /// <param name="targetRevision">Revision to load. Ignored for local config. Null means load the latest.</param>
        /// <param name="reporter">Error reporter for writing configuration errors to CDF.</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>True if a new configuration file was loaded, false otherwise.</returns>
        public abstract Task<bool> ResolveConfig(int? targetRevision, BaseErrorReporter reporter, CancellationToken token);
    }

    /// <summary>
    /// Configuration source for local files.
    /// </summary>
    /// <typeparam name="T">Type of config object to load.</typeparam>
    public class LocalConfigSource<T> : ConfigSource<T> where T : VersionedConfig
    {
        private readonly string _configFilePath;
        private readonly ILogger _logger;
        private DateTime _lastLocalConfigModifyTime = DateTime.MinValue;
        private string? _lastErrorMsg;

        /// <summary>
        /// The path to the config file this source will read from.
        /// </summary>
        public string ConfigFilePath => _configFilePath;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="logger">Logger</param>
        /// <param name="configFilePath">Path to local config file.</param>
        public LocalConfigSource(ILogger? logger, string configFilePath)
        {
            _logger = logger ?? new NullLogger<LocalConfigSource<T>>();
            _configFilePath = configFilePath;
        }

        /// <inheritdoc />
        public override ConfigWrapper<T> GetConfigWrapper()
        {
            if (Config == null) throw new InvalidOperationException("Attempt to get config wrapper before config has been resolved");
            return new ConfigWrapper<T>(Config, null);
        }

        /// <inheritdoc />
        public override async Task<bool> ResolveConfig(int? targetRevision, BaseErrorReporter reporter, CancellationToken token)
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
            if (lastTime <= _lastLocalConfigModifyTime && Config != null)
            {
                return false;
            }

            // Avoid reporting a fatal error if the file has not changed.
            bool isNewConfig = lastTime > _lastLocalConfigModifyTime;
            _lastLocalConfigModifyTime = lastTime;


            string rawConfig;
            try
            {
                _logger.LogInformation("Reading local config file from {Path}", _configFilePath);
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
                Config = config;
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
    }

    /// <summary>
    /// Configuration source for remote config files.
    /// </summary>
    /// <typeparam name="T">Type of configuration object.</typeparam>
    public class RemoteConfigSource<T> : ConfigSource<T> where T : VersionedConfig
    {
        private readonly Client _client;
        private readonly ILogger _logger;
        private readonly string _integrationId;
        private readonly string? _bufferFilePath;
        private string? _lastErrorMsg;
        private int? _lastAttemptedRevision;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="client">CDF Client</param>
        /// <param name="logger">Logger</param>
        /// <param name="integrationId">ID of the integration to write to.</param>
        /// <param name="configFilePath">Path to local configuration file. The folder is used to create local copies
        /// of remote config files if <paramref name="bufferConfigFile"/> is set.</param>
        /// <param name="bufferConfigFile">Whether to store a local copy of the configuration file.</param>
        public RemoteConfigSource(
            Client client,
            ILogger? logger,
            string integrationId,
            string configFilePath,
            bool bufferConfigFile)
        {
            _client = client;
            _logger = logger ?? new NullLogger<ConfigSource<T>>();
            _integrationId = integrationId;
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

        /// <inheritdoc />
        public override ConfigWrapper<T> GetConfigWrapper()
        {
            if (Config == null || Revision == null) throw new InvalidOperationException("Attempt to get config wrapper before config has been resolved");
            return new ConfigWrapper<T>(Config, Revision);
        }

        private bool ShouldLoadNewConfig(int? targetRevision)
        {
            if (Config == null) return true;

            return targetRevision == null || targetRevision != Revision;
        }

        /// <inheritdoc />
        public override async Task<bool> ResolveConfig(int? targetRevision, BaseErrorReporter reporter, CancellationToken token)
        {
            if (reporter == null) throw new ArgumentNullException(nameof(reporter));
            if (!ShouldLoadNewConfig(targetRevision)) return false;

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
                Config = config;
                Revision = revision;
            }
            catch (Exception ex)
            {
                _logger.LogCritical($"Failed to parse config: {isNewConfig}");
                if (isNewConfig)
                {
                    _logger.LogCritical("Writing to reporter");
                    reporter.Fatal($"Failed to parse configuration file from CDF: {ex.Message}");
                }
                throw;
            }
            return true;
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
                _logger.LogError("Failed to retrieve configuration from integration {Int}: {Message}", _integrationId, ex.Message);
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

    /// <summary>
    /// Configuration source for a config that was provided externally,
    /// for example through the command line.
    /// </summary>
    /// <typeparam name="T">Type of configuration object.</typeparam>
    public class StaticConfigSource<T> : ConfigSource<T> where T : VersionedConfig
    {
        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="config">Constant config file to use.</param>
        public StaticConfigSource(T config)
        {
            Config = config;
            Revision = null;
        }

        /// <inheritdoc />
        public override ConfigWrapper<T> GetConfigWrapper()
        {
            return new ConfigWrapper<T>(Config!, null);
        }

        private bool _hasResolved;
        private object _lock = new object();

        /// <inheritdoc />
        public override Task<bool> ResolveConfig(int? targetRevision, BaseErrorReporter reporter, CancellationToken token)
        {
            lock (_lock)
            {
                if (_hasResolved)
                {
                    return Task.FromResult(false);
                }
                _hasResolved = true;
            }

            return Task.FromResult(true);
        }
    }
}