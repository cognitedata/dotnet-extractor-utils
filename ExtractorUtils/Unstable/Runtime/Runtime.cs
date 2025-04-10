using System;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extractor.Common;
using Cognite.Extractor.Configuration;
using Cognite.Extractor.Logging;
using Cognite.Extractor.Metrics;
using Cognite.Extractor.StateStorage;
using Cognite.Extractor.Utils.Unstable.Configuration;
using Cognite.Extractor.Utils.Unstable.Tasks;
using CogniteSdk;
using Google.Protobuf.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Cognite.Extractor.Utils.Unstable.Runtime
{
    enum ExtractorRunResult
    {
        /// <summary>
        /// The extractor shut down normally.
        /// </summary>
        CleanShutdown,
        /// <summary>
        /// The extractor failed before actually starting. Typically requires backoff.
        /// </summary>
        EarlyError,
        /// <summary>
        /// The extractor crashed.
        /// </summary>
        Error,
    }

    /// <summary>
    /// Runtime for extractors. See <see cref="ExtractorRuntimeBuilder{TConfig, TExtractor}"/>
    /// for how to create a runtime instance.
    /// </summary>
    /// <typeparam name="TConfig">Configuration type.</typeparam>
    /// <typeparam name="TExtractor">Extractor type.</typeparam>
    public class ExtractorRuntime<TConfig, TExtractor> : IDisposable
    where TConfig : VersionedConfig
    where TExtractor : BaseExtractor<TConfig>
    {
        private readonly ExtractorRuntimeBuilder<TConfig, TExtractor> _params;
        private readonly ConfigSource<TConfig> _configSource;
        private readonly ConnectionConfig? _connectionConfig;
        private readonly CancellationTokenSource _source;
        private readonly IServiceProvider _setupServiceProvider;
        private bool disposedValue;

        private AutoResetEvent _revisionChangedEvent = new AutoResetEvent(false);

        private bool _isFatal;

        private ILogger _activeLogger;


        internal ExtractorRuntime(
            ExtractorRuntimeBuilder<TConfig, TExtractor> builder,
            ConfigSource<TConfig> configSource,
            ConnectionConfig? connectionConfig,
            CancellationTokenSource source
        )
        {
            _params = builder;
            _configSource = configSource;
            _connectionConfig = connectionConfig;
            _source = source;
            _activeLogger = builder.StartupLogger;
            var startupServices = new ServiceCollection();
            if (_params.ExternalServices != null)
            {
                startupServices.Add(_params.ExternalServices);
            }
            if (connectionConfig != null)
            {
                startupServices.AddConfig(connectionConfig, typeof(ConnectionConfig));
                startupServices.AddCogniteClient(_params.AppId, _params.UserAgent, _params.AddLogger,
                    _params.AddMetrics, _params.SetupHttpClient, false);
            }
            _setupServiceProvider = startupServices.BuildServiceProvider();
        }

        private bool _isRunning;
        private object _runningLock = new object();

        /// <summary>
        /// Start the runtime. This method may only be called once.
        /// </summary>
        /// <returns>Does not return until the extractor is stopped or cancelled.</returns>
        public async Task Run()
        {
            lock (_runningLock)
            {
                if (_isRunning) throw new InvalidOperationException("Extractor runtime already started");
                _isRunning = true;
            }
            int backoff = 0;
            while (!_source.IsCancellationRequested)
            {
                var startTime = DateTime.UtcNow;
                var result = await RunExtractorIteration().ConfigureAwait(false);
                if (_params.RestartPolicy == ExtractorRestartPolicy.Never || _source.IsCancellationRequested) break;
                if (result == ExtractorRunResult.EarlyError)
                {
                    _isFatal = true;
                    backoff += 1;
                }
                else if (result == ExtractorRunResult.Error)
                {
                    _isFatal = false;
                    backoff += 1;
                    var elapsed = DateTime.UtcNow - startTime;
                    // If the extractor shut down quickly, avoid immediately restarting
                    // it, since this can really hit source systems hard.
                    if (elapsed >= TimeSpan.FromSeconds(600))
                    {
                        backoff = 0;
                    }
                }
                else if (result == ExtractorRunResult.CleanShutdown)
                {
                    if (_params.RestartPolicy == ExtractorRestartPolicy.OnError)
                    {
                        _activeLogger.LogInformation("Extractor closed cleanly, shutting down");
                        break;
                    }

                    _isFatal = false;
                    backoff = 0;
                }

                if (backoff == 0)
                {
                    _activeLogger.LogInformation("Restarting extractor");
                    backoff += 1;
                    continue;
                }

                var backoffTime = TimeSpan.FromSeconds(5 * Math.Pow(2, Math.Min(4, backoff)));
                _activeLogger.LogInformation("Restarting extractor after {Time}", backoffTime);
                backoff += 1;
                await Task.Delay(backoffTime, _source.Token).ConfigureAwait(false);
            }
        }

        private static Exception ProcessConfigException(Exception ex)
        {
            Exception? exception = null;
            if (ex is TargetInvocationException targetExc)
            {
                if (targetExc.InnerException is ConfigurationException configExc) exception = configExc;
                else if (targetExc.InnerException is AggregateException aggregateExc)
                {
                    exception = aggregateExc.Flatten().InnerExceptions.OfType<ConfigurationException>().FirstOrDefault();
                }
                if (exception == null)
                {
                    exception = new ConfigurationException($"Failed to load config file: {targetExc.Message}", targetExc);
                }
            }
            else if (ex is AggregateException aggregateExc)
            {
                exception = aggregateExc.Flatten().InnerExceptions.OfType<ConfigurationException>().FirstOrDefault();
                if (exception == null)
                {
                    exception = new ConfigurationException($"Failed to load config file: {ex.Message}", ex);
                }
            }
            else if (ex is ConfigurationException configExc)
            {
                return configExc;
            }
            else
            {
                return new ConfigurationException($"Failed to load configuration: {ex.Message}", ex);
            }

            return exception;
        }

        /// <summary>
        /// Run the extractor once.
        /// </summary>
        /// <returns>Returns the type of extractor termination.
        /// Extractors generally do not terminate.</returns>
        private async Task<ExtractorRunResult> RunExtractorIteration()
        {
            var services = new ServiceCollection();

            if (_params.ExternalServices != null)
            {
                services.Add(_params.ExternalServices);
            }

            var bootstrap = new BootstrapErrorReporter(_setupServiceProvider.GetService<Client>(), _connectionConfig?.Integration, _activeLogger);

            try
            {
                // Reset the revision changed event as late as possible, to avoid
                // restarting unnecessarily.
                _revisionChangedEvent.Reset();

                var newConfig = await _configSource.ResolveConfig(null, bootstrap, _source.Token).ConfigureAwait(false);
                if (_isFatal && !newConfig)
                {
                    _activeLogger.LogDebug("No new after fatal error, retrying");
                    return ExtractorRunResult.EarlyError;
                }
            }
            catch (Exception ex)
            {
                ex = ProcessConfigException(ex);
                _activeLogger.LogError(ex, "Failed to resolve config");
                await bootstrap.Flush(_source.Token).ConfigureAwait(false);
                return ExtractorRunResult.EarlyError;
            }

            var config = _configSource.GetConfigWrapper();
            // Register well-known config types.
            var configTypes = (_params.ConfigTypes ?? Enumerable.Empty<Type>())
                .Append(typeof(TConfig))
                .Concat(new[] {
                    typeof(BaseCogniteConfig),
                    typeof(LoggerConfig),
                    typeof(HighAvailabilityConfig),
                    typeof(MetricsConfig),
                    typeof(StateStoreConfig),
                })
                .Distinct().ToArray();
            services.AddConfig(config.Config, configTypes);
            services.AddConfig(_connectionConfig, typeof(ConnectionConfig));
            services.AddSingleton(config);
            services.AddSingleton<ExtractorTaskScheduler>();

            if (_connectionConfig != null)
            {
                services.AddConfig(_connectionConfig, typeof(ConnectionConfig));
                services.AddCogniteClient(_params.AppId, _params.UserAgent, _params.AddLogger,
                    _params.AddMetrics, _params.SetupHttpClient, false);
                services.AddCogniteDestination();
            }

            if (_connectionConfig?.Integration != null)
            {
                services.AddSingleton<IIntegrationSink>(provider =>
                    new CheckInWorker(
                        _connectionConfig.Integration,
                        provider.GetRequiredService<ILogger<CheckInWorker>>(),
                        provider.GetRequiredService<Client>(),
                        (rev) => _revisionChangedEvent.Set(),
                        config.Revision,
                        _params.RetryStartupRequest));
            }
            else
            {
                services.AddSingleton<IIntegrationSink>(provider => new LogIntegrationSink(provider.GetRequiredService<ILogger<LogIntegrationSink>>()));
            }

            if (_params.AddStateStore) services.AddStateStore();
            if (_params.AddLogger) services.AddLogger(_params.BuildLogger);
            if (_params.AddMetrics) services.AddCogniteMetrics();

            services.AddSingleton<TExtractor>();
            services.AddSingleton<BaseExtractor<TConfig>>(prov => prov.GetRequiredService<TExtractor>());

            if (_params.OnConfigure != null)
            {
                _params.OnConfigure(config.Config, _params, services);
            }

            var provider = services.BuildServiceProvider();
            await using (provider.ConfigureAwait(false))
            {
                using var internalTokenSource = CancellationTokenSource.CreateLinkedTokenSource(_source.Token);
                TExtractor extractor;
                try
                {
                    if (_params.AddMetrics)
                    {
                        var metrics = provider.GetRequiredService<MetricsService>();
                        metrics.Start();
                    }
                    if (_params.AddLogger)
                    {
                        _activeLogger = provider.GetRequiredService<ILogger<TExtractor>>();
                    }
                    extractor = provider.GetRequiredService<TExtractor>();
                    if (_params.OnCreateExtractor != null)
                    {
                        var destination = provider.GetService<CogniteDestination>();
                        _params.OnCreateExtractor(destination, extractor);
                    }
                }
                catch (Exception ex)
                {
                    _activeLogger.LogError(ex, "Failed to build extractor: {msg}", ex.Message);
                    // Possibly a config error, but this would be a bug in the extractor.
                    // Extractors should strive to report all possible config errors before
                    // constructing the extractor itself.
                    return ExtractorRunResult.EarlyError;
                }

                try
                {
                    var waitTask = CommonUtils.WaitAsync(_revisionChangedEvent, Timeout.InfiniteTimeSpan, CancellationToken.None);
                    var extractorTask = extractor.Start(internalTokenSource.Token);

                    var completed = await Task.WhenAny(waitTask, extractorTask).ConfigureAwait(false);
                    if (_source.IsCancellationRequested)
                    {
                        _activeLogger.LogInformation("Extractor stopped manually");
                        return ExtractorRunResult.CleanShutdown;
                    }
                    if (completed == waitTask)
                    {
                        _activeLogger.LogInformation("Revision changed, reloading config");
                        internalTokenSource.Cancel();
                        await extractorTask.ConfigureAwait(false);
                    }
                }
                catch (OperationCanceledException) when (internalTokenSource.IsCancellationRequested)
                {
                    _activeLogger.LogInformation("Extractor stopped manually");
                }
                catch (Exception ex)
                {
                    if (ex is AggregateException aex) ex = aex.Flatten().InnerExceptions.First();

                    if (_source.IsCancellationRequested)
                    {
                        _activeLogger.LogWarning("Extractor stopped manually");
                        return ExtractorRunResult.CleanShutdown;
                    }
                    else
                    {
                        _params.LogException(_activeLogger, ex, "Extractor crashed unexpectedly");
                    }

                    if (_params.IsFatalException?.Invoke(ex) ?? false)
                    {
                        _activeLogger.LogInformation("Fatal exception encountered, waiting until new config is provided");
                        _isFatal = true;
                    }
                    return ExtractorRunResult.Error;
                }

                return ExtractorRunResult.CleanShutdown;
            }
        }

        /// <summary>
        /// Dispose managed resources.
        /// </summary>
        /// <param name="disposing">Whether to actually dispose resources.</param>
        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    _source.Cancel();
                    _source.Dispose();
                }

                disposedValue = true;
            }
        }

        /// <inheritdoc />
        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }
    }
}