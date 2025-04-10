using System;
using System.Threading;
using Cognite.Extractor.Configuration;
using Cognite.Extractor.Utils.Unstable.Configuration;

namespace Cognite.Extractor.Utils.Unstable.Runtime
{
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
        private bool disposedValue;

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