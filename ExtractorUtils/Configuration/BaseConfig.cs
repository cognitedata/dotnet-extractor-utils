using System.Collections.Generic;
using Cognite.Extensions;
using Cognite.Extractor.Configuration;
using Cognite.Extractor.Logging;
using Cognite.Extractor.Metrics;
using Cognite.Extractor.StateStorage;
using Microsoft.Extensions.Logging;

namespace Cognite.Extractor.Utils
{

    /// <summary>
    /// Base configuration object for extractors.
    /// The config should have a version property, so that versioning and compatibility can be tracked by the extractor.
    /// </summary>
    public class BaseConfig : VersionedConfig
    {

        /// <summary>
        /// Logging configuration (optional)
        /// </summary>
        /// <value>A <see cref="LoggerConfig"/> config object</value>
        public LoggerConfig Logger { get; set; } = null!;

        /// <summary>
        /// Metrics configuration (optional)
        /// </summary>
        /// <value>A <see cref="MetricsConfig"/> config object</value>
        public MetricsConfig Metrics { get; set; } = null!;

        /// <summary>
        /// Cognite configuration (optional)
        /// </summary>
        /// <value>A <see cref="CogniteConfig"/> config object</value>
        public CogniteConfig Cognite { get; set; } = null!;

        /// <summary>
        /// Configuration for extraction state storage (optional)
        /// </summary>
        public StateStoreConfig StateStore { get; set; } = null!;
        /// <summary>
        /// Generate default configuration objects if the corresponding tags are not
        /// present in the yaml config file/string
        /// </summary>
        public override void GenerateDefaults()
        {
            if (Cognite == null) Cognite = new CogniteConfig();
            if (Metrics == null) Metrics = new MetricsConfig();
            if (Logger == null) Logger = new LoggerConfig();
            if (StateStore == null) StateStore = new StateStoreConfig();
        }
    }

    #region Cognite configuration

    /// <summary>
    /// Cognite configuration object
    /// </summary>
    public class CogniteConfig
    {
        /// <summary>
        /// The project name
        /// </summary>
        /// <value>project name</value>
        public string? Project { get; set; }

        /// <summary>
        /// API key for authentication (optional)
        /// </summary>
        /// <value>API key</value>
        public string? ApiKey { get; set; }

        /// <summary>
        /// Authenticator config, if a bearer access token is to be used for authentication (optional)
        /// </summary>
        /// <value>Authenticator</value>
        public AuthenticatorConfig? IdpAuthentication { get; set; }

        /// <summary>
        /// API host
        /// </summary>
        /// <value>Absolute Uri for the host. Default: https://api.cognitedata.com</value>
        public string Host { get; set; } = "https://api.cognitedata.com";

        /// <summary>
        /// Configuration for retries of failed requests to CDF.
        /// </summary>
        public RetryConfig CdfRetries { get => _cdfRetries; set { _cdfRetries = value ?? _cdfRetries; } }
        private RetryConfig _cdfRetries = new RetryConfig();

        /// <summary>
        /// Chunking sizes towards CDF 
        /// </summary>
        public ChunkingConfig CdfChunking { get => _cdfChunking; set { _cdfChunking = value ?? _cdfChunking; } }
        private ChunkingConfig _cdfChunking = new ChunkingConfig();

        /// <summary>
        /// Throttling of requests to CDF
        /// </summary>
        public ThrottlingConfig CdfThrottling { get => _cdfThrottling; set { _cdfThrottling = value ?? _cdfThrottling; } }
        private ThrottlingConfig _cdfThrottling = new ThrottlingConfig();

        /// <summary>
        /// Enables logging of Cognite Sdk operations. Enabled by default.
        /// Leaving this empty also disables.
        /// </summary>
        public SdkLoggingConfig? SdkLogging { get; set; }

        /// <summary>
        /// Optional replacement for non-finite double values in datapoints
        /// </summary>
        public double? NanReplacement { get; set; }

        /// <summary>
        /// Configuration for automatically reporting extraction pipeline runs.
        /// </summary>
        public ExtractionRunConfig? ExtractionPipeline { get; set; }

        /// <summary>
        /// Configuration for handling SSL certificates.
        /// </summary>
        public CertificateConfig? Certificates { get; set; }
    }

    /// <summary>
    /// Cognite Sdk logging configuration
    /// </summary>
    public class SdkLoggingConfig
    {
        /// <summary>
        /// Disables Sdk logging 
        /// </summary>
        /// <value></value>
        public bool Disable { get; set; }

        /// <summary>
        /// Cognite Sdk logs are diplayed using this level.
        /// </summary>
        /// <value>One of the <see cref="LogLevel"/> levels, case insensitive</value>
        public LogLevel Level { get; set; } = LogLevel.Debug;

        /// <summary>
        /// Format of the log message.
        /// Default is <c>"CDF ({Message}): {HttpMethod} {Url} - {Elapsed} ms"</c>
        /// </summary>
        /// <returns>String format</returns>
        public string Format { get; set; } = "CDF ({Message}): {HttpMethod} {Url} - {Elapsed} ms";
    }

    /// <summary>
    /// Chunking configuration
    /// </summary>
    public class ChunkingConfig
    {
        /// <summary>
        /// Maximum number of time series per time series request
        /// </summary>
        /// <value>Maximum chunk size</value>
        public int TimeSeries { get; set; } = 1_000;

        /// <summary>
        /// Maximum number of assets per asset request
        /// </summary>
        /// <value>Maximum chunk size</value>
        public int Assets { get; set; } = 1_000;

        /// <summary>
        /// Maximum number of time series per data point request
        /// </summary>
        /// <value>Maximum chunk size</value>
        public int DataPointTimeSeries { get; set; } = 10_000;

        /// <summary>
        /// Maximum number of ranges per data point delete request
        /// </summary>
        /// <value>Maximum chunk size</value>
        public int DataPointDelete { get; set; } = 10_000;

        /// <summary>
        /// Maximum number of time series per data point list request
        /// </summary>
        /// <value>Maximum chunk size</value>
        public int DataPointList { get; set; } = 100;

        /// <summary>
        /// Maximum number of data points per data point update request
        /// </summary>
        /// <value>Maximum chunk size</value>
        public int DataPoints { get; set; } = 100_000;

        /// <summary>
        /// Minimum number of datapoints in a request before gzip should be used.
        /// </summary>
        public int DataPointsGzipLimit { get; set; } = 5_000;

        /// <summary>
        /// Maximum number of rows per Raw row insert request
        /// </summary>
        /// <value>Maximum chunk size</value>
        public int RawRows { get; set; } = 10_000;

        /// <summary>
        /// Maximum number of rows per Raw row delete request
        /// </summary>
        /// <value>Maximum chunk size</value>
        public int RawRowsDelete { get; set; } = 1000;

        /// <summary>
        /// Maximum number of timeseries to check for each request to get latest datapoint
        /// </summary>
        public int DataPointLatest { get; set; } = 100;

        /// <summary>
        /// Maximum number of events to create or retrieve per request
        /// </summary>
        public int Events { get; set; } = 1_000;

        /// <summary>
        /// Maximum number of sequences to create or retrieve per request
        /// </summary>
        public int Sequences { get; set; } = 1_000;

        /// <summary>
        /// Maximum number of sequences per row insert request.
        /// </summary>
        public int SequenceRowSequences { get; set; } = 1000;

        /// <summary>
        /// Maximum number of rows per sequence per row insert request
        /// </summary>
        public int SequenceRows { get; set; } = 10_000;
    }

    /// <summary>
    /// Throttling configuration
    /// </summary>
    public class ThrottlingConfig
    {
        /// <summary>
        /// Maximum number of parallel requests per time series operation
        /// </summary>
        /// <value>Maximum number of parallel operations</value>
        public int TimeSeries { get; set; } = 20;

        /// <summary>
        /// Maximum number of parallel requests per assets operation
        /// </summary>
        /// <value>Maximum number of parallel operations</value>
        public int Assets { get; set; } = 20;

        /// <summary>
        /// Maximum number of parallel requests per data points operations
        /// </summary>
        /// <value>Maximum number of parallel operations</value>
        public int DataPoints { get; set; } = 10;

        /// <summary>
        /// Maximum number of parallel requests per raw operation
        /// </summary>
        /// <value></value>
        public int Raw { get; set; } = 10;

        /// <summary>
        /// Maximum number of parallel requests per get extracted ranges operation
        /// </summary>
        public int Ranges { get; set; } = 20;

        /// <summary>
        /// Maximum number of parallel requests per events operation
        /// </summary>
        public int Events { get; set; } = 20;

        /// <summary>
        /// Maximum number of parallel requests per sequence operation
        /// </summary>
        public int Sequences { get; set; } = 10;
    }
    /// <summary>
    /// Configure automatic retries on requests to CDF.
    /// </summary>
    public class RetryConfig
    {
        /// <summary>
        /// Timeout in milliseconds for each individual try. Less than or equal to zero for no timeout.
        /// </summary>
        public int Timeout { get; set; } = 80_000;
        /// <summary>
        /// Maximum number of retries. Less than 0 retries forever.
        /// </summary>
        public int MaxRetries { get; set; } = 5;
        /// <summary>
        /// Max delay in ms between each retry. Base delay is calculated according to 125*2^retry ms.
        /// If less than 0, there is no maximum.
        /// </summary>
        public int MaxDelay { get; set; } = 5_000;
    }

    /// <summary>
    /// Configure options relating to SSL certificates.
    /// </summary>
    public class CertificateConfig
    {
        /// <summary>
        /// True to accept all certificates. This must be considered a security risk in most circumstances.
        /// </summary>
        public bool AcceptAll { get; set; }
        /// <summary>
        /// List of certificate thumbprints to manually allow. This is much safer.
        /// </summary>
        public IEnumerable<string>? AllowList { get; set; }
    }

    #endregion
}
