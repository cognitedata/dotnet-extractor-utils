using Cognite.Extractor.Configuration;
using Cognite.Extractor.Logging;
using Cognite.Extractor.Metrics;
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
        public LoggerConfig Logger { get; set; }

        /// <summary>
        /// Metrics configuration (optional)
        /// </summary>
        /// <value>A <see cref="MetricsConfig"/> config object</value>
        public MetricsConfig Metrics { get; set; }

        /// <summary>
        /// Cognite configuration (optional)
        /// </summary>
        /// <value>A <see cref="CogniteConfig"/> config object</value>
        public CogniteConfig Cognite { get; set; }

        /// <summary>
        /// Generate default configuration objects if the corresponding tags are not
        /// present in the yaml config file/string
        /// </summary>
        public override void GenerateDefaults()
        {
            if (Cognite == null) Cognite = new CogniteConfig();
            if (Cognite.CdfChunking == null) Cognite.CdfChunking = new ChunkingConfig(); // default chunking according to CDF limits
            if (Cognite.CdfThrottling == null) Cognite.CdfThrottling = new ThrottlingConfig();
            if (Cognite.SdkLogging == null) Cognite.SdkLogging = new SdkLoggingConfig();
            if (Metrics == null) Metrics = new MetricsConfig();
            if (Logger == null) Logger = new LoggerConfig();
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
        public string Project { get; set; }

        /// <summary>
        /// API key for authentication (optional)
        /// </summary>
        /// <value>API key</value>
        public string ApiKey { get; set; }
        
        /// <summary>
        /// Authenticator config, if a bearer access token is to be used for authentication (optional)
        /// </summary>
        /// <value>Authenticator</value>
        public AuthenticatorConfig IdpAuthentication{ get; set; }
        
        /// <summary>
        /// API host
        /// </summary>
        /// <value>Absolute Uri for the host. Default: https://api.cognitedata.com</value>
        public string Host { get; set; } = "https://api.cognitedata.com";
        
        /// <summary>
        /// Chunking sizes towards CDF 
        /// </summary>
        public ChunkingConfig CdfChunking { get; set; }
        
        /// <summary>
        /// Throttling of requests to CDF
        /// </summary>
        public ThrottlingConfig CdfThrottling { get; set; }

        /// <summary>
        /// Enables logging of Cognite Sdk operations. Enabled by default
        /// </summary>
        public SdkLoggingConfig SdkLogging { get; set; }
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
        public bool Disable { get; set; } = false;
        
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
    /// Authenticator configuration. For more information, read the 
    /// <see href="https://docs.microsoft.com/en-us/azure/active-directory/develop/v2-oauth2-client-creds-grant-flow">OAth 2.0 client credentials flow</see>
    /// </summary>
    public class AuthenticatorConfig
    {
        /// <summary>
        /// The application (client) Id
        /// </summary>
        /// <value>Client Id</value>
        public string ClientId { get; set; }
        
        /// <summary>
        /// The directory tenant
        /// </summary>
        /// <value>Tenant</value>
        public string Tenant { get; set; }
        
        /// <summary>
        /// The client secret
        /// </summary>
        /// <value>Secret</value>
        public string Secret { get; set; }
        
        /// <summary>
        /// Resource scope
        /// </summary>
        /// <value>Scope</value>
        public string Scope { get; set; }

        /// <summary>
        /// Minimum time-to-live for the token (optional)
        /// </summary>
        /// <value>Minimum TTL</value>
        public int MinTtl { get; set; } = 30;
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
        /// Maximum number of rows per Raw row insert request
        /// </summary>
        /// <value>Maximum chunk size</value>
        public int RawRows { get; set; } = 10_000;

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
    }

#endregion
}
