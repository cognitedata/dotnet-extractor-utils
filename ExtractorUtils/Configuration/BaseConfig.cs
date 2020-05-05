using Cognite.Extractor.Configuration;
using Cognite.Extractor.Logging;
using Cognite.Extractor.Metrics;

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
        public ChunkingConfig CdfChunking { get; set; } = new ChunkingConfig();
        
        /// <summary>
        /// Throttling of requests to CDF
        /// </summary>
        public ThrottlingConfig CdfThrottling { get; set; } = new ThrottlingConfig();
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
        /// Maximum number of time series per data point request
        /// </summary>
        /// <value>Maximum chunk size</value>
        public int DataPointTimeSeries { get; set; } = 10_000;
        
        /// <summary>
        /// Maximum number of data points per data point update request
        /// </summary>
        /// <value>Maximum chunk size</value>
        public int DataPoints { get; set; } = 100_000;

    }

    /// <summary>
    /// Throttling configuration
    /// </summary>
    public class ThrottlingConfig
    {
        /// <summary>
        /// Maximum number of parallel time series operations
        /// </summary>
        /// <value>Maximum number of parallel operations</value>
        public int TimeSeries { get; set; } = 20;
        
        /// <summary>
        /// Maximum number of parallel data points operations
        /// </summary>
        /// <value>Maximum number of parallel operations</value>
        public int DataPoints { get; set; } = 10;
    }

#endregion
}
