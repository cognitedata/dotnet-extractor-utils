using System.Collections.Generic;

namespace ExtractorUtils {
    
    /// <summary>
    /// Base configuration object for extractors.
    /// The config should have a version property, so that versioning and compatibility can be tracked by the extractor.
    /// </summary>
    public class BaseConfig {
        
        /// <summary>
        /// Current version of this config object
        /// </summary>
        public int Version { get; set; }

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
    }

#region Logging configuration
    /// <summary>
    /// Logger configuration object
    /// </summary>
    public class LoggerConfig
    {
        /// <summary>
        /// Logging to console (optional)
        /// </summary>
        /// <value>A <see cref="ConsoleConfig"/> config object</value>
        public ConsoleConfig Console { get; set; }
        
        /// <summary>
        /// Logging to file (optional)
        /// </summary>
        /// <value>A <see cref="FileConfig"/> config object</value>
        public FileConfig File { get; set; }
        
        /// <summary>
        /// Logging to Google Stackdriver (optional)
        /// </summary>
        /// <value>A <see cref="StackdriverConfig"/> config object</value>
        public StackdriverConfig Stackdriver { get; set; }
    }

    /// <summary>
    /// Logging to console configuration object
    /// </summary>
    public class ConsoleConfig
    {
        /// <summary>
        /// Logging level.
        /// </summary>
        /// <value>One of 'verbose', 'debug', 'information', 'warning', 'error', 'fatal'</value>
        public string Level { get; set; }
    }

    /// <summary>
    /// Logging to file configuration object
    /// </summary>
    public class FileConfig
    {
        /// <summary>
        /// Logging level.
        /// </summary>
        /// <value>One of 'verbose', 'debug', 'information', 'warning', 'error', 'fatal'</value>
        public string Level { get; set; }
        
        /// <summary>
        /// Path to the location where the logs will be saved.
        /// Example: <c>'logs/log.txt'</c> will create log files with <c>log</c> prefix followed by a date as suffix, 
        /// and <c>txt</c> as extension in the <c>logs</c> folder.
        /// </summary>
        /// <value>Path to file</value>
        public string Path { get; set; }
        
        /// <summary>
        /// The maximum number of log files that will be retained in the log folder.
        /// </summary>
        /// <value>Maximum number of files</value>
        public int RetentionLimit { get; set; } = 31;

        /// <summary>
        /// Rolling interval for log files.
        /// </summary>
        /// <value>One of 'day', 'hour'. Defaults to 'day'</value>
        public string RollingInterval { get; set; } = "day";
    }

    /// <summary>
    /// Logging to Google Stackdriver configuration object.
    /// </summary>
    /// <remarks>
    /// Logging utiltities to Stackdriver are experimental and log level cannot be set (all log messages are sent).
    /// This requires credentials to Google Cloud Platform: <see cref="LogName"/>
    /// </remarks>
    /// 
    public class StackdriverConfig
    {
        /// <summary>
        /// Path to the credentials file
        /// </summary>
        /// <value>Path to a json file containing credentials</value>
        public string Credentials { get; set; }
        
        /// <summary>
        /// Log name
        /// </summary>
        /// <value>Log name</value>
        public string LogName { get; set; }
    }
#endregion

#region Metrics configuration
    
    /// <summary>
    /// Metrics configuration object
    /// </summary>
    public class MetricsConfig
    {
        /// <summary>
        /// Start a metrics server in the extractor for Prometheus scrape (optional)
        /// </summary>
        /// <value>A <see cref="MetricsServerConfig"/> config object</value>
        public MetricsServerConfig Server { get; set; }
#pragma warning disable CA2227 // Collection properties should be read only
        
        /// <summary>
        /// A list of Prometheus push gateway destinations (optional)
        /// </summary>
        /// <value>A list <see cref="PushGatewayConfig"/> of config objects</value>
        public List<PushGatewayConfig> PushGateways { get; set; }
#pragma warning restore CA2227 // Collection properties should be read only

    }

    /// <summary>
    /// Metrics server configuration
    /// </summary>
    public class MetricsServerConfig
    {
        /// <summary>
        /// Host name
        /// </summary>
        /// <value>Host name for the server. Example: localhost (without scheme and port)</value>
        public string Host { get; set; }
        /// <summary>
        /// Port
        /// </summary>
        /// <value>Server port</value>
        public int Port { get; set; }
    }

    /// <summary>
    /// Push gateway configuration
    /// </summary>
    public class PushGatewayConfig
    {
        /// <summary>
        /// Gateway host
        /// </summary>
        /// <value>Absolute Uri for the host. Example: http://localhost:9091</value>
        public string Host { get; set; }
        /// <summary>
        /// Job name
        /// </summary>
        /// <value>Name of the job</value>
        public string Job { get; set; }
        /// <summary>
        /// Username for basic authentication (optional)
        /// </summary>
        /// <value>Username</value>
        public string Username { get; set; }
        /// <summary>
        /// Password for basic authentication (optional)
        /// </summary>
        /// <value>Password</value>
        public string Password { get; set; }
        /// <summary>
        /// Interval in seconds for pushing data to the gateway.
        /// </summary>
        /// <value></value>
        public int PushInterval { get; internal set; } = 1;
    }

#endregion
}
