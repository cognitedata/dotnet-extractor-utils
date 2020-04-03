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
}
