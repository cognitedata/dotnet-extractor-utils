
namespace Cognite.Extractor.Logging
{

    /// <summary>
    /// Logging configuration object
    /// </summary>
    public class LoggerConfig
    {
        /// <summary>
        /// Logging to console (optional)
        /// </summary>
        /// <value>A <see cref="LogConfig"/> config object</value>
        public ConsoleConfig? Console { get; set; }

        /// <summary>
        /// Logging to file (optional)
        /// </summary>
        /// <value>A <see cref="FileConfig"/> config object</value>
        public FileConfig? File { get; set; }

        /// <summary>
        /// Adds a listener that uses the configured logger to output <see cref="System.Diagnostics.Trace"/> messages
        /// </summary>
        /// <value></value>
        public TraceListenerConfig? TraceListener { get; set; }

    }

    /// <summary>
    /// Base class for log configuration
    /// </summary>
    public class LogConfig
    {
        /// <summary>
        /// Logging level.
        /// </summary>
        /// <value>One of 'verbose', 'debug', 'information', 'warning', 'error', 'fatal'</value>
        public string? Level { get; set; }
    }

    /// <summary>
    /// Configuration for console logging.
    /// </summary>
    public class ConsoleConfig : LogConfig
    {
        /// <summary>
        /// Minimum logging level to output to standard error.
        /// </summary>
        /// <value>One of 'verbose', 'debug', 'information', 'warning', 'error', 'fatal'</value>
        public string? StderrLevel { get; set; }
    }

    /// <summary>
    /// Logging to file configuration object
    /// </summary>
    public class FileConfig : LogConfig
    {
        /// <summary>
        /// Path to the location where the logs will be saved.
        /// Example: <c>'logs/log.txt'</c> will create log files with <c>log</c> prefix followed by a date as suffix, 
        /// and <c>txt</c> as extension in the <c>logs</c> folder.
        /// </summary>
        /// <value>Path to file</value>
        public string? Path { get; set; }

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
    /// Trace listener configuration object
    /// </summary>
    public class TraceListenerConfig : LogConfig
    {

    }

}
