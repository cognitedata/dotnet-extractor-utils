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
        /// Logging configuration
        /// </summary>
        /// <value></value>
        public LoggerConfig Logger { get; set; }
    }

#region Logging configuration
    public class LoggerConfig
    {
        public ConsoleConfig Console { get; set; }
        public FileConfig File { get; set; }
        public StackdriverConfig Stackdriver { get; set; }
    }

    public class ConsoleConfig
    {
        public string Level { get; set; }
    }

    public class FileConfig
    {
        public string Level { get; set; }
        public string Path { get; set; }
        public int RetentionLimit { get; set; } = 31;
    }

    public class StackdriverConfig
    {
        public string Credentials { get; set; }
        public string LogName { get; set; }
    }
#endregion
}
