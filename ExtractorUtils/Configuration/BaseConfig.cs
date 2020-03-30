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

    }
}
