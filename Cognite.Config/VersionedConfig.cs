namespace Cognite.Extractor.Configuration 
{
    
    /// <summary>
    /// Base configuration object for supporting versioned configuration.
    /// The config should have a version property, so that versioning and compatibility can be tracked.
    /// </summary>
    public abstract class VersionedConfig 
    {
        
        /// <summary>
        /// Current version of this config object
        /// </summary>
        public int Version { get; set; }

        /// <summary>
        /// Should be implemented in Base classes to initialize properties with default
        /// values, in case the values were not present in the parsed yaml config
        /// </summary>
        public abstract void GenerateDefaults();

    }

}
