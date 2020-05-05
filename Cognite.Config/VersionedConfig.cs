namespace Cognite.Extractor.Configuration 
{
    
    /// <summary>
    /// Base configuration object for supporting versioned configuration.
    /// The config should have a version property, so that versioning and compatibility can be tracked.
    /// </summary>
    public class VersionedConfig 
    {
        
        /// <summary>
        /// Current version of this config object
        /// </summary>
        public int Version { get; set; }

    }

}
