using LiteDB;

namespace Cognite.Extractor.StateStorage
{
    /// <summary>
    /// Minimal state that may be stored to a state-storage system.
    /// Only contains an Id. This should be extended to create custom storable objects.
    /// </summary>
    public class BaseStorableState
    {
        /// <summary>
        /// Unique identifier for the state in the destination storage
        /// </summary>
        [BsonId]
        public string Id { get; set; }
    }
}
