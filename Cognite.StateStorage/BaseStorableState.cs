using LiteDB;

namespace Cognite.Extractor.StateStorage
{
    /// <summary>
    /// Minimal state that may be stored to a state-storage system.
    /// </summary>
    public class BaseStorableState
    {
        /// <summary>
        /// Unique identifier for the state in the destination storage
        /// </summary>
        [BsonId]
        [StateStoreProperty("id")]
        public string Id { get; set; }
    }
}
