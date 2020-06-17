using System;

namespace Cognite.Extractor.StateStorage
{
    /// <summary>
    /// Represents an historical object in the destination system
    /// </summary>
    public class BaseExtractionStatePoco : BaseStorableState
    {
        /// <summary>
        /// Earliest known extracted timestamp
        /// </summary>
        [StateStoreProperty("first")]
        public DateTime FirstTimestamp { get; set; }

        /// <summary>
        /// Last known extracted timestamp
        /// </summary>
        [StateStoreProperty("last")]
        public DateTime LastTimestamp { get; set; }
    }
}
