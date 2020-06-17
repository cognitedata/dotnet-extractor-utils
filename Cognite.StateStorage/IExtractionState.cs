using System;
using System.Collections.Generic;
using System.Text;

namespace Cognite.Extractor.StateStorage
{
    /// <summary>
    /// Minimum required properties for an extraction state
    /// </summary>
    public interface IExtractionState
    {
        /// <summary>
        /// Unique Id
        /// </summary>
        string Id { get; }
        /// <summary>
        /// Last time this was modified in a way that requires storage.
        /// Update this value to now if you want it to be persisted to the state-storage.
        /// </summary>
        DateTime? LastTimeModified { get; }
    }
}
