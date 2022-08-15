using System;
using System.Threading.Tasks;
using System.Collections.Generic;

namespace Cognite.Extractor.Common
{
    /// <summary>
    /// Interface for a manager used to add high availability.
    /// </summary>
    public interface IHighAvailabilityManager
    {
        /// <summary>
        /// Method used to add high availability to an extractor.
        /// </summary>
        /// <returns></returns>
        Task WaitToBecomeActive();
    }

    /// <summary>
    /// Interface for the instance of an extractor.
    /// </summary>
    public interface IExtractorInstance
    {
        /// <summary>
        /// The index of the extractor.
        /// </summary>
        int Index { get; set; }

        /// <summary>
        /// The time when the extractor was last updated.
        /// </summary>
        DateTime TimeStamp { get; set; }

        /// <summary>
        /// The active status of the extractor.
        /// </summary>
        bool Active { get; set; }
    }

    /// <summary>
    /// Class used to store the state of the extractors.
    /// </summary>
    public class ExtractorState
    {
        /// <summary>
        /// State of the current extractors.
        /// </summary>
        public List<IExtractorInstance> CurrentState { get; set; }

        /// <summary>
        /// Value used by the extractor to update its own active status.
        /// </summary>
        public bool UpdatedStatus { get; set; }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="initialStatus">The initial active status of the extractor.</param>
        public ExtractorState(bool initialStatus = false)
        {
            CurrentState = new List<IExtractorInstance>();
            UpdatedStatus = initialStatus;
        }
    }
}