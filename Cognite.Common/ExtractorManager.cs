using System;
using System.Threading.Tasks;
using System.Collections.Generic;

namespace Cognite.Extractor.Common
{
    /// <summary>
    /// Interface for an ExtractorManager
    /// </summary>
    public interface IExtractorManager
    {
        /// <summary>
        /// Method used to add high availability to an extractor.
        /// </summary>
        /// <returns></returns>
        public Task WaitToBecomeActive();
    }

    /// <summary>
    /// Interface for the instance of an extractor
    /// </summary>
    public interface IExtractorInstance
    {
        /// <summary>
        /// The index of an extractor.
        /// </summary>
        public int Index { get; set; }

        /// <summary>
        /// The time when the extractor was last updated
        /// </summary>
        public DateTime TimeStamp { get; set; }

        /// <summary>
        /// Whether the extractor is marked as being active
        /// </summary>
        public bool Active { get; set; }
    }

    /// <summary>
    /// Class used to store the state of the extractors
    /// </summary>
    public class ExtractorState
    {
        /// <summary>
        /// State of all the current extractors
        /// </summary>
        public List<IExtractorInstance> CurrentState { get; set; }

        /// <summary>
        /// The last status of the given extractor
        /// </summary>
        public bool UpdatedStatus { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="initialStatus">The initial active status of the extractor</param>
        public ExtractorState(bool initialStatus = false)
        {
            CurrentState = new List<IExtractorInstance>();
            UpdatedStatus = initialStatus;
        }
    }
}