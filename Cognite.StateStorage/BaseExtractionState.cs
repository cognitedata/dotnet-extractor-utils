using Cognite.Extractor.Common;
using System;
using System.Collections.Generic;
using System.Text;

namespace Cognite.Extractor.StateStorage
{
    /// <summary>
    /// Represents a minimal extraction state implementation
    /// </summary>
    public class BaseExtractionState : IExtractionState
    {
        /// <summary>
        /// Mutex used for safely modifying ranges
        /// </summary>
        protected readonly object _mutex = new object();
        /// <summary>
        /// Unique id for extracted object
        /// </summary>
        public string Id { get; }
        /// <summary>
        /// Range of data pushed to destination(s)
        /// </summary>
        public TimeRange DestinationExtractedRange
        {
            get => _destinationExtractedRange;
            protected set
            {
                if (value == _destinationExtractedRange) return;
                LastTimeModified = DateTime.UtcNow;
                _destinationExtractedRange = value;
            }
        }
        private TimeRange _destinationExtractedRange;
        /// <summary>
        /// Last time the destination range was modified.
        /// </summary>
        public DateTime? LastTimeModified { get; protected set; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="id">Id of state</param>
        public BaseExtractionState(string id)
        {
            Id = id;
            DestinationExtractedRange = TimeRange.Empty;
        }

        /// <summary>
        /// Called when initializing extracted range from destinations and state storage.
        /// </summary>
        /// <param name="first"></param>
        /// <param name="last"></param>
        public virtual void InitExtractedRange(DateTime first, DateTime last)
        {
            lock (_mutex)
            {
                DestinationExtractedRange = new TimeRange(first, last);
                LastTimeModified = null;
            }
        }

        /// <summary>
        /// Update the state with first and last points successfully pushed to destination(s).
        /// </summary>
        /// <param name="first">Earliest timestamp in successful push to destination(s)</param>
        /// <param name="last">Latest timestamp in successful push to destination(s)</param>
        public virtual void UpdateDestinationRange(DateTime first, DateTime last)
        {
            lock (_mutex)
            {
                DestinationExtractedRange = DestinationExtractedRange.Extend(first, last);
            }
        }
    }
}
