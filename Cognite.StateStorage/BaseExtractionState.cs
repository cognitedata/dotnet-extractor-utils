using Cognite.Extractor.Common;
using System;
using System.Collections.Generic;
using System.Text;

namespace Cognite.Extractor.StateStorage
{
    /// <summary>
    /// A minimal extraction state implementation.
    /// Represents a single object in a source system that maps to a single
    /// object in some destination system.
    /// 
    /// Keeps track of a <see cref="TimeRange"/> DestinationExtractedRange which represents
    /// the range of timestamps that are currently present in the destination system.
    /// </summary>
    public class BaseExtractionState : IExtractionState
    {
        private object _mutex = new object();
        /// <summary>
        /// Mutex used for safely modifying ranges
        /// </summary>
        protected object Mutex { 
            get { return _mutex; }
            private set { _mutex = value; }
        }

        /// <summary>
        /// Unique id for extracted object. Used as unique ID when storing in permanent storage,
        /// so it must be unique within each store.
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
        /// Sets the DestinationExtractedRange to (first, last) and sets LastTimeModified to null.
        /// </summary>
        /// <param name="first">First point in destination system</param>
        /// <param name="last">Last point in destination system</param>
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
