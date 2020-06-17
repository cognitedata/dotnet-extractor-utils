using Cognite.Extractor.Common;
using System;

namespace Cognite.Extractor.StateStorage
{
    /// <summary>
    /// Represents an object to be extracted from the source system.
    /// This object is intended to be used with a system that can both feed live data
    /// and read historical data.
    /// </summary>
    public class HistoryExtractionState : BaseExtractionState
    {
        /// <summary>
        /// Range of data extracted from source system
        /// </summary>
        public TimeRange SourceExtractedRange { get; protected set; }

        /// <summary>
        /// True if this state has backfill enabled.
        /// </summary>
        public bool BackfillEnabled { get; protected set; }
        /// <summary>
        /// True if this state has frontfill enabled.
        /// </summary>
        public bool FrontfillEnabled { get; protected set; }

        /// <summary>
        /// True if state is actively backfilling.
        /// </summary>
        public bool IsBackfilling { get; protected set; }

        /// <summary>
        /// True if state is actively frontfilling
        /// </summary>
        public bool IsFrontfilling { get; protected set; }

        /// <summary>
        /// Create an extraction state with given id.
        /// </summary>
        /// <param name="id">Unique id of extraction state</param>
        /// <param name="backfill">True if this state will be backfilled</param>
        /// <param name="frontfill">True if this state will be frontfilled</param>
        public HistoryExtractionState(string id, bool frontfill = false, bool backfill = false) : base(id)
        {
            BackfillEnabled = backfill;
            FrontfillEnabled = frontfill;
            SourceExtractedRange = TimeRange.Empty;
            DestinationExtractedRange = TimeRange.Complete;
            LastTimeModified = null;
            Initialized = false;
            IsBackfilling = backfill;
            IsFrontfilling = frontfill;
        }
        /// <summary>
        /// Called when initializing extracted range from destinations and state storage.
        /// This will always contract the believed range.
        /// </summary>
        /// <param name="first"></param>
        /// <param name="last"></param>
        public override void InitExtractedRange(DateTime first, DateTime last)
        {
            if (Initialized) throw new InvalidOperationException("Extracted state is already initialized");
            lock (_mutex)
            {
                DestinationExtractedRange = DestinationExtractedRange.Contract(first, last);
            }
        }
        /// <summary>
        /// Called after range initialization to set uninitialized ranges to proper default values depending on whether
        /// backfill is enabled or not.
        /// </summary>
        public virtual void FinalizeRangeInit()
        {
            if (Initialized) throw new InvalidOperationException("Extracted state is already initialized");
            lock (_mutex)
            {
                if (DestinationExtractedRange == TimeRange.Complete)
                {
                    if (!FrontfillEnabled || BackfillEnabled)
                    {
                        SourceExtractedRange = DestinationExtractedRange = new TimeRange(DateTime.UtcNow, DateTime.UtcNow);
                    }
                    else
                    {
                        SourceExtractedRange = DestinationExtractedRange = new TimeRange(CogniteTime.DateTimeEpoch, CogniteTime.DateTimeEpoch);
                    }
                }
                else
                {
                    SourceExtractedRange = DestinationExtractedRange;
                }
                if (DestinationExtractedRange.First == CogniteTime.DateTimeEpoch)
                {
                    IsBackfilling = false;
                }
                Initialized = true;
            }
        }
        /// <summary>
        /// Update start of source range from history backfill.
        /// </summary>
        /// <param name="first">Earliest timestamp in backfilled chunk</param>
        /// <param name="final">True if this is the end of history</param>
        public virtual void UpdateFromBackfill(DateTime first, bool final)
        {
            if (!Initialized) throw new InvalidOperationException("Extracted state must not be updated before it is initialized");
            lock (_mutex)
            {
                if (first < SourceExtractedRange.First)
                {
                    SourceExtractedRange = new TimeRange(first, SourceExtractedRange.Last);
                }

                IsBackfilling &= !final;
            }
        }

        /// <summary>
        /// Update end of source range from history frontfill.
        /// </summary>
        /// <param name="last">Last timestamp in frontfilled chunk</param>
        /// <param name="final">True if this is the end of history</param>
        public virtual void UpdateFromFrontfill(DateTime last, bool final)
        {
            if (!Initialized) throw new InvalidOperationException("Extracted state must not be updated before it is initialized");
            lock (_mutex)
            {
                if (last > SourceExtractedRange.Last)
                {
                    SourceExtractedRange = new TimeRange(SourceExtractedRange.First, last);
                }
                IsFrontfilling &= !final;
            }
        }

        /// <summary>
        /// Update end of source range from streamed data.
        /// Streamed data may come from anywhere. If backfill/frontfill is running,
        /// only points after/before the first/last timestamp are considered.
        /// </summary>
        /// <param name="first">First timestamp in streamed chunk</param>
        /// <param name="last">Last timestamp in streamed chunk</param>
        public virtual void UpdateFromStream(DateTime first, DateTime last)
        {
            if (!Initialized) throw new InvalidOperationException("Extracted state must not be updated before it is initialized");
            if (IsFrontfilling && IsBackfilling) return;
            lock (_mutex)
            {
                if (IsFrontfilling && last > SourceExtractedRange.Last)
                {
                    last = SourceExtractedRange.Last;
                }
                if (IsBackfilling && first < SourceExtractedRange.First)
                {
                    first = SourceExtractedRange.First;
                }
                if (first != SourceExtractedRange.First || last != SourceExtractedRange.Last)
                {
                    SourceExtractedRange = SourceExtractedRange.Extend(first, last);
                }
            }
        }

        /// <summary>
        /// Update the state with first and last points successfully pushed to destination(s).
        /// If backfill/frontfill is enabled, this will not extend the destination range outside of the
        /// first/last point in the source range.
        /// If backfill is enabled and done, and first is at or before the earliest extracted point,
        /// the destination range start will be set to zero.
        /// </summary>
        /// <param name="first">Earliest timestamp in successful push to destination(s)</param>
        /// <param name="last">Latest timestamp in successful push to destination(s)</param>
        public override void UpdateDestinationRange(DateTime first, DateTime last)
        {
            if (!Initialized) throw new InvalidOperationException("Extracted state must not be updated before it is initialized");
            lock (_mutex)
            {
                // If points are pushed outside of the source range, we must getting points from some other source.
                // To make sure that all source data is extracted, even if the extractor crashes immediately,
                // we avoid updating the destination range to outside of the source range.
                // This does not hold if frontfill is done, in which case any future data is good.
                if (IsFrontfilling && last > SourceExtractedRange.Last)
                {
                    last = SourceExtractedRange.Last;
                }

                // The same logic applies for backfill.
                if (IsBackfilling && first < SourceExtractedRange.First)
                {
                    first = SourceExtractedRange.First;
                }

                // If backfill is enabled and we reach the end of backfill history, we can update the extracted range to
                // start at zero, so that we skip backfill in the future.
                if (!IsBackfilling && BackfillEnabled && first <= SourceExtractedRange.First)
                {
                    first = CogniteTime.DateTimeEpoch;
                }
                DestinationExtractedRange = DestinationExtractedRange.Extend(first, last);
            }
        }

        /// <summary>
        /// Restart history by reseting the source range to the destination range,
        /// and setting IsStreaming and IsBackfilling to false.
        /// </summary>
        public virtual void RestartHistory()
        {
            lock (_mutex)
            {
                IsFrontfilling = FrontfillEnabled;
                IsBackfilling = BackfillEnabled && DestinationExtractedRange.First > CogniteTime.DateTimeEpoch;
                SourceExtractedRange = DestinationExtractedRange;
            }
        }
    }
}
