using System;

namespace Cognite.Extractor.Common
{
    /// <summary>
    /// Represents a range of time from First to Last. First and Last are both considered
    /// to be included in the range.
    /// The legal range of values is from the unix epoch to DateTime.MaxValue.
    /// </summary>
    public sealed class TimeRange
    {
        /// <summary>
        /// An empty range where first > last, and both are at the extreme range.
        /// Extending an empty range will return the given range
        /// Contracting an empty range will return the empty range
        /// </summary>
        public static readonly TimeRange Empty = new TimeRange(DateTime.MaxValue, CogniteTime.DateTimeEpoch);
        /// <summary>
        /// The full range of legal values. 
        /// </summary>
        public static readonly TimeRange Complete = new TimeRange(CogniteTime.DateTimeEpoch, DateTime.MaxValue);

        /// <summary>
        /// Initialize a TimeRange object
        /// </summary>
        /// <param name="first">First point in the time range</param>
        /// <param name="last">Last point in the time range</param>
        public TimeRange(DateTime first, DateTime last)
        {
            if (first < CogniteTime.DateTimeEpoch)
                first = CogniteTime.DateTimeEpoch;
            First = first;
            Last = last;
        }
        /// <summary>
        /// First point in the range
        /// </summary>
        public DateTime First { get; }
        /// <summary>
        /// Last point in the range
        /// </summary>
        public DateTime Last { get; }

        /// <summary>
        /// True if there are no points in the range at all (first > last).
        /// </summary>
        public bool IsEmpty
        {
            get
            {
                return First > Last;
            }
        }
        /// <summary>
        /// True if given datetime is inside the range
        /// </summary>
        /// <param name="t">DateTime to test</param>
        /// <returns></returns>
        public bool Contains(DateTime t)
        {
            return t >= First && t <= Last;
        }
        /// <summary>
        /// True if given datetime is ahead of the range.
        /// </summary>
        /// <param name="t">Datetime to test</param>
        /// <returns></returns>
        public bool Before(DateTime t)
        {
            return t < First;
        }
        /// <summary>
        /// True if given datetime is after the range
        /// </summary>
        /// <param name="t">Datetime to test</param>
        /// <returns></returns>
        public bool After(DateTime t)
        {
            return t > Last;
        }
        /// <summary>
        /// Return a new TimeRange extended by the given timestamps.
        /// New TimeRange is the earliest start point and the latest end point.
        /// </summary>
        /// <param name="first">First point in extending range</param>
        /// <param name="last">Last point in extending range</param>
        /// <returns></returns>
        public TimeRange Extend(DateTime first, DateTime last)
        {
            if (first >= First)
                first = First;
            if (last <= Last)
                last = Last;
            if (first != First || last != Last)
                return new TimeRange(first, last);
            return this;
        }
        /// <summary>
        /// Returns a new TimeRange extended by the given TimeRange
        /// New TimeRange is the earliest start point and the latest end point.
        /// </summary>
        /// <param name="newRange">Extending range</param>
        /// <returns></returns>
        public TimeRange Extend(TimeRange newRange)
        {
            return Extend(newRange.First, newRange.Last);
        }
        /// <summary>
        /// Return a new TimeRange contracted by the given timestamps.
        /// New TimeRange is the latest start point and earliest end point.
        /// </summary>
        /// <param name="first">First point in contracting range</param>
        /// <param name="last">Last point in contracting range</param>
        /// <returns></returns>
        public TimeRange Contract(DateTime first, DateTime last)
        {
            if (first <= First)
                first = First;
            if (last >= Last)
                last = Last;
            if (first != First || last != Last)
                return new TimeRange(first, last);
            return this;
        }
        /// <summary>
        /// Returns a new TimeRange contracted by the given TimeRange
        /// New TimeRange is the latest start point and the earliest end point.
        /// </summary>
        /// <param name="newRange">Extending range</param>
        /// <returns></returns>
        public TimeRange Contract(TimeRange newRange)
        {
            return Contract(newRange.First, newRange.Last);
        }
        /// <summary>
        /// Returns a string representation of the TimeRange.
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return $"({First.ToISOString()}, {Last.ToISOString()})";
        }
        /// <summary>
        /// Compares this time range with the provided object and returns
        /// true if they are equal
        /// </summary>
        /// <param name="obj">Object to compare</param>
        /// <returns>true, if equal. false otherwise</returns>
        public override bool Equals(object obj)
        {
            if (ReferenceEquals(obj, this)) return true;
            if (obj == null) return false;
            return obj is TimeRange && this == (TimeRange)obj;
        }
        /// <summary>
        /// Returns the computed hash code for this time range using its (First, Last) tuple
        /// </summary>
        /// <returns></returns>
        public override int GetHashCode()
        {
            var tuple = (First, Last);
            return tuple.GetHashCode();
        }

        /// <summary>
        /// Returns true if the provided time ranges are equal
        /// </summary>
        /// <param name="x">time range</param>
        /// <param name="y"> time range</param>
        /// <returns>true, if equal. false otherwise</returns>
        public static bool operator ==(TimeRange x, TimeRange y)
        {
            if (x is null)
            {
                return y is null;
            }
            if (y is null) return false;
            return x.First == y.First && x.Last == y.Last;
        }

        /// <summary>
        /// Returns true if the provided time ranges are different
        /// </summary>
        /// <param name="x">time range</param>
        /// <param name="y"> time range</param>
        /// <returns>true, if different. false otherwise</returns>
        public static bool operator !=(TimeRange x, TimeRange y)
        {
            return !(x == y);
        }
    }
}
