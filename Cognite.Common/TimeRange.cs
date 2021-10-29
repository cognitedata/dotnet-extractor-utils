using System;

namespace Cognite.Extractor.Common
{
    /// <summary>
    /// Represents a range of time from First to Last. First and Last are both considered
    /// to be included in the range.
    /// The legal range of values is from the unix epoch, midnight 1/1/1970, to DateTime.MaxValue.
    /// </summary>
    public sealed class TimeRange
    {
        /// <summary>
        /// An empty range where first > last, and both are at the extreme range, (DateTime.MaxValue, Epoch).
        /// Extending an empty range will return the given range
        /// Contracting an empty range will return the empty range
        /// </summary>
        public static readonly TimeRange Empty = new TimeRange(DateTime.MaxValue, CogniteTime.DateTimeEpoch);

        /// <summary>
        /// The largest legal range, (Epoch, DateTime.MaxValue).
        /// Extending a complete range will return the complete range.
        /// Contracting a complete range will return the given range.
        /// </summary>
        public static readonly TimeRange Complete = new TimeRange(CogniteTime.DateTimeEpoch, DateTime.MaxValue);

        /// <summary>
        /// Initialize a TimeRange object from two timestamps.
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
        public bool IsEmpty => First > Last;

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
        /// Check if <paramref name="t"/> is before the first point in the range.
        /// </summary>
        /// <param name="t">Datetime to test</param>
        /// <returns>True if <paramref name="t"/> is before the first point in the range</returns>
        public bool Before(DateTime t)
        {
            return t < First;
        }
        /// <summary>
        /// Check if <paramref name="t"/> is after the last point in the range.
        /// </summary>
        /// <param name="t">Datetime to test</param>
        /// <returns>True if <paramref name="t"/> is after the last point in the range</returns>
        public bool After(DateTime t)
        {
            return t > Last;
        }

        /// <summary>
        /// Return a new TimeRange extended by the given timestamps.
        /// New TimeRange is the earliest start point and the latest end point.
        /// Letting first or last be null will keep the existing value for that end of the range.
        /// </summary>
        /// <param name="first">First point in extending range</param>
        /// <param name="last">Last point in extending range</param>
        /// <returns>A new TimeRange extended by <paramref name="first"/> and <paramref name="last"/></returns>
        public TimeRange Extend(DateTime? first, DateTime? last)
        {
            if (!first.HasValue || first >= First)
                first = First;
            if (!last.HasValue || last <= Last)
                last = Last;
            if (first != First || last != Last)
                return new TimeRange(first.Value, last.Value);
            return this;
        }

        /// <summary>
        /// Returns a new TimeRange extended by the given TimeRange
        /// New TimeRange is the earliest start point and the latest end point.
        /// </summary>
        /// <param name="newRange">Extending range</param>
        /// <returns>A new TimeRange extended by <paramref name="newRange"/></returns>
        public TimeRange Extend(TimeRange newRange)
        {
            if (newRange is null)
            {
                throw new ArgumentNullException(nameof(newRange));
            }
            return Extend(newRange.First, newRange.Last);
        }

        /// <summary>
        /// Return a new TimeRange contracted by the given timestamps.
        /// New TimeRange is the latest start point and earliest end point.
        /// Letting first or last be null will keep the existing value for that end of the range.
        /// </summary>
        /// <param name="first">First point in contracting range</param>
        /// <param name="last">Last point in contracting range</param>
        /// <returns>A new TimeRange contracted by <paramref name="first"/> and <paramref name="last"/></returns>
        public TimeRange Contract(DateTime? first, DateTime? last)
        {
            if (!first.HasValue || first <= First)
                first = First;
            if (!last.HasValue || last >= Last)
                last = Last;
            if (first != First || last != Last)
                return new TimeRange(first.Value, last.Value);
            return this;
        }

        /// <summary>
        /// Returns a new TimeRange contracted by the given TimeRange
        /// New TimeRange is the latest start point and the earliest end point.
        /// </summary>
        /// <param name="newRange">Extending range</param>
        /// <returns>A new TimeRange contracted by <paramref name="newRange"/></returns>
        public TimeRange Contract(TimeRange newRange)
        {
            if (newRange is null)
            {
                throw new ArgumentNullException(nameof(newRange));
            }
            return Contract(newRange.First, newRange.Last);
        }

        /// <summary>
        /// Returns a string representation of the TimeRange, on the form ([First as ISO-string], [Last as ISO-string]).
        /// </summary>
        /// <returns>A string representation of the TimeRange</returns>
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
