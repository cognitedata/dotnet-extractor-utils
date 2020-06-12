using System;

namespace Cognite.Extractor.Utils
{
    /// <summary>
    /// Holds the timestamps of the first and last data points in CDF
    /// for a particular time series. The range is valid only if it is not Empty.
    /// The interval [First, Last] includes the First and Last timestamps.
    /// </summary>
    public sealed class TimeRange
    {
        // Create an empty range where first > last, and both are at the extreme range.
        // Extending an empty range will return an empty range.
        public static readonly TimeRange Empty = new TimeRange(DateTime.MaxValue, CogniteTime.DateTimeEpoch);
        
        public TimeRange(DateTime first, DateTime last)
        {
            First = first;
            Last = last;
        }

        public DateTime First { get; private set; }

        public DateTime Last { get; private set; }

        public bool IsEmpty
        {
            get
            {
                return First > Last;
            }
        }

        public bool Contains(DateTime t)
        {
            return !IsEmpty && t >= First && t <= Last;
        }

        public bool Before(DateTime t)
        {
            return t < First;
        }

        public bool After(DateTime t)
        {
            return t > Last;
        }

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
        public TimeRange Extend(TimeRange newRange)
        {
            return Extend(newRange.First, newRange.Last);
        }

        public override string ToString()
        {
            return $"({First.ToISOString()}, {Last.ToISOString()})";
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(obj, this)) return true;
            if (obj == null) return false;
            return obj is TimeRange && this == (TimeRange) obj;
        }

        public static bool operator ==(TimeRange x, TimeRange y)
        {
            return x.First == y.First && x.Last == y.Last;
        }

        public static bool operator !=(TimeRange x, TimeRange y)
        {
            return !(x == y);
        }

    }
}
