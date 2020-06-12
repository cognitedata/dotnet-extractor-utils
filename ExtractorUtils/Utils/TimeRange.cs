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
        /// <summary>
        /// Create an empty range where first > last, and both are at the extreme range.
        /// Extending an empty range will return an empty range.
        /// </summary>
        public static readonly TimeRange Empty = new TimeRange(DateTime.MaxValue, CogniteTime.DateTimeEpoch);
        
        /// <summary>
        /// Creates a new <see cref="TimeRange"/> object with the <paramref name="first"/> and <paramref name="last"/>
        /// timestamps passed as parameters.
        /// </summary>
        /// <param name="first">First timestamp</param>
        /// <param name="last">Last timestamp</param>
        public TimeRange(DateTime first, DateTime last)
        {
            First = first;
            Last = last;
        }

        /// <summary>
        /// Last timestamp in this range
        /// </summary>
        public DateTime First { get; private set; }

        /// <summary>
        /// Last timestamp in this range
        /// </summary>
        public DateTime Last { get; private set; }

        /// <summary>
        /// True if this range is empty. That is, the first timestamp is greater than the last
        /// </summary>
        public bool IsEmpty
        {
            get
            {
                return First > Last;
            }
        }

        /// <summary>
        /// The DateTime <paramref name="t"/> is within the first and the last
        /// timestamps of this range
        /// </summary>
        /// <param name="t"><see cref="DateTime"/> timestamp</param>
        /// <returns>true, if <paramref name="t"/> is contained in this range</returns>
        public bool Contains(DateTime t)
        {
            return !IsEmpty && t >= First && t <= Last;
        }

        /// <summary>
        /// The DateTime <paramref name="t"/> is before the first
        /// timestamp of this range
        /// </summary>
        /// <param name="t"><see cref="DateTime"/> timestamp</param>
        /// <returns>true, if <paramref name="t"/> is before this range</returns>
        public bool Before(DateTime t)
        {
            return t < First;
        }

        /// <summary>
        /// The DateTime <paramref name="t"/> is after the last
        /// timestamp of this range
        /// </summary>
        /// <param name="t"><see cref="DateTime"/> timestamp</param>
        /// <returns>true, if <paramref name="t"/> is after this range</returns>
        public bool After(DateTime t)
        {
            return t > Last;
        }

        /// <summary>
        /// Creates a new <see cref="TimeRange"/> object extending this
        /// time range with the first and last <see cref="DateTime"/> objects passed as parameter
        /// </summary>
        /// <param name="first">First timestamp</param>
        /// <param name="last">Last timestamp</param>
        /// <returns>new extended time range</returns>
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
        /// Creates a new <see cref="TimeRange"/> object extending this
        /// time range with the range passed as parameter
        /// </summary>
        /// <param name="newRange">target range</param>
        /// <returns>new extended time range</returns>
        public TimeRange Extend(TimeRange newRange)
        {
            return Extend(newRange.First, newRange.Last);
        }

        /// <summary>
        /// Return a string containing the first and last <see cref="DateTime"/> objects
        /// formated according to ISO 8601
        /// </summary>
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
            return obj is TimeRange && this == (TimeRange) obj;
        }

        /// <summary>
        /// Returns true if the provided time ranges are equal
        /// </summary>
        /// <param name="x">time range</param>
        /// <param name="y"> time range</param>
        /// <returns>true, if equal. false otherwise</returns>
        public static bool operator ==(TimeRange x, TimeRange y)
        {
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
