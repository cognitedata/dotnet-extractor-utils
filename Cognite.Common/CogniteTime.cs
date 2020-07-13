using System;

namespace Cognite.Extractor.Common
{
    /// <summary>
    /// DateTime utility functions.
    /// </summary>
    public static class CogniteTime
    {
        /// <summary>
        /// DateTime object representing the Unix Epoch, midnight 1/1/1970, in UTC.
        /// </summary>
        public static DateTime DateTimeEpoch => new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        
        private static readonly long epochTicks = DateTimeEpoch.Ticks;
        private static readonly long maxTsValue = DateTime.MaxValue.ToUnixTimeMilliseconds();
        private static readonly long maxTicksValue = DateTime.MaxValue.TicksSinceEpoch();

        /// <summary>
        /// Creates an UTC DateTime object at <paramref name="msSinceEpoch"/> milliseconds after the Unix Epoch, midnight 1/1/1970.
        /// </summary>
        /// <param name="msSinceEpoch">Number of milliseconds since Epoch</param>
        /// <returns>UTC DateTime object at <paramref name="msSinceEpoch"/> milliseconds after midnight 1/1/1970</returns>
        public static DateTime FromUnixTimeMilliseconds(long msSinceEpoch)
        {
            if (msSinceEpoch < 0 || msSinceEpoch > maxTsValue)
            {
                throw new ArgumentOutOfRangeException(nameof(msSinceEpoch), $"Timestamp value should be between {0} and {maxTsValue} ms");
            }
            return DateTimeEpoch.AddMilliseconds(msSinceEpoch);
        }

        /// <summary>
        /// Creates an UTC DateTime object at <paramref name="ticksSinceEpoch"/> ticks after the Unix Epoch, midninght 1/1/1970.
        /// </summary>
        /// <param name="ticksSinceEpoch">Number of ticks since Epoch</param>
        /// <returns>UTC DateTime object at <paramref name="ticksSinceEpoch"/> ticks after epoch</returns>
        public static DateTime FromTicks(long ticksSinceEpoch)
        {
            if (ticksSinceEpoch < 0 || ticksSinceEpoch > maxTicksValue)
            {
                throw new ArgumentOutOfRangeException(nameof(ticksSinceEpoch), $"Timestamp value should be between {0} and {maxTsValue} ticks");
            }
            return DateTimeEpoch.AddTicks(ticksSinceEpoch);
        }

        /// <summary>
        /// Returns the how many milliseconds have passed since the Unix Epoch, 1/1/1970 to the date passed as parameter.
        /// NOTE: Using TimeSpan.ToMilliseconds may cause rounding problems when 'time' is DateTime.MaxValue:
        /// The resulting(time - DateTimeEpoch).ToMilliseconds is 253402300800000, which cannot be
        /// converted back to DateTime(ArgumentOutOfRangeException). This method returns 253402300799999 instead.
        /// </summary>
        /// <param name="time">DateTime object to convert</param>
        /// <returns>Number of milliseconds since epoch</returns>
        public static long ToUnixTimeMilliseconds(this DateTime time)
        {
            var timestamp = TicksSinceEpoch(time) / TimeSpan.TicksPerMillisecond;
            return timestamp;
        }

        /// <summary>
        /// Returns the how many ticks have passed since the Unix Epoch, 1/1/1970 to the
        /// date passed as parameter. A Tick corresponds to 10 000 ms (ref. TimeSpan.TicksPerMillisecond).
        /// </summary>
        /// <param name="time">UTC DateTime object to convert</param>
        /// <returns>Number of ticks since epoch</returns>
        public static long TicksSinceEpoch(this DateTime time)
        {
            if (time < DateTimeEpoch)
            {
                throw new ArgumentException($"Date {time.ToISOString()} is before Unix Epoch.");
            }
            if (time.Kind == DateTimeKind.Local)
            {
                throw new ArgumentException("DateTime object should be represented using UTC");
            }
            return time.Ticks - epochTicks;
        }

        /// <summary>
        /// Returns the how many nanoseconds have passed since the Unix Epoch, 1/1/1970 to the
        /// date passed as parameter. A Nanosecond corresponds to 100 ticks.
        /// </summary>
        /// <param name="time">UTC DateTime object to convert</param>
        /// <returns>Number of ticks since epoch</returns>
        public static long NanosecondsSinceEpoch(this DateTime time)
        {
            var timestamp = TicksSinceEpoch(time);
            if (timestamp > long.MaxValue / 100)
            {
                throw new ArgumentException("Maxim value for nanosecond timestamp exceeded");
            }
            return timestamp * 100;
        }

        /// <summary>
        /// Return ISO 8601 formatted string (yyyy-MM-dd HH:mm:ss.fff) with millisecond resolution.
        /// </summary>
        /// <param name="time"></param>
        /// <returns>ISO 8601 formatted string</returns>
        public static string ToISOString(this DateTime time)
        {
            return time.ToString("yyyy-MM-dd HH\\:mm\\:ss.fff");
        }

        /// <summary>
        /// Return the latest of the two given DateTimes
        /// </summary>
        public static DateTime Max(DateTime t1, DateTime t2)
        {
            return t1 > t2 ? t1 : t2;
        }

        /// <summary>
        /// Return the earliest of the two given DateTimes.
        /// </summary>
        public static DateTime Min(DateTime t1, DateTime t2)
        {
            return t1 < t2 ? t1 : t2;
        }

    }
}
