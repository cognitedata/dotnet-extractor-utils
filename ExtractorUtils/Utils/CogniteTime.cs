using System;

namespace ExtractorUtils
{
    /// <summary>
    /// DateTime utility functions.
    /// </summary>
    public static class CogniteTime
    {
        /// <summary>
        /// DateTime object representing the Unix Epoch.
        /// </summary>
        public static DateTime DateTimeEpoch => new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        
        private static readonly long epochTicks = DateTimeEpoch.Ticks;
        private static readonly long maxTsValue = DateTime.MaxValue.ToUnixTimeMilliseconds();
        private static readonly long maxTicksValue = DateTime.MaxValue.TicksSinceEpoch();

        /// <summary>
        /// Constructs a DateTime object adding the number of milliseconds passed as parameter to
        /// Unix Epoch.
        /// </summary>
        /// <param name="msSinceEpoch">number of milliseconds since Epoch</param>
        /// <returns>DateTime object correponding to the Unix time</returns>
        public static DateTime FromMilliseconds(long msSinceEpoch)
        {
            if (msSinceEpoch < 0 || msSinceEpoch > maxTsValue)
            {
                throw new ArgumentOutOfRangeException(nameof(msSinceEpoch), $"Timestamp value should be between {0} and {maxTsValue} ms");
            }
            return DateTimeEpoch.AddMilliseconds(msSinceEpoch);
        }

        /// <summary>
        /// Constructs a DateTime object adding the number of ticks passed as parameter to
        /// Unix Epoch.
        /// </summary>
        /// <param name="ticksSinceEpoch">number of ticks since Epoch</param>
        /// <returns>DateTime object correponding to the Unix time</returns>
        public static DateTime FromTicks(long ticksSinceEpoch)
        {
            if (ticksSinceEpoch < 0 || ticksSinceEpoch > maxTicksValue)
            {
                throw new ArgumentOutOfRangeException(nameof(ticksSinceEpoch), $"Timestamp value should be between {0} and {maxTsValue} ticks");
            }
            return DateTimeEpoch.AddTicks(ticksSinceEpoch);
        }

        /// <summary>
        /// Returns the how many milliseconds have passed since Unix Epoch to the
        /// date passed as parameter.
        /// NOTE: Using TimeSpan.ToMilliseconds may cause rounding problems when 'time' is DateTime.MaxValue:
        /// The resulting(time - DateTimeEpoch).ToMilliseconds is 253402300800000, which cannot be
        /// converted back to DateTime(ArgumentOutOfRangeException). This method returns 253402300799999 instead.
        /// </summary>
        /// <param name="time">DateTime object to convert</param>
        /// <returns>Number of milliseconds since Epoch</returns>
        public static long ToUnixTimeMilliseconds(this DateTime time)
        {
            var timestamp = TicksSinceEpoch(time) / TimeSpan.TicksPerMillisecond;
            return timestamp;
        }

        /// <summary>
        /// Returns the how many ticks have passed since Unix Epoch to the
        /// date passed as parameter. A Tick correspond to 10.000 ms (ref. TimeSpan.TicksPerMillisecond).
        /// </summary>
        /// <param name="time">DateTime object to convert</param>
        /// <returns>Number of ticks since Epoch</returns>
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
        /// Returns the how many nanoseconds have passed since Unix Epoch to the
        /// date passed as parameter. A Nanosecond correspond to 100 ticks.
        /// </summary>
        /// <param name="time">DateTime object to convert</param>
        /// <returns>Number of ticks since Epoch</returns>
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
        /// Return ISO 8601 formatted string with millisecond resolution.
        /// </summary>
        /// <param name="time"></param>
        /// <returns>ISO 8601 formatted string</returns>
        public static string ToISOString(this DateTime time)
        {
            return time.ToString("yyyy-MM-dd HH\\:mm\\:ss.fff");
        }

        /// <summary>
        /// Return latest DateTime.
        /// </summary>
        public static DateTime Max(DateTime t1, DateTime t2)
        {
            return t1 > t2 ? t1 : t2;
        }

        /// <summary>
        /// Return earliest DateTime.
        /// </summary>
        public static DateTime Min(DateTime t1, DateTime t2)
        {
            return t1 < t2 ? t1 : t2;
        }

    }
}
