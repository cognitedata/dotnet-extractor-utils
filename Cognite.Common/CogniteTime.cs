using System.Globalization;
using System;
using System.Text.RegularExpressions;
using System.Threading;

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
            return time.ToString("yyyy-MM-dd HH\\:mm\\:ss.fff", CultureInfo.InvariantCulture);
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

        /// <summary>
        /// Return the longest of the two given TimeSpans
        /// </summary>
        public static TimeSpan Max(TimeSpan t1, TimeSpan t2)
        {
            return t1 > t2 ? t1 : t2;
        }

        /// <summary>
        /// Return the shortest of the two given TimeSpans.
        /// </summary>
        public static TimeSpan Min(TimeSpan t1, TimeSpan t2)
        {
            return t1 < t2 ? t1 : t2;
        }

        private static TimeSpan? GetSpan(string type, long value)
        {
            switch (type)
            {
                case "w": return TimeSpan.FromDays(7 * value);
                case "d": return TimeSpan.FromDays(value);
                case "h": return TimeSpan.FromHours(value);
                case "m": return TimeSpan.FromMinutes(value);
                case "s": return TimeSpan.FromSeconds(value);
                case "ms": return TimeSpan.FromMilliseconds(value);
            }
            return null;
        }


        private static readonly Regex timestampStringRegex = new Regex("^([0-9]+)(ms?|w|d|h|s)-ago$");
        private static readonly Regex timespanStringRegex = new Regex("^([0-9]+)(ms?|w|d|h|s)$");

        /// <summary>
        /// Parse Cognite timestamp string.
        /// The format is N[timeunit]-ago where timeunit is w,d,h,m,s. Example: '2d-ago'
        /// returns a timestamp two days ago.
        /// If the format is N[timeunit], without -ago, it is set to the future, or after <paramref name="relative"/>
        /// Without timeunit, it is converted to a datetime in milliseconds since epoch.
        /// </summary>
        /// <param name="t">Timestamp string</param>
        /// <param name="relative">Set time relative to this if -ago syntax is used</param>
        /// <returns>DateTime or null if the input is invalid</returns>
        public static DateTime? ParseTimestampString(string? t, DateTime? relative = null)
        {
            if (t == null) return null;
            var now = relative ?? DateTime.UtcNow;
            var match = timestampStringRegex.Match(t);
            if (match.Success)
            {
                var rawUnit = match.Groups[2].Value;
                var rawValue = match.Groups[1].Value;

                long value = Convert.ToInt64(rawValue, CultureInfo.InvariantCulture);

                return now.Add(-GetSpan(rawUnit, value)!.Value);
            }
            else
            {
                var span = ParseTimeSpanString(t);
                if (span != null) return now.Add(span.Value);
            }

            try
            {
                return FromUnixTimeMilliseconds(Convert.ToInt64(t, CultureInfo.InvariantCulture));
            }
            catch
            {
                return null;
            }
        }

        /// <summary>
        /// Parse cognite timestamp into timespan.
        /// Format is N[timeunit].
        /// If timeunit is left out, then <paramref name="defaultUnit"/> can be specified
        /// and if t is a whole number it can be converted using that.
        /// </summary>
        /// <param name="t">Raw input</param>
        /// <param name="defaultUnit">Default unit to use if no unit is specified</param>
        /// <returns>TimeSpan or null if input is invalid</returns>
        public static TimeSpan? ParseTimeSpanString(string? t, string? defaultUnit = null)
        {
            if (t == null) return null;
            var match = timespanStringRegex.Match(t);
            if (match.Success)
            {
                var rawUnit = match.Groups[2].Value;
                var rawValue = match.Groups[1].Value;
                
                long value = Convert.ToInt64(rawValue, CultureInfo.InvariantCulture);

                return GetSpan(rawUnit, value);
            }

            if (defaultUnit != null)
            {
                try
                {
                    var value = Convert.ToInt64(t, CultureInfo.InvariantCulture);
                    return GetSpan(defaultUnit, value);
                }
                catch
                {
                    return null;
                }
            }

            return null;
        }
    }
}
