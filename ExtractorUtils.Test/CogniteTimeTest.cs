using System;
using Xunit;
using Cognite.Extractor.Utils;

namespace ExtractorUtils.Test
{
    public static class CogniteTimeTest
    {

        [Theory]
        [InlineData(1573627744000, "2019-11-13 06:49:04.000")]
        public static void TestToFormattedString(
            long ms, string expected)
        {
            var time = CogniteTime.FromUnixTimeMilliseconds(ms);
            Assert.Equal(expected, time.ToISOString());
        }

        [Fact]
        public static void TestEpochToString()
        {
            Assert.Equal("1970-01-01 00:00:00.000", CogniteTime.DateTimeEpoch.ToISOString());
        }

        [Fact]
        public static void TestMaxValueToString()
        {
            Assert.Equal("9999-12-31 23:59:59.999", DateTime.MaxValue.ToISOString());
        }

        [Fact]
        public static void TestMax()
        {
            var t1 = CogniteTime.FromUnixTimeMilliseconds(1000);
            var t2 = CogniteTime.FromUnixTimeMilliseconds(2000);
            Assert.Equal(t2, CogniteTime.Max(t1, t2));
        }

        [Fact]
        public static void TestMin()
        {
            var t1 = CogniteTime.FromUnixTimeMilliseconds(1000);
            var t2 = CogniteTime.FromUnixTimeMilliseconds(2000);
            Assert.Equal(t1, CogniteTime.Min(t1, t2));
        }
        [Fact]
        public static void TestMaxValue()
        {
            var msMax = DateTime.MaxValue.ToUnixTimeMilliseconds();
            var dMax = CogniteTime.FromUnixTimeMilliseconds(msMax);
            Assert.Equal("9999-12-31 23:59:59.999", dMax.ToISOString());
            Assert.Equal(DateTime.MaxValue.Ticks / 10_000, dMax.Ticks / 10_000);
        }

        [Fact]
        public static void TestFromMilliseconds()
        {
            var d1 = CogniteTime.FromUnixTimeMilliseconds(0);
            Assert.Equal(CogniteTime.DateTimeEpoch, d1);

            Assert.Throws<ArgumentOutOfRangeException>(() => CogniteTime.FromUnixTimeMilliseconds(-1));

            var outOfRangeValue = DateTime.MaxValue.ToUnixTimeMilliseconds() + 1;
            Assert.Throws<ArgumentOutOfRangeException>(() => CogniteTime.FromUnixTimeMilliseconds(outOfRangeValue));
        }

        [Fact]
        public static void TestNanosecondsSinceEpoch()
        {
            // Maximum date that can be stored as (long) nanoseconds since epoch.
            var valid = DateTime.Parse("2262-04-11T23:47:16.8547758Z").ToUniversalTime();
            var invalid1 = valid.AddTicks(1);
            var invalid2 = new DateTime(2262, 04, 11, 23, 47, 16, 855, DateTimeKind.Utc);
            var ns = CogniteTime.NanosecondsSinceEpoch(valid);
            Assert.Equal(CogniteTime.FromTicks(ns / 100), valid);
            Assert.Throws<ArgumentException>(() => CogniteTime.NanosecondsSinceEpoch(invalid1));
            Assert.Throws<ArgumentException>(() => CogniteTime.NanosecondsSinceEpoch(invalid2));
        }

        [Fact]
        public static void TestTimeRange()
        {
            Assert.Equal(new TimeRange(DateTime.MaxValue, CogniteTime.DateTimeEpoch), TimeRange.Empty);
            Assert.True(new TimeRange(DateTime.MaxValue, CogniteTime.DateTimeEpoch) == TimeRange.Empty);
        }

    }
}