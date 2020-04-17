using Xunit;
using System;

namespace ExtractorUtils.Test
{
    public static class CogniteTimeTest
    {

        [Theory]
        [InlineData(1573627744000, "2019-11-13 06:49:04.000")]
        public static void TestToFormattedString(
            long ms, string expected)
        {
            var time = CogniteTime.FromMilliseconds(ms);
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
            var t1 = CogniteTime.FromMilliseconds(1000);
            var t2 = CogniteTime.FromMilliseconds(2000);
            Assert.Equal(t2, CogniteTime.Max(t1, t2));
        }

        [Fact]
        public static void TestMin()
        {
            var t1 = CogniteTime.FromMilliseconds(1000);
            var t2 = CogniteTime.FromMilliseconds(2000);
            Assert.Equal(t1, CogniteTime.Min(t1, t2));
        }
        [Fact]
        public static void TestMaxValue()
        {
            var msMax = DateTime.MaxValue.ToUnixTimeMilliseconds();
            var dMax = CogniteTime.FromMilliseconds(msMax);
            Assert.Equal("9999-12-31 23:59:59.999", dMax.ToISOString());
            Assert.Equal(DateTime.MaxValue.Ticks / 10_000, dMax.Ticks / 10_000);
        }

        [Fact]
        public static void TestFromMilliseconds()
        {
            var d1 = CogniteTime.FromMilliseconds(0);
            Assert.Equal(CogniteTime.DateTimeEpoch, d1);

            Assert.Throws<ArgumentOutOfRangeException>(() => CogniteTime.FromMilliseconds(-1));

            var outOfRangeValue = DateTime.MaxValue.ToUnixTimeMilliseconds() + 1;
            Assert.Throws<ArgumentOutOfRangeException>(() => CogniteTime.FromMilliseconds(outOfRangeValue));
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

    }
}