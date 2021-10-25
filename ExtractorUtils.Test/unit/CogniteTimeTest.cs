using System;
using Xunit;
using Cognite.Extractor.Common;

namespace ExtractorUtils.Test.Unit
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
        public static void TestTimeRangeEquality()
        {
            Assert.Equal(new TimeRange(DateTime.MaxValue, CogniteTime.DateTimeEpoch), TimeRange.Empty);
            Assert.True(new TimeRange(DateTime.MaxValue, CogniteTime.DateTimeEpoch) == TimeRange.Empty);
            Assert.Equal((new TimeRange(DateTime.MaxValue, CogniteTime.DateTimeEpoch)).GetHashCode(), TimeRange.Empty.GetHashCode());

            TimeRange r1 = new TimeRange(new DateTime(2000, 01, 01), new DateTime(2010, 01, 01));
            TimeRange r2 = new TimeRange(new DateTime(2005, 01, 01), new DateTime(2010, 01, 01));

            Assert.NotEqual(r1, r2);
            Assert.True(r1 != r2);
            Assert.NotEqual(r1.GetHashCode(), r2.GetHashCode());

            r2 = r2.Extend(r1);
            Assert.Equal(r1, r2);
            Assert.Equal(r1.GetHashCode(), r2.GetHashCode());
        }

        [Fact]
        public static void TestTimeRangeContains()
        {
            TimeRange r1 = new TimeRange(new DateTime(2000, 01, 01), new DateTime(2010, 01, 01));
            DateTime d1 = new DateTime(1990, 01, 01);
            DateTime d2 = new DateTime(2005, 01, 01);
            DateTime d3 = new DateTime(2020, 01, 01);

            Assert.False(r1.Contains(d1));
            Assert.True(r1.Contains(d2));
            Assert.False(r1.Contains(d3));

            Assert.True(r1.Before(d1));
            Assert.False(r1.Before(d2));
            Assert.False(r1.Before(d3));

            Assert.False(r1.After(d1));
            Assert.False(r1.After(d2));
            Assert.True(r1.After(d3));
        }

        [Fact]
        public static void TestTimeRangeExtend()
        {
            TimeRange r1 = TimeRange.Empty;
            Assert.True(r1.IsEmpty);
            DateTime d1 = new DateTime(1990, 01, 01);
            DateTime d2 = new DateTime(2005, 01, 01);
            DateTime d3 = new DateTime(2010, 01, 01);
            DateTime d4 = new DateTime(2020, 01, 01);

            var r2 = r1.Extend(d2, d3);
            Assert.False(r2.IsEmpty);

            var r3 = new TimeRange(d3, d2);
            Assert.True(r3.IsEmpty);

            var r4 = r3.Extend(r2);
            Assert.Equal(r4, r2);

            var r5 = r4.Extend(d1, d4);
            Assert.Equal(new TimeRange(d1, d4), r5);
        }
        [Fact]
        public static void TestTimeRangeContract()
        {
            TimeRange r1 = TimeRange.Complete;
            Assert.Equal(CogniteTime.DateTimeEpoch, r1.First);
            Assert.Equal(DateTime.MaxValue, r1.Last);

            DateTime d1 = new DateTime(1990, 01, 01);
            DateTime d2 = new DateTime(2000, 01, 01);
            DateTime d3 = new DateTime(2010, 01, 01);
            DateTime d4 = new DateTime(2020, 01, 01);

            var r2 = r1.Contract(d1, d4);
            Assert.Equal(new TimeRange(d1, d4), r2);

            var r3 = r2.Contract(d3, d2);
            Assert.True(r3.IsEmpty);
            Assert.Equal(new TimeRange(d3, d2), r3);

            var r4 = r2.Contract(new TimeRange(d2, d3));
            Assert.Equal(new TimeRange(d2, d3), r4);
        }
        [Fact]
        public static void TestSmallExtendContract()
        {
            var r1 = new TimeRange(DateTime.UtcNow, DateTime.UtcNow);
            Assert.Equal(r1, r1);
            var r2 = r1.Extend(r1.First - TimeSpan.FromTicks(1), r1.Last + TimeSpan.FromTicks(1));
            Assert.NotEqual(r1, r2);
            var r3 = new TimeRange(r1.First - TimeSpan.FromTicks(1), r1.Last + TimeSpan.FromTicks(1));
            Assert.Equal(r3, r2);

            var r4 = r2.Contract(r1);
            Assert.Equal(r1, r4);
        }
        [Theory]
        [InlineData("2w-ago", false)]
        [InlineData("14d-ago", false)]
        [InlineData("336h-ago", false)]
        [InlineData("20160m-ago", false)]
        [InlineData("1209600s-ago", false)]
        [InlineData("bleh", true)]
        [InlineData("1234s-ag", true)]
        [InlineData("123456s", true)]
        [InlineData("1234s-agoooo", true)]
        [InlineData("1234k-ago", true)]
        public static void TestParseTime(string input, bool resultNull)
        {
            var time = DateTime.UtcNow;
            var result = time.AddDays(-14);

            var converted = CogniteTime.ParseTimestampString(input, time);

            if (resultNull)
            {
                Assert.Null(converted);
            }
            else
            {
                Assert.NotNull(converted);
                Assert.Equal(result, converted);
            }
        }

        [Fact]
        public static void TestParseTimeAbsolute()
        {
            var time = DateTime.UtcNow.AddDays(-6);
            var raw = CogniteTime.ToUnixTimeMilliseconds(time);

            Assert.Equal(raw, CogniteTime.ParseTimestampString(raw.ToString(), time)?.ToUnixTimeMilliseconds());
        }
    }
}