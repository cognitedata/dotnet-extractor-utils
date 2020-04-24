using System;
using System.Linq;
using Xunit;

namespace ExtractorUtils.Test
{
    public class CogniteUtilsTest
    {
        private static readonly DataPoint[] _points = {
            new DataPoint(DateTime.UtcNow , 0),
            new DataPoint(DateTime.UtcNow, 100),        // 1
            new DataPoint(DateTime.UtcNow, -100),       // 2
            new DataPoint(DateTime.UtcNow, -101),       // 3
            new DataPoint(DateTime.UtcNow, 101),        // 4
            new DataPoint(DateTime.UtcNow, -1e100),     // 5
            new DataPoint(DateTime.UtcNow, 1e100),      // 6
            new DataPoint(DateTime.UtcNow, -1e101),     // 7
            new DataPoint(DateTime.UtcNow, 1e101),      // 8
            new DataPoint(DateTime.UtcNow, double.NaN), // 9
            new DataPoint(DateTime.UtcNow, double.PositiveInfinity), // 10
            new DataPoint(DateTime.UtcNow, double.NegativeInfinity), // 11
        };

        private static readonly DataPoint[] _stringPoints = {
            new DataPoint(DateTime.UtcNow, "Some String Value"),
            new DataPoint(DateTime.UtcNow, String.Empty),
            new DataPoint(DateTime.UtcNow, new string(Enumerable.Repeat('a', 255).ToArray())),
            new DataPoint(DateTime.UtcNow, new string(Enumerable.Repeat('b', 256).ToArray())),
        };

        [Fact]
        public static void TestTrimDoubles()
        {
            var values = _points.TrimValues().ToList();
            Assert.Equal(_points.Length - 3, values.Count()); // except invalid ones
            Assert.True(values.All(p => p.NumericValue <= CogniteUtils.NumericValueMax));
            Assert.True(values.All(p => p.NumericValue >= CogniteUtils.NumericValueMin));
            Assert.True(!values.Any(p => double.IsNaN(p.NumericValue.Value)));
        }


        [Fact]
        public static void TestTrimStrings()
        {
            var values = _stringPoints.TrimValues().ToList();
            Assert.Equal(_stringPoints.Count(), values.Count());
            Assert.True(values.All(p => p.StringValue.Length <= CogniteUtils.StringLengthMax));
            var payloads = values.Select(v => v.StringValue).ToList();
            Assert.True(_stringPoints.Select(v => v.StringValue).All(v => payloads.Any(p => v.StartsWith(p))));
        }

    }
}