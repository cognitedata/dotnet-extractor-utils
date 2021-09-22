using System;
using System.Collections.Generic;
using System.Linq;
using CogniteSdk;
using Xunit;
using Cognite.Extractor.Common;
using Cognite.Extensions;

namespace ExtractorUtils.Test.Unit
{
    public class CogniteUtilsTest
    {
        private static readonly Datapoint[] _points = {
            new Datapoint(DateTime.UtcNow , 0),
            new Datapoint(DateTime.UtcNow, 100),        // 1
            new Datapoint(DateTime.UtcNow, -100),       // 2
            new Datapoint(DateTime.UtcNow, -101),       // 3
            new Datapoint(DateTime.UtcNow, 101),        // 4
            new Datapoint(DateTime.UtcNow, -1e100),     // 5
            new Datapoint(DateTime.UtcNow, 1e100),      // 6
            new Datapoint(DateTime.UtcNow, -1e101),     // 7
            new Datapoint(DateTime.UtcNow, 1e101),      // 8
            new Datapoint(DateTime.UtcNow, double.NaN), // 9
            new Datapoint(DateTime.UtcNow, double.PositiveInfinity), // 10
            new Datapoint(DateTime.UtcNow, double.NegativeInfinity), // 11
        };

        private static readonly Datapoint[] _stringPoints = {
            new Datapoint(DateTime.UtcNow, "Some String Value"),
            new Datapoint(DateTime.UtcNow, String.Empty),
            new Datapoint(DateTime.UtcNow, new string(Enumerable.Repeat('a', 255).ToArray())),
            new Datapoint(DateTime.UtcNow, new string(Enumerable.Repeat('b', 256).ToArray())),
        };

        private static readonly Datapoint[] _points_timestamp = {
            new Datapoint(CogniteTime.DateTimeEpoch , 0),
            new Datapoint(CogniteTime.FromUnixTimeMilliseconds(CogniteUtils.TimestampMin), 1),
            new Datapoint(CogniteTime.FromUnixTimeMilliseconds(CogniteUtils.TimestampMax), 2),
            new Datapoint(new DateTime(2051, 1, 1, 12, 0, 0, DateTimeKind.Utc), 3)
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

        [Fact]
        public static void TestRemoveOutOfRangeTimestamps()
        {
            var values = _points_timestamp.RemoveOutOfRangeTimestamps();
            Assert.Equal(_points_timestamp.Count() - 2, values.Count());
            Assert.Contains(_points_timestamp[1], values);
            Assert.Contains(_points_timestamp[2], values);
        }
    }
}