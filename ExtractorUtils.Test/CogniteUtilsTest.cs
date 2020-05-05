using System;
using System.Collections.Generic;
using System.Linq;
using Cognite.Extractor.Utils;
using CogniteSdk;
using Xunit;
using DataPoint = Cognite.Extractor.Utils.DataPoint;

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

        [Fact]
        public static void TestIdentityComparer()
        {
            var comparer = new IdentityComparer();
            var id1 = new Identity(1L);
            var id2 = new Identity("ExternalId");
            Assert.True(comparer.Equals(id1, id1)); // same object
            Assert.False(comparer.Equals(id1, null));
            Assert.False(comparer.Equals(null, id1));
            Assert.False(comparer.Equals(id1, new Identity(null)));
            Assert.False(comparer.Equals(id1, id2));
            Assert.True(comparer.Equals(id1, new Identity(1L)));
            Assert.True(comparer.Equals(id2, new Identity("ExternalId")));

            var set = new HashSet<Identity>(comparer);
            set.Add(id1);
            set.Add(id2);

            Assert.Contains(id1, set);
            Assert.Contains(new Identity(1L), set);
            Assert.Contains(id2, set);
            Assert.Contains(new Identity("ExternalId"), set);
            Assert.DoesNotContain(new Identity(2L), set);
            Assert.DoesNotContain(new Identity("ExternalId2"), set);
        }
    }
}