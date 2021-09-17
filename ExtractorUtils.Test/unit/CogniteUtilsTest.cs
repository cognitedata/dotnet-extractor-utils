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