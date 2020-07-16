using Cognite.Extractor.Utils;
using CogniteSdk;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace ExtractorUtils.Test
{
    public class BinaryBufferTest
    {
        [Fact]
        public async Task TestBinaryBufferData()
        {
            var t1 = DateTime.UtcNow;
            var t2 = t1.AddDays(1);
            var t3 = t2.AddDays(1);
            var t4 = t3.AddDays(1);
            // Illegal combinations doesn't matter in this test, it's just to check that serialization goes
            // well for any combination
            var dps1 = new Dictionary<Identity, IEnumerable<Datapoint>>(new IdentityComparer()) {
                { Identity.Create("id1"), new[] { new Datapoint(t1, 123.123), new Datapoint(t2, "123") } },
                { Identity.Create(123), new [] {new Datapoint(t1, "321"), new Datapoint(t2, 321.321) } },
                { Identity.Create("empty"), Array.Empty<Datapoint>() }
            };
            var dps2 = new Dictionary<Identity, IEnumerable<Datapoint>>(new IdentityComparer()) {
                { Identity.Create("id1"), new[] { new Datapoint(t3, 123.123), new Datapoint(t4, "123") } },
                { Identity.Create(234), new [] {new Datapoint(t3, "321"), new Datapoint(t4, 321.321) } },
                { Identity.Create("empty"), Array.Empty<Datapoint>() }
            };

            bool dpEquals(Datapoint lhs, Datapoint rhs)
            {
                if (lhs.Timestamp != rhs.Timestamp) return false;
                if (lhs.IsString != rhs.IsString) return false;
                if (lhs.IsString) return lhs.StringValue == rhs.StringValue;
                return lhs.NumericValue == rhs.NumericValue;
            }

            IDictionary<Identity, IEnumerable<Datapoint>> readDps;

            using (var stream = new MemoryStream())
            {
                await CogniteUtils.WriteDatapointsAsync(dps1, stream);
                await CogniteUtils.WriteDatapointsAsync(dps2, stream);

                stream.Position = 0;

                readDps = await CogniteUtils.ReadDatapointsAsync(stream, CancellationToken.None);
            }
            Assert.Equal(3, readDps.Count);
            Assert.False(readDps.ContainsKey(Identity.Create("empty")));
            Assert.True(readDps.ContainsKey(Identity.Create("id1")));
            Assert.True(readDps.ContainsKey(Identity.Create(123)));
            Assert.True(readDps.ContainsKey(Identity.Create(234)));
            var id1 = Identity.Create("id1");
            Assert.Equal(4, readDps[id1].Count());
            foreach (var dp in readDps[id1])
            {
                Assert.True(dps1[id1].Any(odp => dpEquals(odp, dp)) || dps2[id1].Any(odp => dpEquals(odp, dp)));
            }
        }
    }
}
