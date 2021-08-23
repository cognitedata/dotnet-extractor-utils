using Cognite.Extensions;
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
                // To check if it handles non utf8 symbols...
                { Identity.Create("idæøå1"), new[] { new Datapoint(t1, 123.123), new Datapoint(t2, "123") } },
                { Identity.Create(123), new [] {new Datapoint(t1, "321"), new Datapoint(t2, 321.321) } },
                { Identity.Create("empty"), Array.Empty<Datapoint>() }
            };
            var dps2 = new Dictionary<Identity, IEnumerable<Datapoint>>(new IdentityComparer()) {
                { Identity.Create("idæøå1"), new[] { new Datapoint(t3, 123.123), new Datapoint(t4, "123") } },
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
                await CogniteUtils.WriteDatapointsAsync(dps1, stream, CancellationToken.None);
                await CogniteUtils.WriteDatapointsAsync(dps2, stream, CancellationToken.None);

                stream.Position = 0;

                readDps = await CogniteUtils.ReadDatapointsAsync(stream, CancellationToken.None);
            }
            Assert.Equal(3, readDps.Count);
            Assert.False(readDps.ContainsKey(Identity.Create("empty")));
            Assert.True(readDps.ContainsKey(Identity.Create("idæøå1")));
            Assert.True(readDps.ContainsKey(Identity.Create(123)));
            Assert.True(readDps.ContainsKey(Identity.Create(234)));
            var id1 = Identity.Create("idæøå1");
            Assert.Equal(4, readDps[id1].Count());
            foreach (var dp in readDps[id1])
            {
                Assert.True(dps1[id1].Any(odp => dpEquals(odp, dp)) || dps2[id1].Any(odp => dpEquals(odp, dp)));
            }
        }
        [Fact]
        public async Task TestBinaryBufferEvents()
        {
            var events = new List<EventCreate>
            {
                new EventCreate
                {
                    ExternalId = "id",
                    Type = "type",
                    AssetIds = new List<long> { 123, 234 },
                    Description = "description",
                    Subtype = "subtype",
                    StartTime = 100,
                    EndTime = 1000,
                    DataSetId = 123123,
                    Metadata = new Dictionary<string, string>
                    {
                        { "key1", "value1" },
                        { "key2", "value2" }
                    },
                    Source = "source"
                },
                new EventCreate
                {
                    Description = "empty"
                },
                new EventCreate
                {
                    ExternalId = "id2",
                    Type = "type",
                    AssetIds = new List<long> { 123, 234 },
                    Description = "description",
                    Subtype = "subtype",
                    StartTime = 100,
                    EndTime = 1000,
                    DataSetId = 123123,
                    Metadata = new Dictionary<string, string>
                    {
                        { "key1", "value1" },
                        { "key2", "value2" }
                    },
                    Source = "source"
                },
            };
            IEnumerable<EventCreate> readEvents;
            using (var stream = new MemoryStream())
            {
                await CogniteUtils.WriteEventsAsync(events, stream, CancellationToken.None);
                await CogniteUtils.WriteEventsAsync(events, stream, CancellationToken.None);

                stream.Position = 0;

                readEvents = await CogniteUtils.ReadEventsAsync(stream, CancellationToken.None);
            }
            Assert.Equal(6, readEvents.Count());

            void EventsEqual(EventCreate ev1, EventCreate ev2)
            {
                Assert.Equal(ev1.ExternalId, ev2.ExternalId);
                Assert.Equal(ev1.Type, ev2.Type);
                Assert.True(ev2.AssetIds == null && ev1.AssetIds == null || ev1.AssetIds != null && ev2.AssetIds != null);
                if (ev1.AssetIds != null)
                {
                    Assert.Equal(ev1.AssetIds.Count(), ev2.AssetIds.Count());
                    for (int i = 0; i < ev1.AssetIds.Count(); i++) Assert.Equal(ev1.AssetIds.ElementAt(i), ev2.AssetIds.ElementAt(i));
                }

                Assert.True(ev2.Metadata == null && ev1.Metadata == null || ev1.Metadata != null && ev2.Metadata != null);
                if (ev1.Metadata != null)
                {
                    Assert.Equal(ev1.Metadata.Count(), ev2.Metadata.Count());
                    foreach (var kvp in ev1.Metadata) Assert.Equal(kvp.Value, ev2.Metadata[kvp.Key]);
                }

                Assert.Equal(ev1.Description, ev2.Description);
                Assert.Equal(ev1.Subtype, ev2.Subtype);
                Assert.Equal(ev1.StartTime, ev2.StartTime);
                Assert.Equal(ev1.DataSetId, ev2.DataSetId);
                Assert.Equal(ev1.Source, ev2.Source);
            }

            for (int i = 0; i < 6; i++)
            {
                EventsEqual(events[i % 3], readEvents.ElementAt(i));
            }
        }
    }
}
