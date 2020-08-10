using Cognite.Extensions;
using CogniteSdk;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Xunit;

namespace ExtractorUtils.Test
{
    public class SanitationTest
    {
        [Fact]
        public void TestSanitizeAsset()
        {
            var asset = new AssetCreate
            {
                ExternalId = new string('æ', 300),
                Description = new string('æ', 1000),
                DataSetId = -2502,
                Labels = new CogniteExternalId[] { null, new CogniteExternalId(null) }.Concat(Enumerable.Range(0, 100).Select(i => new CogniteExternalId(new string('æ', 300)))),
                Metadata = Enumerable.Range(0, 100)
                    .ToDictionary(i => $"key{i.ToString("000")}{new string('æ', 100)}", i => new string('æ', 200)),
                Name = new string('ø', 1000),
                ParentExternalId = new string('æ', 300),
                ParentId = -1234,
                Source = new string('æ', 12345)
            };

            asset.Sanitize();

            Assert.Equal(new string('æ', 255), asset.ExternalId);
            Assert.Equal(new string('æ', 500), asset.Description);
            Assert.Null(asset.DataSetId);
            Assert.Equal(10, asset.Labels.Count());
            Assert.All(asset.Labels, ext => Assert.Equal(new string('æ', 255), ext.ExternalId));
            Assert.Equal(19, asset.Metadata.Count);
            // 'æ' is 2 bytes, key{i} will be 6 bytes, so 128-6 = 122, 122/2 = 61, 61 + 6 = 67
            Assert.All(asset.Metadata, kvp => Assert.Equal(67, kvp.Key.Length));
            Assert.All(asset.Metadata, kvp => Assert.Equal(new string('æ', 200), kvp.Value));
            Assert.Equal(new string('ø', 140), asset.Name);
            Assert.Equal(new string('æ', 255), asset.ParentExternalId);
            Assert.Null(asset.ParentId);
            Assert.Equal(new string('æ', 128), asset.Source);
        }
        [Fact]
        public void TestSanitizeTimeSeries()
        {
            var ts = new TimeSeriesCreate
            {
                ExternalId = new string('æ', 300),
                Description = new string('æ', 2000),
                DataSetId = -2952,
                AssetId = -1239,
                LegacyName = new string('æ', 300),
                Metadata = Enumerable.Range(0, 100)
                    .ToDictionary(i => $"key{i.ToString("000")}{new string('æ', 100)}", i => new string('æ', 600)),
                Name = new string('æ', 300),
                Unit = new string('æ', 200)
            };

            ts.Sanitize();

            Assert.Equal(new string('æ', 255), ts.ExternalId);
            Assert.Equal(new string('æ', 1000), ts.Description);
            Assert.Null(ts.DataSetId);
            Assert.Null(ts.AssetId);
            Assert.Equal(new string('æ', 255), ts.LegacyName);
            Assert.Equal(16, ts.Metadata.Count);
            // 32-6 = 26, 26/2 = 13, 13+6 = 19.
            Assert.All(ts.Metadata, kvp => Assert.Equal(19, kvp.Key.Length));
            Assert.All(ts.Metadata, kvp => Assert.Equal(256, kvp.Value.Length));
            Assert.Equal(new string('æ', 255), ts.Name);
            Assert.Equal(new string('æ', 32), ts.Unit);
        }
        [Fact]
        public void TestSanitizeEvent()
        {
            var evt = new EventCreate
            {
                AssetIds = Enumerable.Range(-100, 100000).Select(i => (long)i),
                DataSetId = -125,
                Description = new string('æ', 1000),
                EndTime = -12345,
                StartTime = -12345,
                ExternalId = new string('æ', 300),
                Metadata = Enumerable.Range(0, 200)
                    .ToDictionary(i => $"key{i.ToString("000")}{new string('æ', 100)}", i => new string('æ', 600)),
                Source = new string('æ', 200),
                Subtype = new string('æ', 300),
                Type = new string('æ', 300)
            };

            evt.Sanitize();

            Assert.Equal(10000, evt.AssetIds.Count());
            Assert.All(evt.AssetIds, id => Assert.True(id > 0));
            Assert.Null(evt.DataSetId);
            Assert.Equal(new string('æ', 500), evt.Description);
            Assert.Equal(0, evt.StartTime);
            Assert.Equal(0, evt.EndTime);
            Assert.Equal(new string('æ', 255), evt.ExternalId);
            Assert.Equal(150, evt.Metadata.Count);
            Assert.Equal(new string('æ', 128), evt.Source);
            Assert.Equal(new string('æ', 64), evt.Type);
            Assert.Equal(new string('æ', 64), evt.Subtype);
        }
        [Theory]
        [InlineData("æææææ", 4)]
        [InlineData("123412341234", 9)]
        [InlineData("123456æææ", 7)]
        public void TestUtf8Truncate(string str, int finalLength)
        {
            Assert.Equal(finalLength, str?.LimitUtf8ByteCount(9)?.Length ?? 0);
        }
    }
}
