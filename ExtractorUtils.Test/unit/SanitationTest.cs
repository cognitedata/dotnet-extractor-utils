using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Cognite.Extensions;
using Cognite.Extractor.Common;
using CogniteSdk;
using Xunit;

namespace ExtractorUtils.Test.Unit
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
        public void TestVerifyAsset()
        {
            var removeFields = new List<ResourceType>
            {
                ResourceType.ExternalId, ResourceType.Name, ResourceType.ParentId,
                ResourceType.ParentExternalId, ResourceType.Description, ResourceType.DataSetId,
                ResourceType.Metadata, ResourceType.Source, ResourceType.Labels
            };
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
            foreach (var field in removeFields)
            {
                var errType = asset.Verify();
                Assert.Equal(field, errType);
                switch (field)
                {
                    case ResourceType.ExternalId: asset.ExternalId = null; break;
                    case ResourceType.Name: asset.Name = "name"; break;
                    case ResourceType.ParentId: asset.ParentId = null; break;
                    case ResourceType.ParentExternalId: asset.ParentExternalId = null; break;
                    case ResourceType.Description: asset.Description = null; break;
                    case ResourceType.DataSetId: asset.DataSetId = null; break;
                    case ResourceType.Metadata: asset.Metadata = null; break;
                    case ResourceType.Source: asset.Source = null; break;
                    case ResourceType.Labels: asset.Labels = null; break;
                }
            }
            Assert.Null(asset.Verify());
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public void TestSanitizeAssetUpdate(bool addMeta)
        {
            var meta = Enumerable.Range(0, 100)
                    .ToDictionary(i => $"key{i.ToString("000")}{new string('æ', 100)}", i => new string('æ', 200));
            var update = new AssetUpdateItem(new string('æ', 300))
            {
                Update = new AssetUpdate
                {
                    Description = new UpdateNullable<string>(new string('æ', 2000)),
                    DataSetId = new UpdateNullable<long?>(-251),
                    ExternalId = new UpdateNullable<string>(new string('æ', 300)),
                    Labels = new UpdateLabels<IEnumerable<CogniteExternalId>>(new CogniteExternalId[] { null, new CogniteExternalId(null) }
                        .Concat(Enumerable.Range(0, 100).Select(i => new CogniteExternalId(new string('æ', 300)))), Array.Empty<CogniteExternalId>()),
                    Metadata = addMeta ? new UpdateDictionary<string>(meta, Enumerable.Empty<string>())
                        : new UpdateDictionary<string>(meta),
                    Name = new Update<string>(new string('æ', 1000)),
                    ParentExternalId = new Update<string>(new string('æ', 300)),
                    ParentId = new Update<long?>(-1234),
                    Source = new UpdateNullable<string>(new string('æ', 12345))
                }
            };

            update.Sanitize();

            Assert.Equal(new string('æ', 255), update.ExternalId);
            Assert.Equal(new string('æ', 500), update.Update.Description.Set);
            Assert.Null(update.Update.DataSetId);
            Assert.Equal(10, update.Update.Labels.Add.Count());
            Assert.All(update.Update.Labels.Add, ext => Assert.Equal(new string('æ', 255), ext.ExternalId));
            var sanitizedMeta = addMeta ? update.Update.Metadata.Add : update.Update.Metadata.Set;
            Assert.Equal(19, sanitizedMeta.Count);
            // 'æ' is 2 bytes, key{i} will be 6 bytes, so 128-6 = 122, 122/2 = 61, 61 + 6 = 67
            Assert.All(sanitizedMeta, kvp => Assert.Equal(67, kvp.Key.Length));
            Assert.All(sanitizedMeta, kvp => Assert.Equal(new string('æ', 200), kvp.Value));
            Assert.Equal(new string('æ', 140), update.Update.Name.Set);
            Assert.Equal(new string('æ', 255), update.Update.ParentExternalId.Set);
            Assert.Null(update.Update.ParentId);
            Assert.Equal(new string('æ', 128), update.Update.Source.Set);
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public void TestVerifyAssetUpdate(bool addMeta)
        {
            var removeFields = new List<ResourceType>
            {
                ResourceType.ExternalId, ResourceType.Id, ResourceType.Id, ResourceType.ExternalId,
                ResourceType.Name, ResourceType.Description, ResourceType.DataSetId, ResourceType.Metadata,
                ResourceType.Source, ResourceType.ParentId, ResourceType.ParentId,
                ResourceType.ParentExternalId, ResourceType.Labels, ResourceType.Update
            };

            var meta = Enumerable.Range(0, 100)
                    .ToDictionary(i => $"key{i.ToString("000")}{new string('æ', 100)}", i => new string('æ', 200));
            var item = new AssetUpdateItem(new string('æ', 300))
            {
                Update = new AssetUpdate
                {
                    Description = new UpdateNullable<string>(new string('æ', 2000)),
                    DataSetId = new UpdateNullable<long?>(-251),
                    ExternalId = new UpdateNullable<string>(new string('æ', 300)),
                    Labels = new UpdateLabels<IEnumerable<CogniteExternalId>>(new CogniteExternalId[] { null, new CogniteExternalId(null) }
                        .Concat(Enumerable.Range(0, 100).Select(i => new CogniteExternalId(new string('æ', 300)))), Array.Empty<CogniteExternalId>()),
                    Metadata = addMeta ? new UpdateDictionary<string>(meta, Enumerable.Empty<string>())
                        : new UpdateDictionary<string>(meta),
                    Name = new Update<string>(new string('æ', 1000)),
                    ParentExternalId = new Update<string>(new string('æ', 300)),
                    ParentId = new Update<long?>(-1234),
                    Source = new UpdateNullable<string>(new string('æ', 12345))
                }
            };

            foreach (var field in removeFields)
            {
                var errType = item.Verify();
                Assert.Equal(field, errType);
                var update = item.Update;
                switch (field)
                {
                    case ResourceType.ExternalId:
                        if (item.ExternalId == null) update.ExternalId = null;
                        else item.ExternalId = null;
                        break;
                    case ResourceType.Id:
                        if (item.Id == null) item.Id = -123;
                        else (item.Id) = 123;
                        break;
                    case ResourceType.Name: update.Name = null; break;
                    case ResourceType.Description: update.Description = null; break;
                    case ResourceType.DataSetId: update.DataSetId = null; break;
                    case ResourceType.Metadata: update.Metadata = null; break;
                    case ResourceType.Source: update.Source = null; break;
                    case ResourceType.ParentId:
                        if (update.ParentId?.Set != null && update.ParentId.Set < 0) update.ParentId = new Update<long?>(123);
                        else update.ParentId = null;
                        break;
                    case ResourceType.ParentExternalId: update.ParentExternalId = null; break;
                    case ResourceType.Labels: update.Labels = null; break;
                    case ResourceType.Update:
                        update.Name = new Update<string>("name");
                        break;
                }
            }
            Assert.Null(item.Verify());
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
                    .ToDictionary(i => $"key{i.ToString("000")}{new string('æ', 100)}", i => new string('æ', 200)),
                Name = new string('æ', 300),
                Unit = new string('æ', 200)
            };

            ts.Sanitize();

            Assert.Equal(new string('æ', 255), ts.ExternalId);
            Assert.Equal(new string('æ', 1000), ts.Description);
            Assert.Null(ts.DataSetId);
            Assert.Null(ts.AssetId);
            Assert.Equal(new string('æ', 255), ts.LegacyName);
            Assert.Equal(18, ts.Metadata.Count);
            // 'æ' is 2 bytes, key{i} will be 6 bytes, so 128-6 = 122, 122/2 = 61, 61 + 6 = 67
            Assert.All(ts.Metadata, kvp => Assert.Equal(67, kvp.Key.Length));
            Assert.All(ts.Metadata, kvp => Assert.Equal(new string('æ', 200), kvp.Value));
            Assert.Equal(new string('æ', 255), ts.Name);
            Assert.Equal(new string('æ', 32), ts.Unit);
        }
        [Fact]
        public void TestVerifyTimeSeries()
        {
            var removeFields = new List<ResourceType>
            {
                ResourceType.ExternalId, ResourceType.Name, ResourceType.AssetId,
                ResourceType.Description, ResourceType.DataSetId, ResourceType.Metadata,
                ResourceType.Unit, ResourceType.LegacyName
            };
            var ts = new TimeSeriesCreate
            {
                ExternalId = new string('æ', 300),
                Description = new string('æ', 2000),
                DataSetId = -2952,
                AssetId = -1239,
                LegacyName = new string('æ', 300),
                Metadata = Enumerable.Range(0, 100)
                    .ToDictionary(i => $"key{i.ToString("000")}{new string('æ', 100)}", i => new string('æ', 200)),
                Name = new string('æ', 300),
                Unit = new string('æ', 200)
            };
            foreach (var field in removeFields)
            {
                var errType = ts.Verify();
                Assert.Equal(field, errType);
                switch (field)
                {
                    case ResourceType.ExternalId: ts.ExternalId = null; break;
                    case ResourceType.Name: ts.Name = null; break;
                    case ResourceType.AssetId: ts.AssetId = null; break;
                    case ResourceType.Description: ts.Description = null; break;
                    case ResourceType.DataSetId: ts.DataSetId = null; break;
                    case ResourceType.Metadata: ts.Metadata = null; break;
                    case ResourceType.Unit: ts.Unit = null; break;
                    case ResourceType.LegacyName: ts.LegacyName = null; break;
                }
            }
            Assert.Null(ts.Verify());
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public void TestSanitizeTimeSeriesUpdate(bool addMeta)
        {
            var meta = Enumerable.Range(0, 100)
                    .ToDictionary(i => $"key{i.ToString("000")}{new string('æ', 100)}", i => new string('æ', 200));
            var ts = new TimeSeriesUpdateItem(new string('æ', 300))
            {
                Update = new TimeSeriesUpdate
                {
                    ExternalId = new UpdateNullable<string>(new string('æ', 300)),
                    DataSetId = new UpdateNullable<long?>(-123),
                    Description = new UpdateNullable<string>(new string('æ', 2000)),
                    AssetId = new UpdateNullable<long?>(-123),
                    Metadata = addMeta ? new UpdateDictionary<string>(meta, Enumerable.Empty<string>())
                        : new UpdateDictionary<string>(meta),
                    Name = new UpdateNullable<string>(new string('æ', 300)),
                    Unit = new UpdateNullable<string>(new string('æ', 200))
                }
            };

            ts.Sanitize();

            Assert.Equal(new string('æ', 255), ts.ExternalId);
            Assert.Equal(new string('æ', 255), ts.Update.ExternalId.Set);
            Assert.Null(ts.Update.DataSetId);
            Assert.Equal(new string('æ', 1000), ts.Update.Description.Set);
            Assert.Null(ts.Update.AssetId);
            if (addMeta)
            {
                meta = ts.Update.Metadata.Add;
            }
            else
            {
                meta = ts.Update.Metadata.Set;
            }
            Assert.Equal(18, meta.Count);
            Assert.All(meta, kvp => Assert.Equal(67, kvp.Key.Length));
            Assert.All(meta, kvp => Assert.Equal(new string('æ', 200), kvp.Value));
            Assert.Equal(new string('æ', 255), ts.Update.Name.Set);
            Assert.Equal(new string('æ', 32), ts.Update.Unit.Set);
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public void TestVerifyTimeSeriesUpdate(bool addMeta)
        {
            var removeFields = new List<ResourceType>
            {
                ResourceType.ExternalId, ResourceType.Id, ResourceType.Id, ResourceType.ExternalId,
                ResourceType.Name, ResourceType.AssetId, ResourceType.Description, ResourceType.DataSetId,
                ResourceType.Metadata, ResourceType.Unit, ResourceType.Update
            };


            var meta = Enumerable.Range(0, 100)
                    .ToDictionary(i => $"key{i.ToString("000")}{new string('æ', 100)}", i => new string('æ', 200));
            var ts = new TimeSeriesUpdateItem(new string('æ', 300))
            {
                Update = new TimeSeriesUpdate
                {
                    ExternalId = new UpdateNullable<string>(new string('æ', 300)),
                    DataSetId = new UpdateNullable<long?>(-123),
                    Description = new UpdateNullable<string>(new string('æ', 2000)),
                    AssetId = new UpdateNullable<long?>(-123),
                    Metadata = addMeta ? new UpdateDictionary<string>(meta, Enumerable.Empty<string>())
                        : new UpdateDictionary<string>(meta),
                    Name = new UpdateNullable<string>(new string('æ', 300)),
                    Unit = new UpdateNullable<string>(new string('æ', 200))
                }
            };

            foreach (var field in removeFields)
            {
                var errType = ts.Verify();
                Assert.Equal(field, errType);
                var update = ts.Update;
                switch (errType)
                {
                    case ResourceType.ExternalId:
                        if (ts.ExternalId == null) update.ExternalId = null;
                        else ts.ExternalId = null;
                        break;
                    case ResourceType.Id:
                        if (ts.Id == null) ts.Id = -123;
                        else (ts.Id) = 123;
                        break;
                    case ResourceType.Name: update.Name = null; break;
                    case ResourceType.AssetId: update.AssetId = null; break;
                    case ResourceType.Description: update.Description = null; break;
                    case ResourceType.DataSetId: update.DataSetId = null; break;
                    case ResourceType.Metadata: update.Metadata = null; break;
                    case ResourceType.Unit: update.Unit = null; break;
                    case ResourceType.Update: update.Name = new UpdateNullable<string>("name"); break;
                }
            }

            Assert.Null(ts.Verify());
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
        [Fact]
        public void TestVerifyEvent()
        {
            var removeFields = new List<ResourceType>
            {
                ResourceType.ExternalId, ResourceType.Type, ResourceType.SubType,
                ResourceType.Source, ResourceType.AssetId, ResourceType.TimeRange,
                ResourceType.DataSetId, ResourceType.Metadata
            };
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
            foreach (var field in removeFields)
            {
                var errType = evt.Verify();
                Assert.Equal(field, errType);
                switch (field)
                {
                    case ResourceType.ExternalId: evt.ExternalId = null; break;
                    case ResourceType.Type: evt.Type = null; break;
                    case ResourceType.SubType: evt.Subtype = null; break;
                    case ResourceType.Source: evt.Source = null; break;
                    case ResourceType.AssetId: evt.AssetIds = null; break;
                    case ResourceType.TimeRange:
                        evt.StartTime = 100;
                        evt.EndTime = 1000;
                        break;
                    case ResourceType.DataSetId: evt.DataSetId = null; break;
                    case ResourceType.Metadata: evt.Metadata = null; break;
                }
            }
            Assert.Null(evt.Verify());
        }

        [Fact]
        public void TestSanitizeSequence()
        {
            var seq = new SequenceCreate
            {
                AssetId = -123,
                DataSetId = -123,
                ExternalId = new string('æ', 300),
                Metadata = Enumerable.Range(0, 200)
                    .ToDictionary(i => $"key{i.ToString("000")}{new string('æ', 100)}", i => new string('æ', 600)),
                Name = new string('æ', 300),
                Description = new string('æ', 2000),
                Columns = Enumerable.Range(0, 20).Select(i =>
                    new SequenceColumnWrite
                    {
                        ExternalId = new string('æ', 300),
                        Name = new string('æ', 300),
                        Description = new string('æ', 2000),
                        Metadata = Enumerable.Range(0, 200)
                                .ToDictionary(i => $"key{i.ToString("000")}{new string('æ', 100)}", i => new string('æ', 600)),
                    }).ToArray()
            };
            seq.Sanitize();
            Assert.Null(seq.AssetId);
            Assert.Null(seq.DataSetId);
            Assert.Equal(new string('æ', 255), seq.ExternalId);
            Assert.Equal(new string('æ', 1000), seq.Description);
            // (600 chars * 2 bytes + 32) * 7 is about 10000 bytes
            Assert.Equal(8, seq.Metadata.Count);
            Assert.Equal(new string('æ', 255), seq.Name);
            int idx = 0;
            foreach (var col in seq.Columns)
            {
                Assert.Equal(new string('æ', 255), col.ExternalId);
                Assert.Equal(new string('æ', 1000), col.Description);
                Assert.Equal(new string('æ', 64), col.Name);
                if (idx < 9)
                {
                    Assert.Equal(8, col.Metadata.Count);
                }
                else if (idx == 9)
                {
                    Assert.Single(col.Metadata);
                }
                else
                {
                    Assert.Empty(col.Metadata);
                }
                idx++;
            }
        }
        [Fact]
        public void TestVerifySequence()
        {
            var removeFields = new List<ResourceType>
            {
                ResourceType.ExternalId, ResourceType.Name, ResourceType.AssetId, ResourceType.Description,
                ResourceType.DataSetId, ResourceType.Metadata, ResourceType.SequenceColumns,
                ResourceType.ColumnExternalId, ResourceType.ColumnName, ResourceType.ColumnDescription,
                ResourceType.ColumnMetadata
            };
            var seq = new SequenceCreate
            {
                AssetId = -123,
                DataSetId = -123,
                ExternalId = new string('æ', 300),
                Metadata = Enumerable.Range(0, 200)
                    .ToDictionary(i => $"key{i.ToString("000")}{new string('æ', 100)}", i => new string('æ', 600)),
                Name = new string('æ', 300),
                Description = new string('æ', 2000)
            };

            foreach (var field in removeFields)
            {
                var errType = seq.Verify();
                Assert.Equal(field, errType);
                switch (field)
                {
                    case ResourceType.ExternalId: seq.ExternalId = null; break;
                    case ResourceType.Name: seq.Name = null; break;
                    case ResourceType.AssetId: seq.AssetId = null; break;
                    case ResourceType.Description: seq.Description = null; break;
                    case ResourceType.DataSetId: seq.DataSetId = null; break;
                    case ResourceType.Metadata: seq.Metadata = null; break;
                    case ResourceType.SequenceColumns:
                        seq.Columns = Enumerable.Range(0, 20).Select(i =>
                            new SequenceColumnWrite
                            {
                                ExternalId = new string('æ', 300),
                                Name = new string('æ', 300),
                                Description = new string('æ', 2000),
                                Metadata = Enumerable.Range(0, 200)
                                        .ToDictionary(i => $"key{i.ToString("000")}{new string('æ', 100)}", i => new string('æ', 600)),
                            }).ToArray();
                        break;
                    case ResourceType.ColumnExternalId:
                        int idx = 0;
                        foreach (var col in seq.Columns) col.ExternalId = $"idx{idx++}";
                        break;
                    case ResourceType.ColumnDescription:
                        foreach (var col in seq.Columns) col.Description = null;
                        break;
                    case ResourceType.ColumnName:
                        foreach (var col in seq.Columns) col.Name = null;
                        break;
                    case ResourceType.ColumnMetadata:
                        foreach (var col in seq.Columns) col.Metadata = null;
                        break;
                }
            }
            Assert.Null(seq.Verify());
        }

        [Fact]
        public void TestSanitizeSequenceData()
        {
            var rows = Enumerable.Range(0, 100).Select(num => new SequenceRow
            {
                RowNumber = num,
                Values = new MultiValue[]
                {
                    new MultiValue.String(new string('æ', 500)),
                    new MultiValue.Long(123),
                    new MultiValue.Double(double.PositiveInfinity),
                    new MultiValue.Double(double.NegativeInfinity),
                    new MultiValue.Double(double.MinValue),
                    new MultiValue.Double(double.MaxValue),
                    new MultiValue.Double(double.NaN),
                    null
                }
            }).ToList();
            var seq = new SequenceDataCreate
            {
                ExternalId = new string('æ', 500),
                Rows = rows
            };

            seq.Sanitize();

            Assert.Equal(new string('æ', 255), seq.ExternalId);
            foreach (var row in rows)
            {
                Assert.Equal(new string('æ', 255), (row.Values.ElementAt(0) as MultiValue.String).Value);
                Assert.Equal(123, (row.Values.ElementAt(1) as MultiValue.Long).Value);
                Assert.Null(row.Values.ElementAt(2));
                Assert.Null(row.Values.ElementAt(3));
                Assert.Equal(-1E100, (row.Values.ElementAt(4) as MultiValue.Double).Value);
                Assert.Equal(1E100, (row.Values.ElementAt(5) as MultiValue.Double).Value);
                Assert.Null(row.Values.ElementAt(6));
                Assert.Null(row.Values.ElementAt(7));
            }
        }

        [Fact]
        public void TestVerifySequenceData()
        {
            var removeFields = new[]
            {
                ResourceType.ExternalId, ResourceType.ExternalId, ResourceType.SequenceColumns,
                ResourceType.SequenceColumns, ResourceType.SequenceRows, ResourceType.SequenceRows
            };
            var seq = new SequenceDataCreate
            {
                Columns = Array.Empty<string>(),
                Rows = Array.Empty<SequenceRow>(),
                ExternalId = new string('æ', 300)
            };

            foreach (var field in removeFields)
            {
                var errType = seq.Verify();
                Assert.Equal(field, errType);
                switch (errType)
                {
                    case ResourceType.ExternalId:
                        if (seq.ExternalId == null) seq.ExternalId = "test";
                        else seq.ExternalId = null;
                        break;
                    case ResourceType.SequenceColumns:
                        if (seq.Columns == null) seq.Columns = new[]
                        {
                            "test"
                        };
                        else seq.Columns = null;
                        break;
                    case ResourceType.SequenceRows:
                        if (seq.Rows == null) seq.Rows = new[]
                        {
                            new SequenceRow
                            {
                                RowNumber = 1,
                                Values = new MultiValue[] { null }
                            }
                        };
                        else seq.Rows = null;
                        break;

                }
            }
        }

        [Theory]
        [InlineData(0.0)]
        [InlineData(321.321)]
        [InlineData(null)]
        public void TestSanitizeDataPoints(double? nanRepl)
        {
            var dps = new[]
            {
                new Datapoint(DateTime.UtcNow, 123.123),
                new Datapoint(DateTime.UtcNow, "test"),
                new Datapoint(DateTime.UtcNow, new string('æ', 500)),
                new Datapoint(DateTime.UtcNow, true, StatusCode.Parse("Good")),
                new Datapoint(DateTime.UtcNow, double.PositiveInfinity),
                new Datapoint(DateTime.UtcNow, double.NegativeInfinity),
                new Datapoint(DateTime.UtcNow, double.NaN),
                new Datapoint(DateTime.UtcNow, double.MaxValue),
                new Datapoint(DateTime.UtcNow, double.MinValue),
                new Datapoint(DateTime.UtcNow, 1E101),
                new Datapoint(DateTime.UtcNow, -1E101),
                new Datapoint(DateTime.UtcNow, false, StatusCode.Parse("Bad")),
                new Datapoint(DateTime.UtcNow, false, StatusCode.Parse("Good")),
                new Datapoint(DateTime.UtcNow, false, StatusCode.Parse("Uncertain"))
            };

            var cleanDps = dps.Select(dp => dp.Sanitize(nanRepl)).ToArray();

            Assert.Equal(123.123, cleanDps[0].NumericValue.Value);
            Assert.Equal("test", cleanDps[1].StringValue);
            Assert.Equal(new string('æ', 255), cleanDps[2].StringValue);
            Assert.Equal("", cleanDps[3].StringValue);
            Assert.Equal(1E100, cleanDps[4].NumericValue.Value);
            Assert.Equal(-1E100, cleanDps[5].NumericValue.Value);
            if (nanRepl.HasValue)
            {
                Assert.Equal(nanRepl, cleanDps[6].NumericValue.Value);
            }
            else
            {
                Assert.True(double.IsNaN(cleanDps[6].NumericValue.Value));
            }
            Assert.Equal(1E100, cleanDps[7].NumericValue.Value);
            Assert.Equal(-1E100, cleanDps[8].NumericValue.Value);
            Assert.Equal(1E100, cleanDps[9].NumericValue.Value);
            Assert.Equal(-1E100, cleanDps[10].NumericValue.Value);
            Assert.Null(cleanDps[11].NumericValue);
            Assert.Equal(0.0, cleanDps[12].NumericValue.Value);
            Assert.Equal(0.0, cleanDps[13].NumericValue.Value);
        }

        [Fact]
        public void TestVerifyDataPoint()
        {
            var dps = new[]
            {
                new Datapoint(DateTime.UtcNow, 123.123),
                new Datapoint(DateTime.UtcNow, "test"),
                new Datapoint(DateTime.UtcNow, false, StatusCode.Parse("Bad")),
                new Datapoint(DateTime.UtcNow, false, StatusCode.Parse("Uncertain")),
                new Datapoint(DateTime.UtcNow, false, StatusCode.Parse("Good")),
                new Datapoint(DateTime.UtcNow, new string('æ', 500)),
                new Datapoint(DateTime.UtcNow, null),
                new Datapoint(DateTime.UtcNow, double.PositiveInfinity),
                new Datapoint(DateTime.UtcNow, double.NegativeInfinity),
                new Datapoint(DateTime.UtcNow, double.NaN),
                new Datapoint(DateTime.UtcNow, double.MaxValue),
                new Datapoint(DateTime.UtcNow, double.MinValue),
                new Datapoint(DateTime.UtcNow, 1E101),
                new Datapoint(DateTime.UtcNow, -1E101),
                new Datapoint(DateTime.MaxValue, 1),
                new Datapoint(DateTime.MinValue, 1)
            };

            var result = dps.Select(dp => dp.Verify()).ToArray();

            var countGood = 3;
            var countValue = 11;

            for (int i = 0; i < countGood; i++) Assert.Null(result[i]);
            for (int i = countGood; i < countGood + countValue; i++) Assert.Equal(ResourceType.DataPointValue, result[i]);
            for (int i = countGood + countValue; i < dps.Length; i++) Assert.Equal(ResourceType.DataPointTimestamp, result[i]);
        }

        [Theory]
        [InlineData("æææææ", 4)]
        [InlineData("123412341234", 9)]
        [InlineData("123456æææ", 7)]
        public void TestUtf8Truncate(string str, int finalLength)
        {
            Assert.Equal(finalLength, str?.LimitUtf8ByteCount(9)?.Length ?? 0);
        }
        [Fact]
        public void TestSanitizeEventRequest()
        {
            var events = new[]
            {
                new EventCreate { ExternalId = "test1" },
                new EventCreate { ExternalId = "test1" },
                new EventCreate { ExternalId = "test2" },
                new EventCreate { ExternalId = "test2" },
                new EventCreate { ExternalId = "test3" }
            };
            var (result, errors) = Sanitation.CleanEventRequest(events, SanitationMode.Clean);
            var err = errors.First();
            Assert.Equal(3, result.Count());
            Assert.Equal(2, err.Values.Count());
            Assert.Equal(ErrorType.ItemDuplicated, err.Type);
            Assert.Equal(ResourceType.ExternalId, err.Resource);
            Assert.Equal(3, result.Select(evt => evt.ExternalId).Distinct().Count());
        }
        [Fact]
        public void TestSanitizeAssetRequest()
        {
            var assets = new[]
            {
                new AssetCreate { ExternalId = "test1", Name = "test" },
                new AssetCreate { ExternalId = "test1", Name = "test" },
                new AssetCreate { ExternalId = "test2", Name = "test" },
                new AssetCreate { ExternalId = "test2", Name = "test" },
                new AssetCreate { ExternalId = "test3", Name = "test" },
                new AssetCreate { ExternalId = "test4", Name = null }
            };
            var (result, errors) = Sanitation.CleanAssetRequest(assets, SanitationMode.Clean);
            var err = errors.First();
            Assert.Equal(2, errors.Count());
            Assert.Equal(3, result.Count());
            Assert.Equal(2, err.Values.Count());
            Assert.Equal(ErrorType.ItemDuplicated, err.Type);
            Assert.Equal(ResourceType.ExternalId, err.Resource);
            Assert.Equal(3, result.Select(evt => evt.ExternalId).Distinct().Count());
        }
        [Fact]
        public void TestSanitizeTimeSeriesRequest()
        {
            var timeseries = new[]
            {
                new TimeSeriesCreate { LegacyName = "test4", ExternalId = "test1" },
                new TimeSeriesCreate { LegacyName = "test5", ExternalId = "test1" },
                new TimeSeriesCreate { LegacyName = "test6", ExternalId = "test2" },
                new TimeSeriesCreate { LegacyName = "test7", ExternalId = "test2" },
                new TimeSeriesCreate { LegacyName = "test8", ExternalId = "test3" },
                new TimeSeriesCreate { LegacyName = "test1", ExternalId = "test4" },
                new TimeSeriesCreate { LegacyName = "test1", ExternalId = "test5" },
                new TimeSeriesCreate { LegacyName = "test2", ExternalId = "test6" },
                new TimeSeriesCreate { LegacyName = "test2", ExternalId = "test7" },
                new TimeSeriesCreate { LegacyName = "test3", ExternalId = "test8" }
            };
            var (result, errors) = Sanitation.CleanTimeSeriesRequest(timeseries, SanitationMode.Clean);
            var err = errors.First();
            var err2 = errors.ElementAt(1);
            Assert.Equal(6, result.Count());
            Assert.Equal(2, err.Values.Count());
            Assert.Equal(ErrorType.ItemDuplicated, err.Type);
            Assert.Equal(ResourceType.ExternalId, err.Resource);
            Assert.Equal(2, err2.Values.Count());
            Assert.Equal(ErrorType.ItemDuplicated, err2.Type);
            Assert.Equal(ResourceType.LegacyName, err2.Resource);
            Assert.Equal(6, result.Select(evt => evt.ExternalId).Distinct().Count());
            Assert.Equal(6, result.Select(evt => evt.LegacyName).Distinct().Count());
        }
        [Fact]
        public void TestSanitizeSequenceRequest()
        {
            var sequences = new[]
            {
                new SequenceCreate { ExternalId = "test1", Columns = new [] { new SequenceColumnWrite { ExternalId = "test1" } } },
                new SequenceCreate { ExternalId = "test2", Columns = new [] { new SequenceColumnWrite { ExternalId = "test1" } } },
                new SequenceCreate { ExternalId = "test3", Columns = new [] { new SequenceColumnWrite { ExternalId = "test1" } } },
                new SequenceCreate { ExternalId = "test3", Columns = new [] { new SequenceColumnWrite { ExternalId = "test2" } } },
                new SequenceCreate { ExternalId = "test4", Columns = new [] { new SequenceColumnWrite { ExternalId = "test1" } } },
                new SequenceCreate { ExternalId = "test4", Columns = new [] { new SequenceColumnWrite { ExternalId = "test2" } } },
                new SequenceCreate { ExternalId = "test5", Columns = null },
                new SequenceCreate { ExternalId = "test6", Columns = null },
                new SequenceCreate { ExternalId = "test7", Columns = new [] {
                    new SequenceColumnWrite { ExternalId = "test3" }, new SequenceColumnWrite { ExternalId = "test3" }
                } },
                new SequenceCreate { ExternalId = "test8", Columns = new [] {
                    new SequenceColumnWrite { ExternalId = "test4" }, new SequenceColumnWrite { ExternalId = "test4" }
                } },
            };
            var (result, errors) = Sanitation.CleanSequenceRequest(sequences, SanitationMode.Clean);
            var errs = errors.ToList();
            Assert.Equal(4, result.Count());
            Assert.Equal(3, errors.Count());

            var err = errs[0];
            Assert.Equal(ErrorType.ItemDuplicated, err.Type);
            Assert.Equal(ResourceType.ExternalId, err.Resource);
            Assert.Equal(2, err.Values.Count());

            err = errs[1];
            Assert.Equal(ErrorType.SanitationFailed, err.Type);
            Assert.Equal(ResourceType.SequenceColumns, err.Resource);
            Assert.Equal(2, err.Skipped.Count());

            err = errs[2];
            Assert.Equal(ErrorType.ItemDuplicated, err.Type);
            Assert.Equal(ResourceType.ColumnExternalId, err.Resource);
            Assert.Equal(2, err.Skipped.Count());
        }
        [Fact]
        public void TestSanitizeSequenceDataRequest()
        {
            var defCols = new[] { "test" };
            var defRows = new[] { new SequenceRow { RowNumber = 1, Values = new MultiValue[] { null } } };

            var sequences = new[]
            {
                // Duplicate externalId
                new SequenceDataCreate { ExternalId = "test1", Columns = defCols, Rows = defRows },
                new SequenceDataCreate { ExternalId = "test1", Columns = defCols, Rows = defRows },
                // Duplicate internalId
                new SequenceDataCreate { Id = 1, Columns = defCols, Rows = defRows },
                new SequenceDataCreate { Id = 1, Columns = defCols, Rows = defRows },
                // Null columns
                new SequenceDataCreate { ExternalId = "test2", Columns = null, Rows = defRows },
                // Null rows
                new SequenceDataCreate { ExternalId = "test3", Columns = defCols, Rows = null },
                // Duplicate row numbers
                new SequenceDataCreate { ExternalId = "test4", Columns = defCols, Rows = new []
                {
                    defRows[0], defRows[0]
                } },
                // Invalid row due to wrong number of fields
                new SequenceDataCreate { ExternalId = "test5", Columns = defCols, Rows = new []
                {
                    defRows[0], new SequenceRow { RowNumber = 2, Values = new MultiValue[] { null, null } }
                } },
                // Invalid row due to failed validation
                new SequenceDataCreate { ExternalId = "test6", Columns = defCols, Rows = new []
                {
                    defRows[0], new SequenceRow {RowNumber = 2, Values = new MultiValue[] { MultiValue.Create(double.NaN) }}
                } },
                // Invalid row due to bad row number
                new SequenceDataCreate { ExternalId = "test7", Columns = defCols, Rows = new []
                {
                    defRows[0], new SequenceRow {RowNumber = -50, Values = new MultiValue[] { null }}
                } },
                // All rows invalid
                new SequenceDataCreate { ExternalId = "test8", Columns = defCols, Rows = new []
                {
                    new SequenceRow {RowNumber = 2, Values = new MultiValue[] { MultiValue.Create(double.PositiveInfinity) }}
                } },
                // Duplicated columns
                new SequenceDataCreate { ExternalId = "test9", Columns = new [] { "test", "test", "test2" }, Rows = new []
                {
                    new SequenceRow { RowNumber = 1, Values = new MultiValue[] { null, null, null } }
                } }
            };

            var (result, errors) = Sanitation.CleanSequenceDataRequest(sequences, SanitationMode.Remove);
            Assert.Equal(6, result.Count());
            Assert.Equal(8, errors.Count());

            var errs = errors.ToList();

            var err = errs[4];
            Assert.Equal(ResourceType.SequenceRowNumber, err.Resource);
            Assert.Equal(ErrorType.ItemDuplicated, err.Type);
            Assert.Equal(409, err.Status);
            Assert.Single(err.Skipped);

            err = errs[3];
            Assert.Equal(ResourceType.ColumnExternalId, err.Resource);
            Assert.Equal(ErrorType.ItemDuplicated, err.Type);
            Assert.Equal(409, err.Status);
            Assert.Single(err.Skipped);

            err = errs[0];
            Assert.Equal(ResourceType.Id, err.Resource);
            Assert.Equal(ErrorType.ItemDuplicated, err.Type);
            Assert.Equal(409, err.Status);
            Assert.Equal(2, err.Values.Count());

            err = errs[1];
            Assert.Equal(ResourceType.SequenceColumns, err.Resource);
            Assert.Equal(ErrorType.SanitationFailed, err.Type);
            Assert.Single(err.Skipped);

            err = errs[2];
            Assert.Equal(ResourceType.SequenceRows, err.Resource);
            Assert.Equal(ErrorType.SanitationFailed, err.Type);
            Assert.Single(err.Skipped);

            err = errs[5];
            Assert.Equal(ResourceType.SequenceRows, err.Resource);
            Assert.Equal(ErrorType.SanitationFailed, err.Type);
            Assert.Single(err.Skipped);

            err = errs[6];
            Assert.Equal(ResourceType.SequenceRowValues, err.Resource);
            Assert.Equal(ErrorType.SanitationFailed, err.Type);
            Assert.Equal(3, err.Skipped.Count());

            err = errs[7];
            Assert.Equal(ResourceType.SequenceRowNumber, err.Resource);
            Assert.Equal(ErrorType.SanitationFailed, err.Type);
            Assert.Single(err.Skipped);
        }
        [Fact]
        public void SanitizeDataPointRequest()
        {
            var dps = new Dictionary<IIdentity, IEnumerable<Datapoint>>()
            {
                { Identity.Create("all-bad-ts"), new[] {
                    new Datapoint(DateTime.MaxValue, 1.0),
                    new Datapoint(DateTime.MinValue, 2.0)
                } },
                { Identity.Create("all-bad-value"), new[]
                {
                    new Datapoint(DateTime.UtcNow, double.NaN)
                } },
                { Identity.Create("all-bad-mixed"), new[]
                {
                    new Datapoint(DateTime.MaxValue, 1.0),
                    new Datapoint(DateTime.UtcNow, double.NaN)
                } },
                { Identity.Create("some-bad"), new[]
                {
                    new Datapoint(DateTime.UtcNow, double.NaN),
                    new Datapoint(DateTime.UtcNow, 2.0),
                    new Datapoint(DateTime.UtcNow, double.PositiveInfinity),
                    new Datapoint(DateTime.UtcNow, false, StatusCode.FromCategory(StatusCodeCategory.Good))
                } },
                { Identity.Create("all-good"), new[]
                {
                    new Datapoint(DateTime.UtcNow, "test"),
                    new Datapoint(DateTime.UtcNow, "test2"),
                    new Datapoint(DateTime.UtcNow, "test3")
                } }
            };

            var (result, errors) = Sanitation.CleanDataPointsRequest(dps, SanitationMode.Clean, null);
            Assert.Equal(2, result.Count());
            Assert.True(result.TryGetValue(Identity.Create("some-bad"), out var ts));
            Assert.Equal(3, ts.Count());
            Assert.True(result.TryGetValue(Identity.Create("all-good"), out ts));
            Assert.Equal(3, ts.Count());

            Assert.Equal(2, errors.Count());
            var err = errors.First(e => e.Resource == ResourceType.DataPointTimestamp);
            Assert.Equal(2, err.Skipped.Count());

            err = errors.First(e => e.Resource == ResourceType.DataPointValue);
            Assert.Equal(3, err.Skipped.Count());
        }
        [Fact]
        public void TestSanitizeAssetUpdateRequest()
        {
            var upd = new AssetUpdate { Name = new Update<string>("name") };
            var items = new[]
            {
                new AssetUpdateItem("test1") { Update = upd },
                new AssetUpdateItem("test1") { Update = upd },
                new AssetUpdateItem(123) { Update = upd },
                new AssetUpdateItem(123) { Update = upd },
                new AssetUpdateItem("test2") { Update = upd },
                new AssetUpdateItem("test3") { Update = new AssetUpdate {
                    ParentId = new Update<long?>(123), ParentExternalId = new Update<string>("test1") }}
            };
            var (result, errors) = Sanitation.CleanAssetUpdateRequest(items, SanitationMode.Clean);

            Assert.Equal(2, errors.Count());
            var err = errors.First();
            Assert.Equal(3, result.Count());
            Assert.Equal(2, err.Values.Count());
            Assert.Equal(ErrorType.ItemDuplicated, err.Type);
            Assert.Equal(ResourceType.Id, err.Resource);
            Assert.Equal(3, result.Select(evt => evt.ExternalId).Distinct().Count());

            err = errors.Last();
            Assert.Single(err.Skipped);
            Assert.Equal(ErrorType.SanitationFailed, err.Type);
            Assert.Equal(ResourceType.ParentId, err.Resource);
        }
        [Fact]
        public void TestSanitizeTimeSeriesUpdateRequest()
        {
            var upd = new TimeSeriesUpdate { Name = new UpdateNullable<string>("name") };

            var items = new[]
            {
                new TimeSeriesUpdateItem("test1") { Update = upd },
                new TimeSeriesUpdateItem("test1") { Update = upd },
                new TimeSeriesUpdateItem(123) { Update = upd },
                new TimeSeriesUpdateItem(123) { Update = upd },
                new TimeSeriesUpdateItem("test2") { Update = upd },
                new TimeSeriesUpdateItem("test3") { Update = new TimeSeriesUpdate() }
            };
            var (result, errors) = Sanitation.CleanTimeSeriesUpdateRequest(items, SanitationMode.Clean);

            Assert.Equal(2, errors.Count());
            var err = errors.First();
            Assert.Equal(3, result.Count());
            Assert.Equal(2, err.Values.Count());
            Assert.Equal(ErrorType.ItemDuplicated, err.Type);
            Assert.Equal(ResourceType.Id, err.Resource);
            Assert.Equal(3, result.Select(ts => ts.ExternalId).Distinct().Count());

            err = errors.Last();
            Assert.Single(err.Skipped);
            Assert.Equal(ErrorType.SanitationFailed, err.Type);
            Assert.Equal(ResourceType.Update, err.Resource);
        }
    }
}
