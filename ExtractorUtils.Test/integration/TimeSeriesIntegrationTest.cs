using Cognite.Extensions;
using Cognite.Extractor.Common;
using CogniteSdk;
using Com.Cognite.V1.Timeseries.Proto;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace ExtractorUtils.Test.Integration
{
    public class TimeSeriesIntegrationTest
    {
        private readonly ITestOutputHelper _output;
        public TimeSeriesIntegrationTest(ITestOutputHelper output)
        {
            _output = output;
        }
        [Theory]
        [InlineData(CogniteHost.GreenField)]
        [InlineData(CogniteHost.BlueField)]
        public async Task TestCreateTimeSeries(CogniteHost host)
        {
            using var tester = new CDFTester(host, _output);
            var ids = new[] {
                $"{tester.Prefix} ts-1",
                $"{tester.Prefix} ts-2",
                $"{tester.Prefix} ts-3",
                $"{tester.Prefix} ts-4",
                $"{tester.Prefix} ts-5",
            };

            var timeseries1 = new[]
            {
                new TimeSeriesCreate
                {
                    ExternalId = $"{tester.Prefix} ts-1"
                },
                new TimeSeriesCreate
                {
                    ExternalId = $"{tester.Prefix} ts-2"
                },
                new TimeSeriesCreate
                {
                    ExternalId = $"{tester.Prefix} ts-3"
                }
            };
            try
            {
                var result = await tester.Destination.EnsureTimeSeriesExistsAsync(timeseries1, RetryMode.None, SanitationMode.None, tester.Source.Token);
                Assert.True(result.IsAllGood);
                Assert.Equal(3, result.Results.Count());

                var timeseries2 = new[]
                {
                    timeseries1[0],
                    new TimeSeriesCreate
                    {
                        ExternalId = $"{tester.Prefix} ts-4"
                    }
                };

                result = await tester.Destination.EnsureTimeSeriesExistsAsync(timeseries2, RetryMode.OnError, SanitationMode.None, tester.Source.Token);

                Assert.Single(result.Errors);
                var error = result.Errors.First();
                Assert.Equal(ErrorType.ItemExists, error.Type);
                Assert.Equal(ResourceType.ExternalId, error.Resource);

                result = await tester.Destination.GetOrCreateTimeSeriesAsync(ids, toCreate =>
                {
                    Assert.Single(toCreate);
                    Assert.Equal($"{tester.Prefix} ts-5", toCreate.First());
                    return new[]
                    {
                        new TimeSeriesCreate
                        {
                            ExternalId = $"{tester.Prefix} ts-5"
                        }
                    };
                }, RetryMode.OnError, SanitationMode.None, tester.Source.Token);

                Assert.Equal(5, result.Results.Count());
                Assert.Equal(Enumerable.Range('1', 5), result.Results.Select(res => (int)res.ExternalId.Last()).OrderBy(v => v));
            }
            finally
            {
                await tester.Destination.CogniteClient.TimeSeries.DeleteAsync(new TimeSeriesDelete
                {
                    IgnoreUnknownIds = true,
                    Items = ids.Select(Identity.Create)
                }, tester.Source.Token);
            }
        }

        [Theory]
        [InlineData(CogniteHost.GreenField)]
        [InlineData(CogniteHost.BlueField)]
        public async Task TestSanitation(CogniteHost host)
        {
            using var tester = new CDFTester(host, _output);

            var timeseries = new[] {
                new TimeSeriesCreate
                {
                    ExternalId = tester.Prefix + new string('æ', 300),
                    Description = new string('æ', 1000),
                    Metadata = Enumerable.Range(0, 100)
                        .ToDictionary(i => $"key{i.ToString("000")}{new string('æ', 100)}", i => new string('æ', 200)),
                    Name = new string('ø', 1000),
                    LegacyName = tester.Prefix + new string('æ', 300),
                    Unit = new string('æ', 1000)
                },
                new TimeSeriesCreate
                {
                    Name = "test-duplicate-externalId",
                    ExternalId = $"{tester.Prefix} test-duplicate-externalId"
                },
                new TimeSeriesCreate
                {
                    Name = "test-duplicate-externalId",
                    ExternalId = $"{tester.Prefix} test-duplicate-externalId"
                }
            };

            try
            {
                var result = await tester.Destination.EnsureTimeSeriesExistsAsync(timeseries, RetryMode.OnError, SanitationMode.Clean, tester.Source.Token);

                Assert.Single(result.Errors);
                var error1 = result.Errors.First();
                Assert.Equal(ErrorType.ItemDuplicated, error1.Type);
                Assert.Equal(ResourceType.ExternalId, error1.Resource);
                Assert.Equal(2, result.Results.Count());
                Assert.Equal(tester.Prefix + new string('æ', 255 - tester.Prefix.Length), result.Results.First().ExternalId);
                Assert.Equal($"{tester.Prefix} test-duplicate-externalId", result.Results.Last().ExternalId);
            }
            finally
            {
                var ids = new[]
                {
                    tester.Prefix + new string('æ', 255 - tester.Prefix.Length),
                    $"{tester.Prefix} test-duplicate-externalId"
                };
                await tester.Destination.CogniteClient.TimeSeries.DeleteAsync(new TimeSeriesDelete
                {
                    IgnoreUnknownIds = true,
                    Items = ids.Select(Identity.Create)
                }, tester.Source.Token);
            }
        }

        [Theory]
        [InlineData(CogniteHost.GreenField)]
        [InlineData(CogniteHost.BlueField)]
        public async Task TestErrorHandling(CogniteHost host)
        {
            using var tester = new CDFTester(host, _output);

            await tester.Destination.EnsureTimeSeriesExistsAsync(new[]
            {
                new TimeSeriesCreate
                {
                    ExternalId = $"{tester.Prefix} existing-ts",
                    LegacyName = $"{tester.Prefix} existing-ts"
                }
            }, RetryMode.None, SanitationMode.None, tester.Source.Token);

            var timeseries = new[]
            {
                new TimeSeriesCreate
                {
                    ExternalId = $"{tester.Prefix} missing-asset-id-1",
                    AssetId = 123
                },
                new TimeSeriesCreate
                {
                    ExternalId = $"{tester.Prefix} missing-asset-id-2",
                    AssetId = 124
                },
                new TimeSeriesCreate
                {
                    ExternalId = $"{tester.Prefix} missing-asset-id-3",
                    AssetId = 124
                },
                new TimeSeriesCreate
                {
                    ExternalId = $"{tester.Prefix} missing-dataset-id",
                    DataSetId = 123
                },
                new TimeSeriesCreate
                {
                    ExternalId = $"{tester.Prefix} missing-dataset-id-2",
                    DataSetId = 124
                },
                new TimeSeriesCreate
                {
                    ExternalId = $"{tester.Prefix} existing-ts"
                },
                new TimeSeriesCreate
                {
                    ExternalId = $"{tester.Prefix} final-ts-ok"
                }
            };
            try
            {
                var result = await tester.Destination.EnsureTimeSeriesExistsAsync(timeseries, RetryMode.OnError, SanitationMode.Remove, tester.Source.Token);

                tester.Logger.LogResult(result, RequestType.CreateTimeSeries, false);

                Assert.Single(result.Results);
                Assert.Equal(3, result.Errors.Count());
                Assert.Equal($"{tester.Prefix} final-ts-ok", result.Results.First().ExternalId);
                foreach (var error in result.Errors)
                {
                    switch (error.Resource)
                    {
                        case ResourceType.AssetId:
                            Assert.Equal(ErrorType.ItemMissing, error.Type);
                            Assert.Equal(2, error.Values.Count());
                            Assert.Contains(error.Values, idt => idt.Id == 123);
                            Assert.Contains(error.Values, idt => idt.Id == 124);
                            Assert.Equal(3, error.Skipped.Count());
                            break;
                        case ResourceType.DataSetId:
                            Assert.Equal(ErrorType.ItemMissing, error.Type);
                            Assert.Equal(2, error.Values.Count());
                            Assert.Contains(error.Values, idt => idt.Id == 123);
                            Assert.Contains(error.Values, idt => idt.Id == 124);
                            Assert.Equal(2, error.Skipped.Count());
                            break;
                        case ResourceType.ExternalId:
                            Assert.Equal(ErrorType.ItemExists, error.Type);
                            Assert.Single(error.Values);
                            Assert.Equal($"{tester.Prefix} existing-ts", error.Values.First().ExternalId);
                            Assert.Single(error.Skipped);
                            break;
                        default:
                            throw new Exception($"Bad resource type: {error.Type}", error.Exception);
                    }
                }
            }
            finally
            {
                var ids = new[]
                {
                    $"{tester.Prefix} existing-ts",
                    $"{tester.Prefix} missing-asset-id-1",
                    $"{tester.Prefix} missing-asset-id-2",
                    $"{tester.Prefix} missing-asset-id-3",
                    $"{tester.Prefix} missing-dataset-id",
                    $"{tester.Prefix} missing-dataset-id-2",
                    $"{tester.Prefix} final-ts-ok"
                };
                await tester.Destination.CogniteClient.TimeSeries.DeleteAsync(new TimeSeriesDelete
                {
                    IgnoreUnknownIds = true,
                    Items = ids.Select(Identity.Create)
                });
            }
        }

        [Theory]
        [InlineData(CogniteHost.GreenField)]
        [InlineData(CogniteHost.BlueField)]
        public async Task TestUpdateTimeSeries(CogniteHost host)
        {
            using var tester = new CDFTester(host, _output);

            var tss = await CreateTestTimeSeries(tester);

            var upd = new TimeSeriesUpdate { Description = new UpdateNullable<string>("new description") };

            var updates = new[]
            {
                new TimeSeriesUpdateItem(tss[0].extId) { Update = upd },
                new TimeSeriesUpdateItem(tss[1].id) { Update = upd },
                new TimeSeriesUpdateItem(tss[2].extId) { Update = upd }
            };

            var json = System.Text.Json.JsonSerializer.Serialize(updates, Oryx.Cognite.Common.jsonOptions);


            try
            {
                var result = await tester.Destination.UpdateTimeSeriesAsync(updates, RetryMode.None, SanitationMode.None, tester.Source.Token);
                result.Throw();

                Assert.Equal(3, result.Results.Count());
                Assert.True(result.IsAllGood);

                Assert.All(result.Results, ts => Assert.Equal("new description", ts.Description));
            }
            finally
            {
                await tester.Destination.CogniteClient.TimeSeries.DeleteAsync(new TimeSeriesDelete
                {
                    IgnoreUnknownIds = true,
                    Items = tss.Select(pair => Identity.Create(pair.id))
                });
            }
        }
        [Theory]
        [InlineData(CogniteHost.GreenField)]
        [InlineData(CogniteHost.BlueField)]
        public async Task TestUpdateSanitation(CogniteHost host)
        {
            using var tester = new CDFTester(host, _output);

            var tss = await CreateTestTimeSeries(tester);

            var meta = Enumerable.Range(0, 100)
                    .ToDictionary(i => $"key{i.ToString("000")}{new string('æ', 100)}", i => new string('æ', 200));

            var updates = new[]
            {
                new TimeSeriesUpdateItem(tss[0].extId)
                {
                    Update = new TimeSeriesUpdate
                    {
                        ExternalId = new UpdateNullable<string>(tester.Prefix + new string('æ', 300)),
                        DataSetId = new UpdateNullable<long?>(-123),
                        Description = new UpdateNullable<string>(new string('æ', 2000)),
                        AssetId = new UpdateNullable<long?>(-123),
                        Metadata = new UpdateDictionary<string>(meta),
                        Name = new UpdateNullable<string>(new string('æ', 300)),
                        Unit = new UpdateNullable<string>(new string('æ', 200))
                    }
                },
                new TimeSeriesUpdateItem(tss[1].extId)
                {
                    Update = new TimeSeriesUpdate { Name = new UpdateNullable<string>("name") }
                },
                new TimeSeriesUpdateItem(tss[1].extId)
                {
                    Update = new TimeSeriesUpdate { Name = new UpdateNullable<string>("name") }
                }
            };

            try
            {
                var result = await tester.Destination.UpdateTimeSeriesAsync(updates, RetryMode.OnError, SanitationMode.Clean, tester.Source.Token);
                result.ThrowOnFatal();

                Assert.Equal(2, result.Results.Count());

                var errs = result.Errors.ToArray();
                Assert.Single(errs);

                Assert.Equal(ErrorType.ItemDuplicated, errs[0].Type);
                Assert.Equal(ResourceType.Id, errs[0].Resource);
                Assert.Single(errs[0].Values);
            }
            finally
            {
                await tester.Destination.CogniteClient.TimeSeries.DeleteAsync(new TimeSeriesDelete
                {
                    IgnoreUnknownIds = true,
                    Items = tss.Select(pair => Identity.Create(pair.id))
                });
            }
        }
        [Theory]
        [InlineData(CogniteHost.GreenField)]
        [InlineData(CogniteHost.BlueField)]
        public async Task TestUpdateErrorHandling(CogniteHost host)
        {
            using var tester = new CDFTester(host, _output);

            var tss = await CreateTestTimeSeries(tester);

            var upd = new TimeSeriesUpdate { Description = new UpdateNullable<string>("new description") };

            var updates1 = new[]
            {
                // Existing timeseries
                new TimeSeriesUpdateItem(tss[0].extId)
                {
                    Update = new TimeSeriesUpdate { ExternalId = new UpdateNullable<string>(tss[1].extId) }
                },
                // Update OK
                new TimeSeriesUpdateItem(tss[1].extId)
                {
                    Update = upd
                },
                // Missing asset
                new TimeSeriesUpdateItem(tss[2].extId)
                {
                    Update = new TimeSeriesUpdate { AssetId = new UpdateNullable<long?>(123) }
                }
            };
            var updates2 = new[]
            {
                // Update OK
                new TimeSeriesUpdateItem(tss[0].extId)
                {
                    Update = upd
                },
                // Missing dataset
                new TimeSeriesUpdateItem(tss[1].extId)
                {
                    Update = new TimeSeriesUpdate { DataSetId = new UpdateNullable<long?>(123) }
                }
            };
            var updates3 = new[]
            {
                // Missing by internal id
                new TimeSeriesUpdateItem(123)
                {
                    Update = upd
                },
                // Missing by external id
                new TimeSeriesUpdateItem("missing-ts")
                {
                    Update = upd
                },
                // Update OK
                new TimeSeriesUpdateItem(tss[2].extId)
                {
                    Update = upd
                }
            };

            try
            {
                var result1 = await tester.Destination.UpdateTimeSeriesAsync(updates1, RetryMode.OnError, SanitationMode.None, tester.Source.Token);
                result1.ThrowOnFatal();
                var result2 = await tester.Destination.UpdateTimeSeriesAsync(updates2, RetryMode.OnError, SanitationMode.None, tester.Source.Token);
                result2.ThrowOnFatal();
                var result3 = await tester.Destination.UpdateTimeSeriesAsync(updates3, RetryMode.OnError, SanitationMode.None, tester.Source.Token);
                result3.ThrowOnFatal();

                var errs = result1.Errors.ToArray();
                Assert.Equal(2, errs.Length);

                var err = errs.First(e => e.Type == ErrorType.ItemExists && e.Resource == ResourceType.ExternalId);
                Assert.Single(err.Skipped);
                Assert.Contains(err.Values, e => e.ExternalId == tss[1].extId);

                err = errs.First(e => e.Type == ErrorType.ItemMissing && e.Resource == ResourceType.AssetId);
                Assert.Single(err.Skipped);
                Assert.Contains(err.Values, e => e.Id == 123);

                errs = result2.Errors.ToArray();
                Assert.Single(errs);

                err = errs.First(e => e.Type == ErrorType.ItemMissing && e.Resource == ResourceType.DataSetId);
                Assert.Single(err.Skipped);
                Assert.Contains(err.Values, e => e.Id == 123);

                errs = result3.Errors.ToArray();
                Assert.Single(errs);

                err = errs.First(e => e.Type == ErrorType.ItemMissing && e.Resource == ResourceType.Id);
                Assert.Equal(2, err.Skipped.Count());
                Assert.Contains(err.Values, e => e.Id == 123);
                Assert.Contains(err.Values, e => e.ExternalId == "missing-ts");
            }
            finally
            {
                await tester.Destination.CogniteClient.TimeSeries.DeleteAsync(new TimeSeriesDelete
                {
                    IgnoreUnknownIds = true,
                    Items = tss.Select(pair => Identity.Create(pair.id))
                });
            }
        }

        [Theory]
        [InlineData(CogniteHost.BlueField)]
        public async Task TestUploadQueue(CogniteHost host)
        {
            using var tester = new CDFTester(host, _output);

            var timeseries = new[]
            {
                new TimeSeriesCreate
                {
                    ExternalId = $"{tester.Prefix} dp-ts-1"
                },
                new TimeSeriesCreate
                {
                    ExternalId = $"{tester.Prefix} dp-ts-2"
                },
                new TimeSeriesCreate
                {
                    ExternalId = $"{tester.Prefix} dp-ts-3",
                }
            };
            int dpCount = 0;
            int cbCount = 0;
            var startTime = DateTime.UtcNow;
            try
            {
                var result = await tester.Destination.EnsureTimeSeriesExistsAsync(timeseries, RetryMode.OnError, SanitationMode.Remove, tester.Source.Token);
                tester.Logger.LogResult(result, RequestType.CreateTimeSeries, false);

                await using (var queue = tester.Destination.CreateTimeSeriesUploadQueue(TimeSpan.FromSeconds(1), 0, res =>
                {
                    dpCount += res.Uploaded?.Count() ?? 0;
                    cbCount++;
                    tester.Logger.LogInformation("Sent {Num} data points to CDF", dpCount);
                    return Task.CompletedTask;
                }))
                {
                    var enqueueTask = Task.Run(async () =>
                    {
                        for (int i = 0; i < 20; ++i)
                        {
                            queue.Enqueue(timeseries[0].ExternalId, new Datapoint(DateTime.UtcNow, i));
                            queue.Enqueue(timeseries[1].ExternalId, new Datapoint(DateTime.UtcNow, i));
                            queue.Enqueue(timeseries[2].ExternalId, new Datapoint(DateTime.UtcNow, i));
                            await Task.Delay(100, tester.Source.Token);
                        }
                    });
                    var uploadTask = queue.Start(tester.Source.Token);

                    var t = Task.WhenAny(uploadTask, enqueueTask);
                    await t;
                    tester.Logger.LogInformation("Enqueueing task completed. Disposing of the upload queue");
                }
                Assert.Equal(3 * 20, dpCount);
                Assert.True(cbCount > 1);

                var query = new List<DataPointsQueryItem>();
                foreach (var ts in timeseries)
                {
                    var queryItem = new DataPointsQueryItem()
                    {
                        ExternalId = ts.ExternalId,
                        Limit = 100
                    };
                    query.Add(queryItem);
                }
                var dataPointsQuery = new DataPointsQuery()
                {
                    Items = query
                };

                DataPointListResponse dpsFromCDF = null;

                for (int i = 0; i < 20; i++)
                {
                    dpsFromCDF = await tester.Destination.CogniteClient.DataPoints.ListAsync(dataPointsQuery, tester.Source.Token);
                    if (dpsFromCDF.Items.All(item => (item.NumericDatapoints?.Datapoints?.Count() ?? 0) == 20)) break;
                    await Task.Delay(1000);
                }

                Assert.Equal(3, dpsFromCDF.Items.Count);
                foreach (var item in dpsFromCDF.Items)
                {
                    Assert.NotNull(item.NumericDatapoints);
                    Assert.Null(item.StringDatapoints);
                    Assert.Equal(20, item.NumericDatapoints.Datapoints.Count);
                }

            }
            finally
            {
                var ids = new[]
                {
                    timeseries[0].ExternalId,
                    timeseries[1].ExternalId,
                    timeseries[2].ExternalId
                };
                await tester.Destination.CogniteClient.TimeSeries.DeleteAsync(new TimeSeriesDelete
                {
                    IgnoreUnknownIds = true,
                    Items = ids.Select(Identity.Create)
                });
            }
        }

        private async Task<(string extId, long id)[]> CreateTestTimeSeries(CDFTester tester)
        {
            var timeseries = new[]
            {
                new TimeSeriesCreate
                {
                    Name = "utils-test-ts-1",
                    ExternalId = $"{tester.Prefix} utils-test-ts-1"
                },
                new TimeSeriesCreate
                {
                    Name = "utils-test-ts-2",
                    ExternalId = $"{tester.Prefix} utils-test-ts-2",
                    IsString = true
                },
                new TimeSeriesCreate
                {
                    Name = "utils-test-ts-3",
                    ExternalId = $"{tester.Prefix} utils-test-ts-3"
                }
            };

            var result = await tester.Destination.EnsureTimeSeriesExistsAsync(timeseries, RetryMode.None, SanitationMode.None, tester.Source.Token);
            return result.Results.Select(ts => (ts.ExternalId, ts.Id)).ToArray();
        }


        [Theory]
        [InlineData(CogniteHost.GreenField)]
        [InlineData(CogniteHost.BlueField)]
        public async Task TestCreateDataPoints(CogniteHost host)
        {
            using var tester = new CDFTester(host, _output);

            var tss = await CreateTestTimeSeries(tester);

            var dps = new Dictionary<Identity, IEnumerable<Datapoint>>()
            {
                { Identity.Create(tss[0].extId), Enumerable.Range(0, 10)
                    .Select(i => new Datapoint(DateTime.UtcNow.AddSeconds(i), i)).ToList() },
                { Identity.Create(tss[1].extId), Enumerable.Range(0, 10)
                    .Select(i => new Datapoint(DateTime.UtcNow.AddSeconds(i), $"value{i}")).ToList() },
                { Identity.Create(tss[2].extId), Enumerable.Range(0, 10)
                    .Select(i => new Datapoint(DateTime.UtcNow.AddSeconds(i), i)).ToList() }
            };

            tester.Config.Cognite.CdfChunking.DataPointTimeSeries = 2;
            tester.Config.Cognite.CdfChunking.DataPoints = 5;

            try
            {
                var result = await tester.Destination.InsertDataPointsAsync(dps, SanitationMode.None, RetryMode.None, tester.Source.Token);
                Assert.Empty(result.Errors);

                int[] counts = new int[3];
                for (int i = 0; i < 10; i++)
                {
                    var foundDps = await tester.Destination.CogniteClient.DataPoints.ListAsync(new DataPointsQuery
                    {
                        Items = tss.Select(ts => new DataPointsQueryItem
                        {
                            ExternalId = ts.extId
                        }).ToArray()
                    });
                    if (foundDps.Items.Count() == 3)
                    {
                        counts[0] = foundDps.Items[0]?.NumericDatapoints?.Datapoints?.Count() ?? 0;
                        counts[1] = foundDps.Items[1]?.StringDatapoints?.Datapoints?.Count() ?? 0;
                        counts[2] = foundDps.Items[2]?.NumericDatapoints?.Datapoints?.Count() ?? 0;
                        if (counts.All(cnt => cnt == 10)) break;
                    }
                    await Task.Delay(1000);
                }
                Assert.All(counts, cnt => Assert.Equal(10, cnt));
            }
            finally
            {
                await tester.Destination.CogniteClient.TimeSeries.DeleteAsync(new TimeSeriesDelete
                {
                    IgnoreUnknownIds = true,
                    Items = tss.Select(ts => Identity.Create(ts.extId))
                });
            }
        }
        [Theory]
        [InlineData(CogniteHost.GreenField)]
        [InlineData(CogniteHost.BlueField)]
        public async Task TestDataPointsSanitation(CogniteHost host)
        {
            using var tester = new CDFTester(host, _output);

            var tss = await CreateTestTimeSeries(tester);

            Dictionary<Identity, IEnumerable<Datapoint>> GetCreates()
            {
                return new Dictionary<Identity, IEnumerable<Datapoint>>()
                {
                    { Identity.Create(tss[0].extId), new []
                    {
                        new Datapoint(DateTime.UtcNow, 1.0),
                        new Datapoint(DateTime.MaxValue, 2.0),
                        new Datapoint(DateTime.UtcNow.AddSeconds(1), double.NaN),
                        new Datapoint(DateTime.MinValue, 3.0),
                        new Datapoint(DateTime.UtcNow.AddSeconds(2), double.PositiveInfinity),
                        new Datapoint(DateTime.UtcNow.AddSeconds(3), double.NegativeInfinity),
                        new Datapoint(DateTime.UtcNow.AddSeconds(4), 1E101),
                        new Datapoint(DateTime.UtcNow.AddSeconds(5), -1E101),
                    } },
                    { Identity.Create(tss[1].extId), new []
                    {
                        new Datapoint(DateTime.UtcNow, new string('æ', 400)),
                        new Datapoint(DateTime.UtcNow.AddSeconds(1), "test"),
                        new Datapoint(DateTime.UtcNow, null)
                    } },
                    { Identity.Create(tss[2].id), new[]
                    {
                        new Datapoint(DateTime.UtcNow, double.NaN)
                    } }
                };
            }

            try
            {
                var result = await tester.Destination.InsertDataPointsAsync(GetCreates(), SanitationMode.Remove, RetryMode.None, tester.Source.Token);

                var errs = result.Errors.ToArray();
                Assert.Equal(2, errs.Length);

                var err = errs[0];
                Assert.Equal(ResourceType.DataPointTimestamp, err.Resource);
                Assert.Equal(ErrorType.SanitationFailed, err.Type);
                Assert.Single(err.Skipped);
                var iErr = err.Skipped.OfType<DataPointInsertError>().First();
                Assert.Equal(2, iErr.DataPoints.Count());
                Assert.Equal(tss[0].extId, iErr.Id.ExternalId);

                err = errs[1];
                Assert.Equal(ResourceType.DataPointValue, err.Resource);
                Assert.Equal(ErrorType.SanitationFailed, err.Type);
                Assert.Equal(3, err.Skipped.Count());
                var insertErrs = err.Skipped.OfType<DataPointInsertError>().ToArray();

                iErr = insertErrs[0];
                Assert.Equal(5, iErr.DataPoints.Count());
                Assert.Equal(tss[0].extId, iErr.Id.ExternalId);
                iErr = insertErrs[1];
                Assert.Equal(2, iErr.DataPoints.Count());
                Assert.Equal(tss[1].extId, iErr.Id.ExternalId);
                iErr = insertErrs[2];
                Assert.Single(iErr.DataPoints);
                Assert.Equal(tss[2].id, iErr.Id.Id);

                tester.Config.Cognite.NanReplacement = 123;

                result = await tester.Destination.InsertDataPointsAsync(GetCreates(), SanitationMode.Clean, RetryMode.None, tester.Source.Token);

                errs = result.Errors.ToArray();
                Assert.Single(errs);

                err = errs[0];
                Assert.Equal(ResourceType.DataPointTimestamp, err.Resource);
                Assert.Equal(ErrorType.SanitationFailed, err.Type);
                Assert.Single(err.Skipped);
                iErr = err.Skipped.OfType<DataPointInsertError>().First();
                Assert.Equal(2, iErr.DataPoints.Count());
                Assert.Equal(tss[0].extId, iErr.Id.ExternalId);
            }
            finally
            {
                await tester.Destination.CogniteClient.TimeSeries.DeleteAsync(new TimeSeriesDelete
                {
                    IgnoreUnknownIds = true,
                    Items = tss.Select(ts => Identity.Create(ts.extId))
                });
            }
        }
        [Theory]
        [InlineData(CogniteHost.GreenField)]
        [InlineData(CogniteHost.BlueField)]
        public async Task TestDataPointsErrorHandling(CogniteHost host)
        {
            using var tester = new CDFTester(host, _output);

            var tss = await CreateTestTimeSeries(tester);

            var dps = new Dictionary<Identity, IEnumerable<Datapoint>>()
            {
                // All mismatched
                { Identity.Create(tss[0].extId), new [] {
                    new Datapoint(DateTime.UtcNow, "test"),
                    new Datapoint(DateTime.UtcNow.AddSeconds(1), "test2")
                } },
                // Some mismatched datapoints
                { Identity.Create(tss[1].extId), new[]
                {
                    new Datapoint(DateTime.UtcNow, 1.0),
                    new Datapoint(DateTime.UtcNow.AddSeconds(1), 2.0),
                    new Datapoint(DateTime.UtcNow.AddSeconds(2), "test")
                } },
                { Identity.Create(tss[2].id), new[]
                {
                    new Datapoint(DateTime.UtcNow, 1.0),
                    new Datapoint(DateTime.UtcNow.AddSeconds(1), "test"),
                    new Datapoint(DateTime.UtcNow.AddSeconds(2), "test2")
                } },
                { Identity.Create("missing-ts-1"), new[] { new Datapoint(DateTime.UtcNow, 1.0) } },
                { Identity.Create(1), new[] { new Datapoint(DateTime.UtcNow, "test") } }
            };

            try
            {
                var result = await tester.Destination.InsertDataPointsAsync(dps, SanitationMode.None, RetryMode.OnError, tester.Source.Token);

                var errs = result.Errors.ToArray();
                // Greenfield reports missing twice, once for each id type.
                CogniteError<DataPointInsertError> err;

                Assert.Equal(2, errs.Length);
                err = errs[0];
                Assert.Equal(ResourceType.Id, err.Resource);
                Assert.Equal(ErrorType.ItemMissing, err.Type);
                Assert.Equal(2, err.Values.Count());
                err = errs[1];

                Assert.Equal(ResourceType.DataPointValue, err.Resource);
                Assert.Equal(ErrorType.MismatchedType, err.Type);
                Assert.Equal(3, err.Skipped.Count());
                var insertErrs = err.Skipped.ToArray();
                Assert.Equal(2, insertErrs[0].DataPoints.Count());
                Assert.Equal(2, insertErrs[1].DataPoints.Count());
                Assert.Equal(2, insertErrs[2].DataPoints.Count());
            }
            finally
            {
                await tester.Destination.CogniteClient.TimeSeries.DeleteAsync(new TimeSeriesDelete
                {
                    IgnoreUnknownIds = true,
                    Items = tss.Select(ts => Identity.Create(ts.extId))
                });
            }
        }
        [Theory]
        [InlineData(CogniteHost.GreenField)]
        [InlineData(CogniteHost.BlueField)]
        public async Task TestDataPointsCreateMissing(CogniteHost host)
        {
            using var tester = new CDFTester(host, _output);

            var tss = await CreateTestTimeSeries(tester);

            var dps = new Dictionary<Identity, IEnumerable<Datapoint>>()
            {
                { Identity.Create(tss[0].id), new[]
                {
                    new Datapoint(DateTime.UtcNow, 1.0),
                    new Datapoint(DateTime.UtcNow.AddSeconds(1), 2.0),
                    new Datapoint(DateTime.UtcNow.AddSeconds(2), 3.0)
                } },
                { Identity.Create($"{tester.Prefix} utils-test-ts-missing-1"), new[] { new Datapoint(DateTime.UtcNow, 1.0) } },
                { Identity.Create($"{tester.Prefix} utils-test-ts-missing-2"), new[] { new Datapoint(DateTime.UtcNow, "test") } },
                { Identity.Create(1), new[] { new Datapoint(DateTime.UtcNow, 1.0) } }
            };

            try
            {
                var (dpResult, tsResult) = await tester.Destination
                    .InsertDataPointsCreateMissingAsync(dps, SanitationMode.Clean, RetryMode.OnError, null, tester.Source.Token);

                Assert.Single(dpResult.Errors);
                var err = dpResult.Errors.First();
                Assert.Equal(ErrorType.ItemMissing, err.Type);

                Assert.True(tsResult.Errors == null || !tsResult.Errors.Any());
                Assert.Equal(2, tsResult.Results.Count());
            }
            finally
            {
                await tester.Destination.CogniteClient.TimeSeries.DeleteAsync(new TimeSeriesDelete
                {
                    IgnoreUnknownIds = true,
                    Items = tss
                        .Select(ts => ts.extId)
                        .Concat(new[] { $"{tester.Prefix} utils-test-ts-missing-1", $"{tester.Prefix} utils-test-ts-missing-2" })
                        .Select(Identity.Create)
                });
            }

        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task TestUpsert(bool replaceMeta)
        {
            using var tester = new CDFTester(CogniteHost.GreenField, _output);

            var upserts = Enumerable.Range(1, 5).Select(i => new TimeSeriesCreate
            {
                ExternalId = $"{tester.Prefix} test-upsert-{i}",
                Name = $"test-upsert-{i}"
            }).ToArray();

            var options = new UpsertParams { ReplaceMetadata = replaceMeta, SetNull = true };

            try
            {
                // Just create
                var result1 = await tester.Destination.UpsertTimeSeriesAsync(upserts, RetryMode.OnError,
                    SanitationMode.Remove, options, tester.Source.Token);
                // Just retrieve
                var result2 = await tester.Destination.UpsertTimeSeriesAsync(upserts, RetryMode.OnError,
                    SanitationMode.Remove, options, tester.Source.Token);
                // Update all
                foreach (var ups in upserts)
                {
                    ups.Description = "Some description";
                    ups.Metadata = new Dictionary<string, string>
                    {
                        { "someKey", "someValue" }
                    };
                }
                var result3 = await tester.Destination.UpsertTimeSeriesAsync(upserts, RetryMode.OnError,
                    SanitationMode.Remove, options, tester.Source.Token);
                // Update all and add 2 more

                foreach (var ups in upserts)
                {
                    ups.Metadata = new Dictionary<string, string>
                    {
                        { "someKey2", "someValue2" }
                    };
                }

                upserts = upserts.Concat(Enumerable.Range(6, 2).Select(i => new TimeSeriesCreate
                {
                    ExternalId = $"{tester.Prefix} test-upsert-{i}",
                    Name = $"test-upsert-{i}"
                })).ToArray();

                var result4 = await tester.Destination.UpsertTimeSeriesAsync(upserts, RetryMode.OnError,
                    SanitationMode.Remove, options, tester.Source.Token);

                // Update all, fail to create 2 more

                foreach (var ups in upserts)
                {
                    ups.Unit = "Some unit";
                }

                upserts = upserts.Concat(Enumerable.Range(8, 2).Select(i => new TimeSeriesCreate
                {
                    ExternalId = $"{tester.Prefix} test-upsert-{i}",
                    AssetId = 123
                })).ToArray();

                var result5 = await tester.Destination.UpsertTimeSeriesAsync(upserts, RetryMode.OnError,
                    SanitationMode.Remove, options, tester.Source.Token);


                Assert.Equal(5, result1.Results.Count());
                result1.Throw();
                Assert.Equal(5, result2.Results.Count());
                result2.Throw();
                Assert.Equal(5, result3.Results.Count());
                result3.Throw();
                Assert.All(result3.Results, res => {
                    Assert.Single(res.Metadata);
                    Assert.Equal("someValue", res.Metadata["someKey"]);
                    Assert.Equal("Some description", res.Description);
                });
                Assert.Equal(7, result4.Results.Count());
                result4.Throw();
                Assert.All(result4.Results.Take(5), res => {
                    if (replaceMeta)
                    {
                        Assert.Single(res.Metadata);
                        Assert.Equal("someValue2", res.Metadata["someKey2"]);
                    }
                    else
                    {
                        Assert.Equal(2, res.Metadata.Count);
                        Assert.Equal("someValue", res.Metadata["someKey"]);
                        Assert.Equal("someValue2", res.Metadata["someKey2"]);
                    }
                });
                Assert.Equal(7, result5.Results.Count());
                Assert.Single(result5.Errors);
                Assert.Equal(2, result5.Errors.First().Skipped.Count());
                Assert.All(result5.Results, res => {
                    Assert.Equal("Some unit", res.Unit);
                });
            }
            finally
            {
                var ids = Enumerable.Range(1, 9)
                    .Select(i => Identity.Create($"{tester.Prefix} test-upsert-{i}"))
                    .ToList();
                await tester.Destination.CogniteClient.TimeSeries.DeleteAsync(new TimeSeriesDelete
                {
                    IgnoreUnknownIds = true,
                    Items = ids
                });
            }
        }


    }
}
