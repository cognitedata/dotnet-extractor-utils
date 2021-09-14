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

namespace ExtractorUtils.Test.Integration
{
    public class TimeSeriesIntegrationTest
    {

        [Theory]
        [InlineData(CogniteHost.GreenField)]
        [InlineData(CogniteHost.BlueField)]
        public async Task TestCreateTimeSeries(CogniteHost host)
        {
            using var tester = new CDFTester(host);
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
            using var tester = new CDFTester(host);

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
            using var tester = new CDFTester(host);

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
        public async Task TestUploadQueue(CogniteHost host)
        {
            using var tester = new CDFTester(host);

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

                using (var queue = tester.Destination.CreateTimeSeriesUploadQueue(TimeSpan.FromSeconds(1), 0, res =>
                {
                    dpCount += res.Uploaded?.Count() ?? 0;
                    cbCount++;
                    tester.Logger.LogInformation("Sent {Num} data points to CDF", dpCount);
                    return Task.CompletedTask;
                }))
                {
                    var enqueueTask = Task.Run(async () => {
                        for (int i =0; i < 20; ++i)
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
                        Start = $"{startTime.ToUnixTimeMilliseconds()}",
                        End = $"{DateTime.UtcNow.ToUnixTimeMilliseconds() + 1}",
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
    }
}
