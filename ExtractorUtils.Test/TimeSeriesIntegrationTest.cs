using Cognite.Extensions;
using CogniteSdk;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace ExtractorUtils.Test
{
    public class TimeSeriesIntegrationTest
    {
        private string[] lines = {
            "version: 2",
            "logger:",
            "  console:",
            "    level: verbose",
            "cognite:",
            "  project: ${TEST_PROJECT}",
            "  api-key: ${TEST_API_KEY}",
            "  host: ${TEST_HOST}",
            "  cdf-chunking:",
            "    time-series: 20",
            "  cdf-throttling:",
            "    time-series: 2" };

        [Fact]
        public async Task TestCreateTimeSeries()
        {
            using var tester = new CDFTester(lines);
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
        [Fact]
        public async Task TestSanitation()
        {
            using var tester = new CDFTester(lines);

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
        [Fact]
        public async Task TestErrorHandling()
        {
            using var tester = new CDFTester(lines);

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
                    ExternalId = $"{tester.Prefix} duplicated-legacyname",
                    LegacyName = $"{tester.Prefix} existing-ts"
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
                Assert.Equal(4, result.Errors.Count());
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
                        case ResourceType.LegacyName:
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
                    $"{tester.Prefix} duplicated-legacyname",
                    $"{tester.Prefix} final-ts-ok"
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
