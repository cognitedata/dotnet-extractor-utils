﻿using Cognite.Extensions;
using CogniteSdk;
using System;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace ExtractorUtils.Test
{
    public class AssetIntegrationTest
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
            "    assets: 20",
            "  cdf-throttling:",
            "    assets: 2" };
        // Basic usage of ensure and GetOrCreate
        [Fact]
        public async Task TestCreateAssets()
        {
            using var tester = new CDFTester(lines);
            var ids = new[] {
                $"{tester.Prefix} asset-1",
                $"{tester.Prefix} asset-2",
                $"{tester.Prefix} asset-3",
                $"{tester.Prefix} asset-4",
                $"{tester.Prefix} asset-5",
            };

            var assets1 = new[]
            {
                new AssetCreate
                {
                    Name = $"{tester.Prefix} asset-1",
                    ExternalId = $"{tester.Prefix} asset-1"
                },
                new AssetCreate
                {
                    Name = $"{tester.Prefix} asset-2",
                    ExternalId = $"{tester.Prefix} asset-2"
                },
                new AssetCreate
                {
                    Name = $"{tester.Prefix} asset-3",
                    ExternalId = $"{tester.Prefix} asset-3"
                }
            };
            try
            {
                var result = await tester.Destination.EnsureAssetsExistsAsync(assets1, RetryMode.None, SanitationMode.None, tester.Source.Token);
                Assert.True(result.IsAllGood);
                Assert.Equal(3, result.Results.Count());

                var assets2 = new[]
                {
                    assets1[0],
                    new AssetCreate
                    {
                        Name = $"{tester.Prefix} asset-4",
                        ExternalId = $"{tester.Prefix} asset-4"
                    }
                };

                result = await tester.Destination.EnsureAssetsExistsAsync(assets2, RetryMode.OnError, SanitationMode.None, tester.Source.Token);

                Assert.Single(result.Errors);
                var error = result.Errors.First();
                Assert.Equal(ErrorType.ItemExists, error.Type);
                Assert.Equal(ResourceType.ExternalId, error.Resource);

                result = await tester.Destination.GetOrCreateAssetsAsync(ids, toCreate =>
                {
                    Assert.Single(toCreate);
                    Assert.Equal($"{tester.Prefix} asset-5", toCreate.First());
                    return new[]
                    {
                        new AssetCreate
                        {
                            Name = $"{tester.Prefix} asset-5",
                            ExternalId = $"{tester.Prefix} asset-5"
                        }
                    };
                }, RetryMode.OnError, SanitationMode.None, tester.Source.Token);

                Assert.Equal(5, result.Results.Count());
                Assert.Equal(Enumerable.Range('1', 5), result.Results.Select(res => (int)res.ExternalId.Last()).OrderBy(v => v));
            }
            finally
            {
                await tester.Destination.CogniteClient.Assets.DeleteAsync(new AssetDelete
                {
                    IgnoreUnknownIds = true,
                    Items = ids.Select(Identity.Create)
                }, tester.Source.Token);
            }
        }
        // This is just for testing that the sanitation conforms with CDF limits in the places where
        // it is reasonable to test.
        [Fact]
        public async Task TestSanitation()
        {
            using var tester = new CDFTester(lines);
            
            var assets = new[] {
                new AssetCreate
                {
                    ExternalId = tester.Prefix + new string('æ', 300),
                    Description = new string('æ', 1000),
                    Metadata = Enumerable.Range(0, 100)
                        .ToDictionary(i => $"key{i.ToString("000")}{new string('æ', 100)}", i => new string('æ', 200)),
                    Name = new string('ø', 1000),
                    Source = new string('æ', 12345)
                },
                new AssetCreate
                {
                    Name = null,
                    ExternalId = $"{tester.Prefix} test-no-name"
                },
                new AssetCreate
                {
                    Name = "test-duplicate-externalId",
                    ExternalId = $"{tester.Prefix} test-duplicate-externalId"
                },
                new AssetCreate
                {
                    Name = "test-duplicate-externalId",
                    ExternalId = $"{tester.Prefix} test-duplicate-externalId"
                }
            };

            try
            {
                var result = await tester.Destination.EnsureAssetsExistsAsync(assets, RetryMode.OnError, SanitationMode.Clean, tester.Source.Token);

                Assert.Equal(2, result.Errors.Count());
                var error1 = result.Errors.First();
                Assert.Equal(ErrorType.ItemDuplicated, error1.Type);
                Assert.Equal(ResourceType.ExternalId, error1.Resource);
                var error2 = result.Errors.Last();
                Assert.Equal(ErrorType.SanitationFailed, error2.Type);
                Assert.Equal(ResourceType.Name, error2.Resource);
                Assert.Equal(2, result.Results.Count());
                Assert.Equal(tester.Prefix + new string('æ', 250), result.Results.First().ExternalId);
                Assert.Equal($"{tester.Prefix} test-duplicate-externalId", result.Results.Last().ExternalId);
            }
            finally
            {
                var ids = new[]
                {
                    tester.Prefix + new string('æ', 250),
                    $"{tester.Prefix} test-duplicate-externalId"
                };
                await tester.Destination.CogniteClient.Assets.DeleteAsync(new AssetDelete
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

            // Create duplicate asset
            await tester.Destination.EnsureAssetsExistsAsync(new[]
            {
                new AssetCreate
                {
                    Name = "existing-asset",
                    ExternalId = $"{tester.Prefix} existing-asset"
                }
            }, RetryMode.None, SanitationMode.None, tester.Source.Token);

            var assets = new[]
            {
                new AssetCreate
                {
                    Name = "missing-label",
                    ExternalId = $"{tester.Prefix} test-missing-label",
                    Labels = new [] { new CogniteExternalId("some-missing-label"), new CogniteExternalId("some-missing-label-2") }
                },
                new AssetCreate
                {
                    Name = "existing-asset",
                    ExternalId = $"{tester.Prefix} existing-asset"
                },
                new AssetCreate
                {
                    Name = "missing-parent-external",
                    ExternalId = $"{tester.Prefix} test-missing-parent-external",
                    ParentExternalId = "some-missing-parent"
                },
                new AssetCreate
                {
                    Name = "missing-parent-external-2",
                    ExternalId = $"{tester.Prefix} test-missing-parent-external-2",
                    ParentExternalId = "some-missing-parent"
                },
                new AssetCreate
                {
                    Name = "missing-parent-external-3",
                    ExternalId = $"{tester.Prefix} test-missing-parent-external-3",
                    ParentExternalId = "some-missing-parent-2"
                },
                new AssetCreate
                {
                    Name = "missing-parent-internal",
                    ExternalId = $"{tester.Prefix} test-missing-parent-internal",
                    ParentId = 123
                },
                new AssetCreate
                {
                    Name = "missing-parent-internal-2",
                    ExternalId = $"{tester.Prefix} test-missing-parent-internal-2",
                    ParentId = 124
                },
                new AssetCreate
                {
                    Name = "missing-dataset",
                    ExternalId = $"{tester.Prefix} test-missing-dataset",
                    DataSetId = 123
                },
                new AssetCreate
                {
                    Name = "missing-dataset-2",
                    ExternalId = $"{tester.Prefix} test-missing-dataset-2",
                    DataSetId = 124
                },
                new AssetCreate
                {
                    Name = "final-asset-ok",
                    ExternalId = $"{tester.Prefix} final-asset-ok"
                }
            };
            try
            {
                var result = await tester.Destination.EnsureAssetsExistsAsync(assets, RetryMode.OnError, SanitationMode.Remove, tester.Source.Token);

                Assert.Single(result.Results);
                Assert.Equal(5, result.Errors.Count());
                Assert.Equal("final-asset-ok", result.Results.First().Name);
                foreach (var error in result.Errors)
                {
                    switch (error.Resource)
                    {
                        case ResourceType.Labels:
                            Assert.Equal(ErrorType.ItemMissing, error.Type);
                            Assert.Equal(2, error.Values.Count());
                            Assert.Contains(error.Values, idt => idt.ExternalId == "some-missing-label");
                            Assert.Contains(error.Values, idt => idt.ExternalId == "some-missing-label-2");
                            Assert.Single(error.Skipped);
                            break;
                        case ResourceType.ExternalId:
                            Assert.Equal(ErrorType.ItemExists, error.Type);
                            Assert.Single(error.Values);
                            Assert.Equal($"{tester.Prefix} existing-asset", error.Values.First().ExternalId);
                            Assert.Single(error.Skipped);
                            break;
                        case ResourceType.ParentExternalId:
                            Assert.Equal(ErrorType.ItemMissing, error.Type);
                            Assert.Equal(2, error.Values.Count());
                            Assert.Contains(error.Values, idt => idt.ExternalId == "some-missing-parent");
                            Assert.Contains(error.Values, idt => idt.ExternalId == "some-missing-parent-2");
                            Assert.Equal(3, error.Skipped.Count());
                            Assert.True(error.Complete);
                            break;
                        case ResourceType.ParentId:
                            Assert.Equal(ErrorType.ItemMissing, error.Type);
                            Assert.Equal(2, error.Values.Count());
                            Assert.Contains(error.Values, idt => idt.Id == 123);
                            Assert.Contains(error.Values, idt => idt.Id == 124);
                            Assert.Equal(2, error.Skipped.Count());
                            break;
                        case ResourceType.DataSetId:
                            Assert.Equal(ErrorType.ItemMissing, error.Type);
                            Assert.Equal(2, error.Values.Count());
                            Assert.Contains(error.Values, idt => idt.Id == 123);
                            Assert.Contains(error.Values, idt => idt.Id == 124);
                            Assert.Equal(2, error.Skipped.Count());
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
                    $"{tester.Prefix} test-missing-label",
                    $"{tester.Prefix} existing-asset",
                    $"{tester.Prefix} test-missing-parent-external",
                    $"{tester.Prefix} test-missing-parent-external-2",
                    $"{tester.Prefix} test-missing-parent-internal",
                    $"{tester.Prefix} test-missing-parent-internal-2",
                    $"{tester.Prefix} test-missing-dataset",
                    $"{tester.Prefix} test-missing-dataset-2",
                    $"{tester.Prefix} final-asset-ok"
                };
                await tester.Destination.CogniteClient.Assets.DeleteAsync(new AssetDelete
                {
                    IgnoreUnknownIds = true,
                    Items = ids.Select(Identity.Create)
                });
            }
        }
    }
}