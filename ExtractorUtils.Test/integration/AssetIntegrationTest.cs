﻿using Cognite.Extensions;
using CogniteSdk;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace ExtractorUtils.Test.Integration
{
    public class AssetIntegrationTest
    {
        private readonly ITestOutputHelper _output;
        public AssetIntegrationTest(ITestOutputHelper output)
        {
            _output = output;
        }


        // Basic usage of ensure and GetOrCreate
        [Theory]
        [InlineData(CogniteHost.GreenField)]
        [InlineData(CogniteHost.BlueField)]
        public async Task TestCreateAssets(CogniteHost host)
        {
            using var tester = new CDFTester(host, _output);
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
        [Theory]
        [InlineData(CogniteHost.GreenField)]
        [InlineData(CogniteHost.BlueField)]
        public async Task TestSanitation(CogniteHost host)
        {
            using var tester = new CDFTester(host, _output);

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
                },
                new AssetCreate
                {
                    Name = "test-null-metadata",
                    ExternalId = $"{tester.Prefix} test-null-metadata",
                    Metadata = new Dictionary<string, string>
                    {
                        { "key",  null }
                    }
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
                Assert.Equal(3, result.Results.Count());
                Assert.Equal(tester.Prefix + new string('æ', 255 - tester.Prefix.Length), result.Results.First().ExternalId);
                Assert.Equal($"{tester.Prefix} test-duplicate-externalId", result.Results.ElementAt(1).ExternalId);
                Assert.Equal($"{tester.Prefix} test-null-metadata", result.Results.Last().ExternalId);
            }
            finally
            {
                var ids = new[]
                {
                    tester.Prefix + new string('æ', 255 - tester.Prefix.Length),
                    $"{tester.Prefix} test-duplicate-externalId",
                    $"{tester.Prefix} test-null-metadata"
                };
                await tester.Destination.CogniteClient.Assets.DeleteAsync(new AssetDelete
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
                    Name = "test-null-metadata",
                    ExternalId = $"{tester.Prefix} test-null-metadata",
                    Metadata = new Dictionary<string, string>
                    {
                        { "key", null }
                    }
                },
                new AssetCreate
                {
                    Name = "final-asset-ok",
                    ExternalId = $"{tester.Prefix} final-asset-ok"
                },
                new AssetCreate {
                    Name = "asset-ok-local-parent",
                    ExternalId = $"{tester.Prefix} asset-ok-local-parent",
                    ParentExternalId = $"{tester.Prefix} final-asset-ok"
                }
            };
            try
            {
                var result = await tester.Destination.EnsureAssetsExistsAsync(assets, RetryMode.OnError, SanitationMode.Remove, tester.Source.Token);

                tester.Logger.LogResult(result, RequestType.CreateAssets, false);

                Assert.Equal(2, result.Results.Count());
                Assert.Equal(6, result.Errors.Count());
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
                        case ResourceType.Metadata:
                            Assert.Equal(ErrorType.SanitationFailed, error.Type);
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
                    $"{tester.Prefix} test-missing-label",
                    $"{tester.Prefix} existing-asset",
                    $"{tester.Prefix} test-missing-parent-external",
                    $"{tester.Prefix} test-missing-parent-external-2",
                    $"{tester.Prefix} test-missing-parent-internal",
                    $"{tester.Prefix} test-missing-parent-internal-2",
                    $"{tester.Prefix} test-missing-dataset",
                    $"{tester.Prefix} test-missing-dataset-2",
                    $"{tester.Prefix} final-asset-ok",
                    $"{tester.Prefix} test-null-metadata",
                    $"{tester.Prefix} asset-ok-local-parent"
                };
                await tester.Destination.CogniteClient.Assets.DeleteAsync(new AssetDelete
                {
                    IgnoreUnknownIds = true,
                    Items = ids.Select(Identity.Create)
                });
            }
        }


        private async Task<IEnumerable<Asset>> CreateAssetsForUpdate(CDFTester tester)
        {
            var assets = new[]
            {
                new AssetCreate
                {
                    Name = $"{tester.Prefix} asset-1",
                    ExternalId = $"{tester.Prefix} asset-1"
                },
                new AssetCreate
                {
                    Name = $"{tester.Prefix} asset-2",
                    ExternalId = $"{tester.Prefix} asset-2",
                    ParentExternalId = $"{tester.Prefix} asset-1"
                },
                new AssetCreate
                {
                    Name = $"{tester.Prefix} asset-3",
                    ExternalId = $"{tester.Prefix} asset-3"
                }
            };

            var res = await tester.Destination.EnsureAssetsExistsAsync(assets, RetryMode.None, SanitationMode.None, tester.Source.Token);
            res.Throw();
            return res.Results;
        }

        [Theory]
        [InlineData(CogniteHost.GreenField)]
        [InlineData(CogniteHost.BlueField)]
        public async Task TestUpdateAssets(CogniteHost host)
        {
            using var tester = new CDFTester(host, _output);

            var assets = (await CreateAssetsForUpdate(tester)).ToArray();

            var ids = assets.Select(a => a.ExternalId);

            var upd = new AssetUpdate { Description = new UpdateNullable<string>("new description") };

            var updates = new AssetUpdateItem[]
            {
                new AssetUpdateItem(assets[0].ExternalId) { Update = upd },
                new AssetUpdateItem(assets[1].Id) { Update = upd },
                new AssetUpdateItem(assets[2].ExternalId) { Update = upd }
            };

            try
            {
                var result = await tester.Destination.UpdateAssetsAsync(updates, RetryMode.None, SanitationMode.None, tester.Source.Token);
                Assert.True(result.IsAllGood);

                Assert.All(result.Results, asset => Assert.Equal("new description", asset.Description));
                Assert.Equal(3, result.Results.Count());
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

        [Theory]
        [InlineData(CogniteHost.GreenField)]
        [InlineData(CogniteHost.BlueField)]
        public async Task TestUpdateSanitation(CogniteHost host)
        {
            using var tester = new CDFTester(host, _output);

            var assets = (await CreateAssetsForUpdate(tester)).ToArray();

            var meta = Enumerable.Range(0, 100)
                    .ToDictionary(i => $"key{i.ToString("000")}{new string('æ', 100)}", i => new string('æ', 200));

            var updates = new AssetUpdateItem[]
            {
                new AssetUpdateItem(assets[0].ExternalId)
                {
                    Update = new AssetUpdate
                    {
                        Description = new UpdateNullable<string>(new string('æ', 2000)),
                        DataSetId = new UpdateNullable<long?>(-251),
                        ExternalId = new UpdateNullable<string>(tester.Prefix + new string('æ', 300)),
                        Metadata = new UpdateDictionary<string>(meta),
                        Name = new Update<string>(new string('æ', 1000)),
                        Source = new UpdateNullable<string>(new string('æ', 12345))
                    }
                },
                new AssetUpdateItem(assets[1].ExternalId)
                {
                    Update = new AssetUpdate { Name = new Update<string>("name") }
                },
                new AssetUpdateItem(assets[1].ExternalId)
                {
                    Update = new AssetUpdate { Name = new Update<string>("name") }
                }
            };

            try
            {
                var result = await tester.Destination.UpdateAssetsAsync(updates, RetryMode.OnError, SanitationMode.Clean, tester.Source.Token);
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
                var ids = assets.Select(a => a.ExternalId).Concat(new[]
                {
                    tester.Prefix + new string('æ', 255 - tester.Prefix.Length)
                });
                await tester.Destination.CogniteClient.Assets.DeleteAsync(new AssetDelete
                {
                    IgnoreUnknownIds = true,
                    Items = ids.Select(Identity.Create)
                }, tester.Source.Token);
            }
        }

        [Theory]
        [InlineData(CogniteHost.GreenField)]
        [InlineData(CogniteHost.BlueField)]
        public async Task TestUpdateErrorHandling(CogniteHost host)
        {
            using var tester = new CDFTester(host, _output);

            var assets = (await CreateAssetsForUpdate(tester)).ToArray();

            // Various missing and duplicated
            var updates1 = new AssetUpdateItem[]
            {
                // Asset exists
                new AssetUpdateItem(assets[0].ExternalId)
                {
                    Update = new AssetUpdate { ExternalId = new UpdateNullable<string>(assets[1].ExternalId) }
                },
                // Duplicate by extId
                new AssetUpdateItem(assets[0].ExternalId)
                {
                    Update = new AssetUpdate { Name = new Update<string>("test") }
                },
                // One update OK
                new AssetUpdateItem(assets[1].ExternalId)
                {
                    Update = new AssetUpdate
                    {
                        Description = new UpdateNullable<string>("New description")
                    }
                },
                // Missing parent external id
                new AssetUpdateItem(assets[2].Id)
                {
                    Update = new AssetUpdate
                    {
                        ParentExternalId = new UpdateNullable<string>("missing-parent")
                    }
                },
                // Duplicate internal id
                new AssetUpdateItem(assets[2].Id)
                {
                    Update = new AssetUpdate { Name = new Update<string>("test") }
                }
            };
            // Various missing 2
            var updates2 = new AssetUpdateItem[]
            {
                // Missing labels
                new AssetUpdateItem(assets[0].ExternalId)
                {
                    Update = new AssetUpdate
                    {
                        Labels = new UpdateLabels<IEnumerable<CogniteExternalId>>(new [] { new CogniteExternalId("missing-label")})
                    }
                },
                // Missing datasetId
                new AssetUpdateItem(assets[1].Id)
                {
                    Update = new AssetUpdate { DataSetId = new UpdateNullable<long?>(123) }
                },
                // Update ok
                new AssetUpdateItem(assets[2].ExternalId)
                {
                    Update = new AssetUpdate { Description = new UpdateNullable<string>("New description") }
                }
            };
            // Various missing 3
            var updates3 = new AssetUpdateItem[]
            {
                // Missing by external id
                new AssetUpdateItem("missing-asset")
                {
                    Update = new AssetUpdate { Description = new UpdateNullable<string>("New description") }
                },
                // Missing by internal id
                new AssetUpdateItem(123)
                {
                    Update = new AssetUpdate { Description = new UpdateNullable<string>("New description") }
                },
                // Update ok
                new AssetUpdateItem(assets[2].ExternalId)
                {
                    Update = new AssetUpdate { Description = new UpdateNullable<string>("New description") }
                }
            };
            // Bad parentIds
            var updates4 = new AssetUpdateItem[]
            {
                // Missing parent internal id
                new AssetUpdateItem(assets[0].ExternalId)
                {
                    Update = new AssetUpdate { ParentId = new UpdateNullable<long?>(123) }
                },
                // Change root asset
                new AssetUpdateItem(assets[1].ExternalId)
                {
                    Update = new AssetUpdate { ParentExternalId = new Update<string>(assets[2].ExternalId) }
                },
                // To/from root
                new AssetUpdateItem(assets[2].ExternalId)
                {
                    Update = new AssetUpdate { ParentExternalId = new Update<string>(assets[0].ExternalId) }
                }
            };


            try
            {
                var result1 = await tester.Destination.UpdateAssetsAsync(updates1, RetryMode.OnError, SanitationMode.Remove, tester.Source.Token);
                result1.ThrowOnFatal();
                var result2 = await tester.Destination.UpdateAssetsAsync(updates2, RetryMode.OnError, SanitationMode.Remove, tester.Source.Token);
                result2.ThrowOnFatal();
                var result3 = await tester.Destination.UpdateAssetsAsync(updates3, RetryMode.OnError, SanitationMode.Remove, tester.Source.Token);
                result3.ThrowOnFatal();
                var result4 = await tester.Destination.UpdateAssetsAsync(updates4, RetryMode.OnError, SanitationMode.Remove, tester.Source.Token);
                result4.ThrowOnFatal();

                var errors = result1.Errors.ToArray();
                Assert.Equal(3, errors.Length);

                var error = errors.First(err => err.Resource == ResourceType.Id && err.Type == ErrorType.ItemDuplicated);
                Assert.Contains(error.Values, e => e.ExternalId == assets[0].ExternalId);
                Assert.Contains(error.Values, e => e.Id == assets[2].Id);

                error = errors.First(err => err.Resource == ResourceType.ExternalId && err.Type == ErrorType.ItemExists);
                Assert.Single(error.Skipped);
                Assert.Contains(error.Values, e => e.ExternalId == assets[1].ExternalId);

                error = errors.First(err => err.Resource == ResourceType.ExternalId && err.Type == ErrorType.ItemMissing);
                Assert.Single(error.Skipped);
                Assert.Contains(error.Values, e => e.ExternalId == "missing-parent");

                Assert.Single(result1.Results);

                errors = result2.Errors.ToArray();
                Assert.Equal(2, errors.Length);

                error = errors.First(err => err.Resource == ResourceType.Labels && err.Type == ErrorType.ItemMissing);
                Assert.Single(error.Skipped);
                Assert.Contains(error.Values, e => e.ExternalId == "missing-label");

                error = errors.First(err => err.Resource == ResourceType.DataSetId && err.Type == ErrorType.ItemMissing);
                Assert.Single(error.Skipped);
                Assert.Contains(error.Values, e => e.Id == 123);

                Assert.Single(result2.Results);

                errors = result3.Errors.ToArray();
                Assert.Equal(2, errors.Length);

                error = errors.First(err => err.Resource == ResourceType.ExternalId && err.Type == ErrorType.ItemMissing);
                Assert.Single(error.Skipped);
                Assert.Contains(error.Values, e => e.ExternalId == "missing-asset");

                error = errors.First(err => err.Resource == ResourceType.Id && err.Type == ErrorType.ItemMissing);
                Assert.Single(error.Skipped);
                Assert.Contains(error.Values, e => e.Id == 123);

                Assert.Single(result3.Results);

                errors = result4.Errors.ToArray();
                Assert.Equal(2, errors.Length);

                error = errors.First(err => err.Resource == ResourceType.ParentId && err.Type == ErrorType.ItemMissing);
                Assert.Single(error.Skipped);
                Assert.Contains(error.Values, e => e.Id == 123);

                error = errors.First(err => err.Resource == ResourceType.ParentId && err.Type == ErrorType.IllegalItem);
                Assert.Equal(2, error.Skipped.Count());
                Assert.Contains(error.Values, e => e.ExternalId == assets[2].ExternalId);
                Assert.Contains(error.Values, e => e.ExternalId == assets[1].ExternalId);

                Assert.Empty(result4.Results);
            }
            finally
            {
                await tester.Destination.CogniteClient.Assets.DeleteAsync(new AssetDelete
                {
                    IgnoreUnknownIds = true,
                    Items = assets.Select(a => Identity.Create(a.Id))
                }, tester.Source.Token);
            }
        }



        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task TestUpsert(bool replaceMeta)
        {
            using var tester = new CDFTester(CogniteHost.GreenField, _output);

            var upserts = Enumerable.Range(1, 5).Select(i => new AssetCreate
            {
                ExternalId = $"{tester.Prefix} test-upsert-{i}",
                Name = $"test-upsert-{i}"
            }).ToArray();

            var options = new UpsertParams { ReplaceMetadata = replaceMeta, SetNull = true };

            try
            {
                // Just create
                var result1 = await tester.Destination.UpsertAssetsAsync(upserts, RetryMode.OnError,
                    SanitationMode.Remove, options, tester.Source.Token);
                // Just retrieve
                var result2 = await tester.Destination.UpsertAssetsAsync(upserts, RetryMode.OnError,
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
                var result3 = await tester.Destination.UpsertAssetsAsync(upserts, RetryMode.OnError,
                    SanitationMode.Remove, options, tester.Source.Token);
                // Update all and add 2 more

                foreach (var ups in upserts)
                {
                    ups.Metadata = new Dictionary<string, string>
                    {
                        { "someKey2", "someValue2" }
                    };
                }

                upserts = upserts.Concat(Enumerable.Range(6, 2).Select(i => new AssetCreate
                {
                    ExternalId = $"{tester.Prefix} test-upsert-{i}",
                    Name = $"test-upsert-{i}"
                })).ToArray();

                var result4 = await tester.Destination.UpsertAssetsAsync(upserts, RetryMode.OnError,
                    SanitationMode.Remove, options, tester.Source.Token);

                // Update all, fail to create 2 more

                foreach (var ups in upserts)
                {
                    ups.Source = "Some source";
                }

                upserts = upserts.Concat(Enumerable.Range(8, 2).Select(i => new AssetCreate
                {
                    ExternalId = $"{tester.Prefix} test-upsert-{i}",
                    Name = null
                })).ToArray();

                var result5 = await tester.Destination.UpsertAssetsAsync(upserts, RetryMode.OnError,
                    SanitationMode.Remove, options, tester.Source.Token);


                Assert.Equal(5, result1.Results.Count());
                result1.Throw();
                Assert.Equal(5, result2.Results.Count());
                result2.Throw();
                Assert.Equal(5, result3.Results.Count());
                result3.Throw();
                Assert.All(result3.Results, res =>
                {
                    Assert.Single(res.Metadata);
                    Assert.Equal("someValue", res.Metadata["someKey"]);
                    Assert.Equal("Some description", res.Description);
                });
                Assert.Equal(7, result4.Results.Count());
                result4.Throw();
                Assert.All(result4.Results.Take(5), res =>
                {
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
                Assert.All(result5.Results, res =>
                {
                    Assert.Equal("Some source", res.Source);
                });
            }
            finally
            {
                var ids = Enumerable.Range(1, 9)
                    .Select(i => Identity.Create($"{tester.Prefix} test-upsert-{i}"))
                    .ToList();
                await tester.Destination.CogniteClient.Assets.DeleteAsync(new AssetDelete
                {
                    IgnoreUnknownIds = true,
                    Items = ids
                });
            }
        }

        private async Task CreateAssetsToRead(CDFTester tester)
        {
            var assets = Enumerable.Range(0, 10)
                .Select(i => new AssetCreate
                {
                    Name = $"test-asset-read-{i}",
                    ExternalId = $"{tester.Prefix}-read-{i}"
                })
                .ToList();
            await tester.Destination.CogniteClient.Assets.CreateAsync(assets);
        }

        private async Task DeleteReadAssets(CDFTester tester)
        {
            var ids = Enumerable.Range(0, 10)
                .Select(i => Identity.Create($"{tester.Prefix}-read-{i}"));

            await tester.Destination.CogniteClient.Assets.DeleteAsync(new AssetDelete { Items = ids, IgnoreUnknownIds = true });
        }

        [Theory]
        [InlineData(CogniteHost.GreenField)]
        [InlineData(CogniteHost.BlueField)]
        public async Task TestFollowCursor(CogniteHost host)
        {
            using var tester = new CDFTester(host, _output);

            await CreateAssetsToRead(tester);

            try
            {
                for (int i = 0; i < 10; i++)
                {
                    var enumerable = ApiUtils.FollowCursor(
                        new AssetQuery { Limit = 2, Filter = new AssetFilter { ExternalIdPrefix = tester.Prefix } },
                        tester.Destination.CogniteClient.Assets.ListAsync<Asset>,
                        tester.Source.Token);

                    var total = await enumerable.ToListAsync(tester.Source.Token);

                    if (total.Count() == 10) break;

                    Assert.NotEqual(9, i);

                    await Task.Delay(1000);
                }
            }
            finally
            {
                await DeleteReadAssets(tester);
            }
        }

        [Theory]
        [InlineData(CogniteHost.GreenField)]
        [InlineData(CogniteHost.BlueField)]
        public async Task TestReadPartitioned(CogniteHost host)
        {
            using var tester = new CDFTester(host, _output);

            await CreateAssetsToRead(tester);

            try
            {
                for (int i = 0; i < 10; i++)
                {

                    var total = await ApiUtils.ReadAllPartitioned(
                        () => new AssetQuery { Limit = 2, Filter = new AssetFilter { ExternalIdPrefix = tester.Prefix } },
                        tester.Destination.CogniteClient.Assets.ListAsync<Asset>,
                        5,
                        5,
                        tester.Source.Token);

                    if (total.Count() == 10) break;

                    Assert.NotEqual(9, i);

                    await Task.Delay(1000);
                }
            }
            finally
            {
                await DeleteReadAssets(tester);
            }
        }
    }
}
