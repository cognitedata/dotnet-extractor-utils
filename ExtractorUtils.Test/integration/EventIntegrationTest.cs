﻿using Cognite.Extensions;
using CogniteSdk;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace ExtractorUtils.Test.Integration
{
    public class EventIntegrationTest
    {
        private readonly ITestOutputHelper _output;
        public EventIntegrationTest(ITestOutputHelper output)
        {
            _output = output;
        }

        [Theory]
        [InlineData(CogniteHost.GreenField)]
        [InlineData(CogniteHost.BlueField)]
        public async Task TestCreateEvents(CogniteHost host)
        {
            using var tester = new CDFTester(host, _output);
            var ids = new[] {
                $"{tester.Prefix} evt-1",
                $"{tester.Prefix} evt-2",
                $"{tester.Prefix} evt-3",
                $"{tester.Prefix} evt-4",
                $"{tester.Prefix} evt-5",
            };

            var events1 = new[]
            {
                new EventCreate
                {
                    ExternalId = $"{tester.Prefix} evt-1"
                },
                new EventCreate
                {
                    ExternalId = $"{tester.Prefix} evt-2"
                },
                new EventCreate
                {
                    ExternalId = $"{tester.Prefix} evt-3"
                }
            };
            try
            {
                var result = await tester.Destination.EnsureEventsExistsAsync(events1, RetryMode.None, SanitationMode.None, tester.Source.Token);
                Assert.True(result.IsAllGood);
                Assert.Equal(3, result.Results.Count());

                var events2 = new[]
                {
                    events1[0],
                    new EventCreate
                    {
                        ExternalId = $"{tester.Prefix} evt-4"
                    }
                };

                result = await tester.Destination.EnsureEventsExistsAsync(events2, RetryMode.OnError, SanitationMode.None, tester.Source.Token);

                Assert.Single(result.Errors);
                var error = result.Errors.First();
                Assert.Equal(ErrorType.ItemExists, error.Type);
                Assert.Equal(ResourceType.ExternalId, error.Resource);

                result = await tester.Destination.GetOrCreateEventsAsync(ids, toCreate =>
                {
                    Assert.Single(toCreate);
                    Assert.Equal($"{tester.Prefix} evt-5", toCreate.First());
                    return new[]
                    {
                        new EventCreate
                        {
                            ExternalId = $"{tester.Prefix} evt-5"
                        }
                    };
                }, RetryMode.OnError, SanitationMode.None, tester.Source.Token);

                Assert.Equal(5, result.Results.Count());
                Assert.Equal(Enumerable.Range('1', 5), result.Results.Select(res => (int)res.ExternalId.Last()).OrderBy(v => v));
            }
            finally
            {
                await tester.Destination.CogniteClient.Events.DeleteAsync(new EventDelete
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

            var events = new[] {
                new EventCreate
                {
                    ExternalId = tester.Prefix + new string('æ', 300),
                    Description = new string('æ', 1000),
                    Metadata = Enumerable.Range(0, 100)
                        .ToDictionary(i => $"key{i.ToString("000")}{new string('æ', 100)}", i => new string('æ', 200)),
                    Source = new string('ø', 1000),
                    Subtype = tester.Prefix + new string('æ', 300),
                    Type = new string('æ', 1000)
                },
                new EventCreate
                {
                    ExternalId = $"{tester.Prefix} test-duplicate-externalId"
                },
                new EventCreate
                {
                    ExternalId = $"{tester.Prefix} test-duplicate-externalId"
                }
            };

            try
            {
                var result = await tester.Destination.EnsureEventsExistsAsync(events, RetryMode.OnError, SanitationMode.Clean, tester.Source.Token);

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
                await tester.Destination.CogniteClient.Events.DeleteAsync(new EventDelete
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

            await tester.Destination.EnsureEventsExistsAsync(new[]
            {
                new EventCreate
                {
                    ExternalId = $"{tester.Prefix} existing-event"
                }
            }, RetryMode.None, SanitationMode.None, tester.Source.Token);

            var events = new[]
            {
                new EventCreate
                {
                    ExternalId = $"{tester.Prefix} missing-asset-1",
                    AssetIds = new [] { 123L, 1234L }
                },
                new EventCreate
                {
                    ExternalId = $"{tester.Prefix} missing-asset-2",
                    AssetIds = new [] { 223L, 2234L }
                },
                new EventCreate
                {
                    ExternalId = $"{tester.Prefix} missing-asset-3",
                    AssetIds = new [] { 123L }
                },
                new EventCreate
                {
                    ExternalId = $"{tester.Prefix} existing-event"
                },
                new EventCreate
                {
                    ExternalId = $"{tester.Prefix} missing-dataset-1",
                    DataSetId = 123
                },
                new EventCreate
                {
                    ExternalId = $"{tester.Prefix} missing-dataset-2",
                    DataSetId = 124
                },
                new EventCreate
                {
                    ExternalId = $"{tester.Prefix} final-evt-ok"
                }
            };
            try
            {
                var result = await tester.Destination.EnsureEventsExistsAsync(events, RetryMode.OnError, SanitationMode.Remove, tester.Source.Token);

                tester.Logger.LogResult(result, RequestType.CreateEvents, false);

                Assert.Single(result.Results);
                Assert.Equal(3, result.Errors.Count());
                Assert.Equal($"{tester.Prefix} final-evt-ok", result.Results.First().ExternalId);

                foreach (var error in result.Errors)
                {
                    switch (error.Resource)
                    {
                        case ResourceType.AssetId:
                            Assert.Equal(ErrorType.ItemMissing, error.Type);
                            Assert.Equal(4, error.Values.Count());
                            Assert.Contains(error.Values, idt => idt.Id == 1234L);
                            Assert.Equal(3, error.Skipped.Count());
                            break;
                        case ResourceType.ExternalId:
                            Assert.Equal(ErrorType.ItemExists, error.Type);
                            Assert.Single(error.Values);
                            Assert.Equal($"{tester.Prefix} existing-event", error.Values.First().ExternalId);
                            Assert.Single(error.Skipped);
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
                    $"{tester.Prefix} missing-asset-1",
                    $"{tester.Prefix} missing-asset-2",
                    $"{tester.Prefix} missing-asset-3",
                    $"{tester.Prefix} existing-event",
                    $"{tester.Prefix} missing-dataset-1",
                    $"{tester.Prefix} missing-dataset-2",
                    $"{tester.Prefix} final-evt-ok"
                };
                await tester.Destination.CogniteClient.Events.DeleteAsync(new EventDelete
                {
                    IgnoreUnknownIds = true,
                    Items = ids.Select(Identity.Create)
                }, tester.Source.Token);
            }
        }
    }
}
