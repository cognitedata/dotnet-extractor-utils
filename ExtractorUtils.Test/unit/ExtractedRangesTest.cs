using Cognite.Extractor.Utils;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net.Http;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using TimeRange = Cognite.Extractor.Common.TimeRange;
using System.Text.Json.Serialization;
using System.Net.Http.Headers;
using Com.Cognite.V1.Timeseries.Proto;
using System.IO;
using Google.Protobuf;
using Microsoft.Extensions.DependencyInjection;
using Cognite.Extractor.Logging;
using System.Linq;
using CogniteSdk;
using CogniteSdk.DataModels;
using Cognite.Extractor.Common;
using Xunit.Abstractions;
using Cognite.Extractor.Testing;

namespace ExtractorUtils.Test.Unit
{
    public class ExtractedRangesTest
    {
        private readonly ITestOutputHelper _output;
        public ExtractedRangesTest(ITestOutputHelper output)
        {
            _output = output;
        }


        private const string _project = "someProject";

        [Fact]
        public async Task TestExtractedRanges()
        {
            string path = "test-insert-ranges-config.yml";
            string[] lines = {  "version: 2",
                                "logger:",
                                "  console:",
                                "    level: verbose",
                                "cognite:",
                               $"  project: {_project}",
                               $"  host: https://test.cognitedata.com",
                                "  cdf-chunking:",
                                "    data-points: 4",
                                "    data-point-time-series: 2",
                                "    data-point-latest: 2",
                                "    data-point-list: 2",
                                "  cdf-throttling:",
                                "    data-points: 2",
                                "    ranges: 2" };
            System.IO.File.WriteAllLines(path, lines);

            var mocks = TestUtilities.GetMockedHttpClientFactory(MockGetFirstLatestAsync);
            var mockHttpMessageHandler = mocks.handler;
            var mockFactory = mocks.factory;

            // Setup services
            var services = new ServiceCollection();
            services.AddSingleton<IHttpClientFactory>(mockFactory.Object); // inject the mock factory
            services.AddConfig<BaseConfig>(path, 2);
            services.AddTestLogging(_output);
            services.AddCogniteClient("testApp");
            using var provider = services.BuildServiceProvider();

            var cogniteDestination = provider.GetRequiredService<CogniteDestination>();



            _mockedRanges["test1"] = new TimeRange(new DateTime(2000, 01, 01), new DateTime(2010, 01, 01));
            _mockedRanges["test2"] = new TimeRange(new DateTime(2010, 01, 01), new DateTime(2050, 01, 01));
            _mockedRanges["test3"] = new TimeRange(new DateTime(2000, 01, 01), new DateTime(2010, 01, 01));
            _mockedRanges["test4"] = new TimeRange(new DateTime(2010, 01, 01), new DateTime(2050, 01, 01));

            var ids = new[] { "test1", "test2", "test3", "test4", "test5" }.Select(Identity.Create).ToArray();

            var ranges = await cogniteDestination.GetExtractedRanges(ids, CancellationToken.None);

            Assert.Equal(_mockedRanges["test1"], ranges[ids[0]]);
            Assert.Equal(_mockedRanges["test2"], ranges[ids[1]]);
            Assert.Equal(_mockedRanges["test3"], ranges[ids[2]]);
            Assert.Equal(_mockedRanges["test4"], ranges[ids[3]]);
            Assert.Equal(TimeRange.Empty, ranges[ids[4]]);

            var limit = new DateTime(2020, 01, 01);

            var ranges2 = await cogniteDestination.GetExtractedRanges(ids.Select(id => (id, new TimeRange(CogniteTime.DateTimeEpoch, limit))),
                CancellationToken.None);

            Assert.Equal(_mockedRanges["test1"], ranges2[ids[0]]);
            Assert.Equal(new TimeRange(new DateTime(2010, 01, 01), limit), ranges2[ids[1]]);
            Assert.Equal(_mockedRanges["test3"], ranges2[ids[2]]);
            Assert.Equal(new TimeRange(new DateTime(2010, 01, 01), limit), ranges2[ids[3]]);
            Assert.Equal(TimeRange.Empty, ranges2[ids[4]]);
        }

        private class MockIdentityWithBefore
        {
            [JsonPropertyName("externalId")]
            public string ExternalId { get; set; }
            [JsonPropertyName("before")]
            public string Before { get; set; }
        }
        private class MockLatestQuery
        {
            [JsonPropertyName("ignoreUnknownIds")]
            public bool IgnoreUnknownIds { get; set; }
            [JsonPropertyName("items")]
            public IEnumerable<MockIdentityWithBefore> Items { get; set; }
        }

        private class MockQueryItem
        {
            [JsonPropertyName("externalId")]
            public string ExternalId { get; set; }
            [JsonPropertyName("start")]
            public string Start { get; set; }
        }

        private class MockQuery
        {
            [JsonPropertyName("ignoreUnknownIds")]
            public bool IgnoreUnknownIds { get; set; }
            [JsonPropertyName("items")]
            public IEnumerable<MockQueryItem> Items { get; set; }
        }
        private class MockDataPoint
        {
            [JsonPropertyName("timestamp")]
            public long Timestamp { get; set; }
            [JsonPropertyName("value")]
            public double Value { get; set; }
        }
        private class MockInstanceId
        {
            [JsonPropertyName("space")]
            public string Space { get; set; }
            [JsonPropertyName("externalId")]
            public string ExternalId { get; set; }
        }
        private class MockDataPointsItem
        {
            [JsonPropertyName("datapoints")]
            public IEnumerable<MockDataPoint> DataPoints { get; set; }
            [JsonPropertyName("externalId")]
            public string ExternalId { get; set; }
            [JsonPropertyName("instanceId")]
            public MockInstanceId InstanceId { get; set; }
            [JsonPropertyName("isString")]
            public bool IsString { get; set; }
        }

        private class ItemWrapper<T>
        {
            [JsonPropertyName("items")]
            public IEnumerable<T> Items { get; set; }
        }
        private static readonly string[] _configLines = [
            "version: 2",
            "logger:",
            "  console:",
            "    level: verbose",
            "cognite:",
           $"  project: someProject",
           $"  host: https://test.cognitedata.com",
            "  cdf-chunking:",
            "    data-points: 4",
            "    data-point-time-series: 2",
            "    data-point-latest: 2",
            "    data-point-list: 2",
            "  cdf-throttling:",
            "    data-points: 2",
            "    ranges: 2"
        ];

        private async Task<CogniteDestination> BuildDestination(
            string configPath,
            Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> handler)
        {
            System.IO.File.WriteAllLines(configPath, _configLines);
            var mockFactory = TestUtilities.GetMockedHttpClientFactory(handler).factory;
            var services = new ServiceCollection();
            services.AddSingleton<IHttpClientFactory>(mockFactory.Object);
            services.AddConfig<BaseConfig>(configPath, 2);
            services.AddTestLogging(_output);
            services.AddCogniteClient("testApp");
            var provider = services.BuildServiceProvider();
            return provider.GetRequiredService<CogniteDestination>();
        }

        // Latest timestamp: fixed epoch 2020-01-01 00:00:00 UTC
        private const long _latestTs = 1577836800000L;
        // Earliest timestamp: fixed epoch 2019-01-01 00:00:00 UTC
        private const long _earliestTs = 1546300800000L;

        /// <summary>
        /// Verifies ExternalId identity resolution in both GetLatestTimestamps (JSON/latest endpoint)
        /// and GetEarliestTimestamps (protobuf/list endpoint), which are called together by GetExtractedRanges.
        ///
        /// "test1" → response ExternalId matches request → timestamps stored → non-empty range.
        /// ""      → !IsNullOrEmpty("") = false → falls to Identity(dp.Id) → not in idSet → Empty.
        /// </summary>
        [Theory]
        [InlineData("test1", false)]
        [InlineData("", true)]
        public async Task TestGetExtractedRangesExternalIdResolution(string responseExternalId, bool expectEmpty)
        {
            Task<HttpResponseMessage> Handler(HttpRequestMessage req, CancellationToken ct)
            {
                string uri = req.RequestUri.ToString();
                if (uri.Contains("timeseries/data/latest"))
                {
                    // Simulates GetLatestTimestamps response (JSON)
                    var body = new ItemWrapper<MockDataPointsItem>
                    {
                        Items =
                        [
                            new MockDataPointsItem
                            {
                                ExternalId = responseExternalId,
                                DataPoints = [new MockDataPoint { Timestamp = _latestTs, Value = 0 }],
                                IsString = false
                            }
                        ]
                    };
                    var resp = new HttpResponseMessage
                    {
                        StatusCode = System.Net.HttpStatusCode.OK,
                        Content = new StringContent(JsonSerializer.Serialize(body))
                    };
                    resp.Content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
                    resp.Headers.Add("x-request-id", "1");
                    return Task.FromResult(resp);
                }
                else
                {
                    // Simulates GetEarliestTimestamps response (protobuf)
                    DataPointListResponse dpList = new();
                    DataPointListItem dpItem = new()
                    {
                        ExternalId = responseExternalId ?? "",
                        NumericDatapoints = new() { Datapoints = { new NumericDatapoint { Timestamp = _earliestTs, Value = 0 } } }
                    };
                    dpList.Items.Add(dpItem);
                    HttpContent responseBody;
                    using (var stream = new MemoryStream())
                    {
                        dpList.WriteTo(stream);
                        responseBody = new ByteArrayContent(stream.ToArray());
                    }
                    var resp = new HttpResponseMessage
                    {
                        StatusCode = System.Net.HttpStatusCode.OK,
                        Content = responseBody
                    };
                    resp.Content.Headers.ContentType = new MediaTypeHeaderValue("application/protobuf");
                    resp.Headers.Add("x-request-id", "1");
                    return Task.FromResult(resp);
                }
            }

            var destination = await BuildDestination("test-extid-resolution-config.yml", Handler);
            var ids = new[] { Identity.Create("test1") };

            var ranges = await destination.GetExtractedRanges(ids, CancellationToken.None);

            if (expectEmpty)
            {
                Assert.Equal(TimeRange.Empty, ranges[ids[0]]);
            }
            else
            {
                Assert.Equal(CogniteTime.FromUnixTimeMilliseconds(_earliestTs), ranges[ids[0]].First);
                Assert.Equal(CogniteTime.FromUnixTimeMilliseconds(_latestTs), ranges[ids[0]].Last);
            }
        }

        /// <summary>
        /// Verifies InstanceId identity resolution in both GetLatestTimestamps and GetEarliestTimestamps.
        /// When response has empty ExternalId but a valid InstanceId, the code takes the InstanceId branch
        /// and stores the timestamp under the InstanceId-based Identity.
        /// </summary>
        [Fact]
        public async Task TestGetExtractedRangesInstanceIdResolution()
        {
            static Task<HttpResponseMessage> Handler(HttpRequestMessage req, CancellationToken ct)
            {
                string uri = req.RequestUri.ToString();
                if (uri.Contains("timeseries/data/latest"))
                {
                    // ExternalId omitted — forces the InstanceId branch in GetLatestTimestamps
                    var body = new ItemWrapper<MockDataPointsItem>
                    {
                        Items =
                        [
                            new MockDataPointsItem
                            {
                                InstanceId = new MockInstanceId { Space = "space1", ExternalId = "inst1" },
                                DataPoints = [new MockDataPoint { Timestamp = _latestTs, Value = 0 }],
                                IsString = false
                            }
                        ]
                    };
                    var resp = new HttpResponseMessage
                    {
                        StatusCode = System.Net.HttpStatusCode.OK,
                        Content = new StringContent(JsonSerializer.Serialize(body))
                    };
                    resp.Content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
                    resp.Headers.Add("x-request-id", "1");
                    return Task.FromResult(resp);
                }
                else
                {
                    // ExternalId defaults to "" in proto — forces the InstanceId branch in GetEarliestTimestamps
                    DataPointListResponse dpList = new();
                    DataPointListItem dpItem = new()
                    {
                        InstanceId = new InstanceId { Space = "space1", ExternalId = "inst1" },
                        NumericDatapoints = new() { Datapoints = { new NumericDatapoint { Timestamp = _earliestTs, Value = 0 } } }
                    };
                    dpList.Items.Add(dpItem);
                    HttpContent responseBody;
                    using (var stream = new MemoryStream())
                    {
                        dpList.WriteTo(stream);
                        responseBody = new ByteArrayContent(stream.ToArray());
                    }
                    var resp = new HttpResponseMessage
                    {
                        StatusCode = System.Net.HttpStatusCode.OK,
                        Content = responseBody
                    };
                    resp.Content.Headers.ContentType = new MediaTypeHeaderValue("application/protobuf");
                    resp.Headers.Add("x-request-id", "1");
                    return Task.FromResult(resp);
                }
            }

            var destination = await BuildDestination("test-instanceid-resolution-config.yml", Handler);
            var instanceId = new Identity(new InstanceIdentifier("space1", "inst1"));
            var ids = new[] { instanceId };

            var ranges = await destination.GetExtractedRanges(ids, CancellationToken.None);

            Assert.Equal(CogniteTime.FromUnixTimeMilliseconds(_earliestTs), ranges[instanceId].First);
            Assert.Equal(CogniteTime.FromUnixTimeMilliseconds(_latestTs), ranges[instanceId].Last);
        }

        private ConcurrentDictionary<string, TimeRange> _mockedRanges = new ConcurrentDictionary<string, TimeRange>();
        private async Task<HttpResponseMessage> MockGetFirstLatestAsync(HttpRequestMessage message, CancellationToken token)
        {
            int cnt = 0;
            string uri = message.RequestUri.ToString();
            if (uri.Contains("timeseries/data/latest"))
            {
                var content = await message.Content.ReadAsStringAsync(token);
                var data = JsonSerializer.Deserialize<MockLatestQuery>(content);

                Assert.True(data.IgnoreUnknownIds);

                var ret = new List<MockDataPointsItem>();

                foreach (var item in data.Items)
                {
                    if (_mockedRanges.TryGetValue(item.ExternalId, out TimeRange tr))
                    {
                        var ts = tr.Last.ToUnixTimeMilliseconds();
                        if (long.TryParse(item.Before, out long before) && before < ts)
                        {
                            ts = before;
                        }
                        ret.Add(new MockDataPointsItem
                        {
                            DataPoints = new[] { new MockDataPoint { Timestamp = ts, Value = 0 } },
                            ExternalId = item.ExternalId,
                            IsString = false
                        });
                    }
                }
                var finalContent = new ItemWrapper<MockDataPointsItem>
                {
                    Items = ret
                };
                var stringContent = JsonSerializer.Serialize(finalContent);
                var response = new HttpResponseMessage
                {
                    StatusCode = System.Net.HttpStatusCode.OK,
                    Content = new StringContent(stringContent)
                };
                response.Content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
                response.Headers.Add("x-request-id", "1");
                return response;
            }
            else
            {
                var content = await message.Content.ReadAsStringAsync(token);
                var data = JsonSerializer.Deserialize<MockQuery>(content);

                Assert.True(data.IgnoreUnknownIds);

                DataPointListResponse dpList = new DataPointListResponse();

                foreach (var item in data.Items)
                {
                    if (_mockedRanges.TryGetValue(item.ExternalId, out TimeRange tr))
                    {
                        var ts = tr.First.ToUnixTimeMilliseconds();
                        if (long.TryParse(item.Start, out long start) && start > ts)
                        {
                            ts = start;
                        }
                        var dpItem = new DataPointListItem
                        {
                            ExternalId = item.ExternalId
                        };
                        if (cnt++ % 2 == 0)
                        {
                            dpItem.NumericDatapoints = new NumericDatapoints();
                            dpItem.NumericDatapoints.Datapoints.Add(new NumericDatapoint { Value = 0, Timestamp = ts });
                        }
                        else
                        {
                            dpItem.StringDatapoints = new StringDatapoints();
                            dpItem.StringDatapoints.Datapoints.Add(new StringDatapoint { Value = "0", Timestamp = ts });
                        }
                        dpList.Items.Add(dpItem);
                    }
                }
                HttpContent responseBody;
                using (MemoryStream stream = new MemoryStream())
                {
                    dpList.WriteTo(stream);
                    responseBody = new ByteArrayContent(stream.ToArray());
                }

                var response = new HttpResponseMessage
                {
                    StatusCode = System.Net.HttpStatusCode.OK,
                    Content = responseBody
                };
                response.Content.Headers.ContentType = new MediaTypeHeaderValue("application/protobuf");
                response.Headers.Add("x-request-id", "1");
                return response;
            }
        }
    }
}
