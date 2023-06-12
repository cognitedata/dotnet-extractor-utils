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
        private class MockDataPointsItem
        {
            [JsonPropertyName("datapoints")]
            public IEnumerable<MockDataPoint> DataPoints { get; set; }
            [JsonPropertyName("externalId")]
            public string ExternalId { get; set; }
            [JsonPropertyName("isString")]
            public bool IsString { get; set; }
        }

        private class ItemWrapper<T>
        {
            [JsonPropertyName("items")]
            public IEnumerable<T> Items { get; set; }
        }
        private ConcurrentDictionary<string, TimeRange> _mockedRanges = new ConcurrentDictionary<string, TimeRange>();
        private async Task<HttpResponseMessage> MockGetFirstLatestAsync(HttpRequestMessage message, CancellationToken token)
        {
            int cnt = 0;
            string uri = message.RequestUri.ToString();
            if (uri.Contains("timeseries/data/latest"))
            {
                var content = await message.Content.ReadAsStringAsync();
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
                var content = await message.Content.ReadAsStringAsync();
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
                
                var response =  new HttpResponseMessage
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
