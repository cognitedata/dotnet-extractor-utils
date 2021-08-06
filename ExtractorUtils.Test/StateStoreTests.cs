using Cognite.Extractor.Common;
using Cognite.Extractor.Logging;
using Cognite.Extractor.StateStorage;
using Cognite.Extractor.Utils;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace ExtractorUtils.Test
{
    [CollectionDefinition("state-store", DisableParallelization = true)]
    public class StateStoreTestCollection
    {}
    [Collection("state-store")]
    public class StateStoreTests
    {
        private const string _project = "someProject";
        private const string _apiKey = "someApiKey";
        private const string _host = "https://test.cognitedata.com";
        private const string _dbName = "testDb";
        private const string _tableName = "testTable";

        private static LoggerConfig loggerConf = new LoggerConfig
        {
            Console = new ConsoleConfig
            {
                Level = "verbose"
            }
        };

        [Theory]
        [InlineData(StateStoreConfig.StorageType.LiteDb)]
        [InlineData(StateStoreConfig.StorageType.Raw)]
        public async Task TestStateStorage(StateStoreConfig.StorageType type)
        {
            string path = "test-state-storage-config.yml";
            string[] lines = {  "version: 2",
                                "logger:",
                                "  console:",
                                "    level: verbose",
                                "cognite:",
                               $"  project: {_project}",
                               $"  api-key: {_apiKey}",
                               $"  host: {_host}",
                                "  cdf-chunking:",
                                "    raw-rows: 4",
                                "  cdf-throttling:",
                                "    raw: 2",
                                "state-store:",
                                $"  location: {(type == StateStoreConfig.StorageType.LiteDb ? "test.db" : _dbName)}",
                                $"  database: {type}"};
            File.WriteAllLines(path, lines);

            var mocks = TestUtilities.GetMockedHttpClientFactory(mockRawRequestAsync);
            var mockHttpMessageHandler = mocks.handler;
            var mockFactory = mocks.factory;

            // Setup services
            var services = new ServiceCollection();
            services.AddSingleton<IHttpClientFactory>(mockFactory.Object); // inject the mock factory
            services.AddSingleton(loggerConf);
            services.AddConfig<BaseConfig>(path, 2);
            services.AddLogger();
            services.AddCogniteClient("testApp");
            services.AddStateStore();
            File.Delete("test.db");

            using var provider = services.BuildServiceProvider();
            var stateStore = provider.GetRequiredService<IExtractionStateStore>();

            var now = DateTime.UtcNow;

            var state1 = new HistoryExtractionState("test0", true, true);
            var state2 = new HistoryExtractionState("test1", true, false);
            var state3 = new HistoryExtractionState("test2", false, true);
            var state4 = new HistoryExtractionState("test3", false, false);

            var states = new[] { state1, state2, state3, state4 };

            var stateDict = states.ToDictionary(state => state.Id);

            // Does nothing, but shouldn't throw or cause issues
            await stateStore.RestoreExtractionState(stateDict, _tableName, false, CancellationToken.None);

            foreach (var state in states) state.FinalizeRangeInit();
            Assert.Equal(now, state1.SourceExtractedRange.First, TimeSpan.FromMilliseconds(500));
            Assert.Equal(CogniteTime.DateTimeEpoch, state2.SourceExtractedRange.Last);
            Assert.Equal(now, state3.SourceExtractedRange.Last, TimeSpan.FromMilliseconds(500));
            Assert.Equal(now, state4.SourceExtractedRange.Last, TimeSpan.FromMilliseconds(500));

            // Update each state from stream
            foreach (var state in states) state.UpdateFromStream(now - TimeSpan.FromDays(1), now + TimeSpan.FromDays(1));
            Assert.Equal(now, state1.SourceExtractedRange.First, TimeSpan.FromMilliseconds(500));
            Assert.Equal(now, state1.SourceExtractedRange.Last, TimeSpan.FromMilliseconds(500));

            Assert.Equal(CogniteTime.DateTimeEpoch, state2.SourceExtractedRange.First);
            Assert.Equal(CogniteTime.DateTimeEpoch, state2.SourceExtractedRange.Last);

            Assert.Equal(now, state3.SourceExtractedRange.First, TimeSpan.FromMilliseconds(500));
            Assert.Equal(now + TimeSpan.FromDays(1), state3.SourceExtractedRange.Last);

            Assert.Equal(now - TimeSpan.FromDays(1), state4.SourceExtractedRange.First);
            Assert.Equal(now + TimeSpan.FromDays(1), state4.SourceExtractedRange.Last);


            state1.UpdateFromFrontfill(now + TimeSpan.FromDays(2), true);
            Assert.False(state1.IsFrontfilling);
            Assert.True(state1.IsBackfilling);
            Assert.Equal(now + TimeSpan.FromDays(2), state1.SourceExtractedRange.Last);

            state3.UpdateFromBackfill(now - TimeSpan.FromDays(2), true);
            Assert.False(state3.IsFrontfilling);
            Assert.False(state3.IsBackfilling);
            Assert.Equal(now - TimeSpan.FromDays(2), state3.SourceExtractedRange.First);


            foreach (var state in states)
            {
                state.UpdateDestinationRange(state.SourceExtractedRange.First, state.SourceExtractedRange.Last);
                if (state.Id == "test2")
                {
                    Assert.Equal(CogniteTime.DateTimeEpoch, state.DestinationExtractedRange.First);
                }
                else
                {
                    Assert.Equal(state.SourceExtractedRange.First, state.DestinationExtractedRange.First);
                }
                Assert.Equal(state.SourceExtractedRange.Last, state.DestinationExtractedRange.Last);
            }

            foreach (var state in states)
            {
                Assert.NotNull(state.LastTimeModified);
                Assert.True(state.LastTimeModified > now);
            }

            await stateStore.StoreExtractionState(states, _tableName, CancellationToken.None);

            state1 = new HistoryExtractionState("test0", true, true);
            state2 = new HistoryExtractionState("test1", true, false);
            state3 = new HistoryExtractionState("test2", false, true);
            state4 = new HistoryExtractionState("test3", false, false);

            states = new[] { state1, state2, state3, state4 };

            stateDict = states.ToDictionary(state => state.Id);

            await stateStore.RestoreExtractionState(stateDict, _tableName, true, CancellationToken.None);

            foreach (var state in states) state.FinalizeRangeInit();

            Assert.Equal(now, state1.SourceExtractedRange.First, TimeSpan.FromMilliseconds(500));
            Assert.Equal(now + TimeSpan.FromDays(2), state1.SourceExtractedRange.Last);

            Assert.Equal(CogniteTime.DateTimeEpoch, state2.SourceExtractedRange.First);
            Assert.Equal(CogniteTime.DateTimeEpoch, state2.SourceExtractedRange.Last);

            Assert.Equal(CogniteTime.DateTimeEpoch, state3.SourceExtractedRange.First);
            Assert.Equal(now + TimeSpan.FromDays(1), state3.SourceExtractedRange.Last);

            Assert.Equal(now - TimeSpan.FromDays(1), state4.SourceExtractedRange.First);
            Assert.Equal(now + TimeSpan.FromDays(1), state4.SourceExtractedRange.Last);

            await stateStore.DeleteExtractionState(states, _tableName, CancellationToken.None);

            state1 = new HistoryExtractionState("test0", true, true);
            state2 = new HistoryExtractionState("test1", true, false);
            state3 = new HistoryExtractionState("test2", false, true);
            state4 = new HistoryExtractionState("test3", false, false);

            states = new[] { state1, state2, state3, state4 };

            stateDict = states.ToDictionary(state => state.Id);

            await stateStore.RestoreExtractionState(stateDict, _tableName, false, CancellationToken.None);

            foreach (var state in states)
            {
                Assert.Equal(TimeRange.Empty, state.SourceExtractedRange);
                Assert.Equal(TimeRange.Complete, state.DestinationExtractedRange);
            }
            await stateStore.DeleteExtractionState(states, "othertable", CancellationToken.None);
        }

        [Theory]
        [InlineData(StateStoreConfig.StorageType.LiteDb)]
        [InlineData(StateStoreConfig.StorageType.Raw)]
        public async Task TestBaseStateStorage(StateStoreConfig.StorageType type)
        {
            string path = "test-state-storage-config.yml";
            string[] lines = {  "version: 2",
                                "logger:",
                                "  console:",
                                "    level: verbose",
                                "cognite:",
                               $"  project: {_project}",
                               $"  api-key: {_apiKey}",
                               $"  host: {_host}",
                                "  cdf-chunking:",
                                "    raw-rows: 4",
                                "  cdf-throttling:",
                                "    raw: 2",
                                "state-store:",
                                $"  location: {(type == StateStoreConfig.StorageType.LiteDb ? "test.db" : _dbName)}",
                                $"  database: {type}"};
            File.WriteAllLines(path, lines);

            var mocks = TestUtilities.GetMockedHttpClientFactory(mockRawRequestAsync);
            var mockHttpMessageHandler = mocks.handler;
            var mockFactory = mocks.factory;
            rows.Clear();

            // Setup services
            var services = new ServiceCollection();
            services.AddSingleton<IHttpClientFactory>(mockFactory.Object); // inject the mock factory
            services.AddSingleton(loggerConf);
            services.AddConfig<BaseConfig>(path, 2);
            services.AddLogger();
            services.AddCogniteClient("testApp");
            services.AddStateStore();
            File.Delete("test.db");

            using var provider = services.BuildServiceProvider();
            var stateStore = provider.GetRequiredService<IExtractionStateStore>();

            var now = DateTime.UtcNow;

            var state1 = new BaseExtractionState("test0");
            var state2 = new BaseExtractionState("test1");
            var state3 = new BaseExtractionState("test2");
            var state4 = new BaseExtractionState("test3");

            var states = new[] { state1, state2, state3, state4 };

            foreach (var state in states) {
                state.InitExtractedRange(new DateTime(2000, 01, 01), new DateTime(2010, 01, 01));
                state.UpdateDestinationRange(new DateTime(2005, 01, 01), new DateTime(2020, 01, 01));
                Assert.Equal(new DateTime(2000, 01, 01), state.DestinationExtractedRange.First);
                Assert.Equal(new DateTime(2020, 01, 01), state.DestinationExtractedRange.Last);
            }

            foreach (var state in states)
            {
                Assert.NotNull(state.LastTimeModified);
                Assert.True(state.LastTimeModified > now);
            }

            await stateStore.StoreExtractionState(states, _tableName, CancellationToken.None);

            state1 = new BaseExtractionState("test0");
            state2 = new BaseExtractionState("test1");
            state3 = new BaseExtractionState("test2");
            state4 = new BaseExtractionState("test3");

            states = new[] { state1, state2, state3, state4 };

            var stateDict = states.ToDictionary(state => state.Id);

            await stateStore.RestoreExtractionState(stateDict, _tableName, true, CancellationToken.None);

            foreach (var state in states)
            {
                state.UpdateDestinationRange(new DateTime(2005, 01, 01), new DateTime(2020, 01, 01));
                Assert.Equal(new DateTime(2000, 01, 01), state.DestinationExtractedRange.First);
                Assert.Equal(new DateTime(2020, 01, 01), state.DestinationExtractedRange.Last);
            }

            await stateStore.DeleteExtractionState(states, _tableName, CancellationToken.None);

            state1 = new BaseExtractionState("test0");
            state2 = new BaseExtractionState("test1");
            state3 = new BaseExtractionState("test2");
            state4 = new BaseExtractionState("test3");

            states = new[] { state1, state2, state3, state4 };

            stateDict = states.ToDictionary(state => state.Id);

            await stateStore.RestoreExtractionState(stateDict, _tableName, false, CancellationToken.None);

            foreach (var state in states)
            {
                Assert.Equal(TimeRange.Empty, state.DestinationExtractedRange);
            }
            await stateStore.DeleteExtractionState(states, "othertable", CancellationToken.None);
        }

        [Theory]
        [InlineData(StateStoreConfig.StorageType.LiteDb)]
        [InlineData(StateStoreConfig.StorageType.Raw)]
        public async Task TestDuplicateIds(StateStoreConfig.StorageType type)
        {
            // In response to an issue causing duplicate ids if storing multiple times in litedb
            string path = "test-store-duplicates-config.yml";
            string[] lines = {  "version: 2",
                                "logger:",
                                "  console:",
                                "    level: verbose",
                                "cognite:",
                               $"  project: {_project}",
                               $"  api-key: {_apiKey}",
                               $"  host: {_host}",
                                "  cdf-chunking:",
                                "    raw-rows: 4",
                                "  cdf-throttling:",
                                "    raw: 2",
                                "state-store:",
                                $"  location: {(type == StateStoreConfig.StorageType.LiteDb ? "test.db" : _dbName)}",
                                $"  database: {type}"};
            File.WriteAllLines(path, lines);

            var mocks = TestUtilities.GetMockedHttpClientFactory(mockRawRequestAsync);
            var mockHttpMessageHandler = mocks.handler;
            var mockFactory = mocks.factory;

            // Setup services
            var services = new ServiceCollection();
            services.AddSingleton<IHttpClientFactory>(mockFactory.Object); // inject the mock factory
            services.AddSingleton(loggerConf);
            services.AddConfig<BaseConfig>(path, 2);
            services.AddLogger();
            services.AddCogniteClient("testApp");
            services.AddStateStore();
            File.Delete("test.db");

            var state = new BaseExtractionState("test");

            using var provider = services.BuildServiceProvider();
            var stateStore = provider.GetRequiredService<IExtractionStateStore>();

            state.InitExtractedRange(new DateTime(2020, 01, 01), new DateTime(2020, 01, 01));
            state.UpdateDestinationRange(new DateTime(2019, 01, 01), new DateTime(2021, 01, 01));

            await stateStore.StoreExtractionState(new[] { state }, _tableName, CancellationToken.None);

            state.UpdateDestinationRange(new DateTime(2015, 01, 01), new DateTime(2025, 01, 01));

            await stateStore.StoreExtractionState(new[] { state }, _tableName, CancellationToken.None);

            int count = 0;

            state = new BaseExtractionState("test");

            await stateStore.RestoreExtractionState<BaseExtractionStatePoco, BaseExtractionState>(
                new Dictionary<string, BaseExtractionState> { { "test", state } }, _tableName,
                (state, poco) => {
                    count++;
                    state.InitExtractedRange(poco.FirstTimestamp, poco.LastTimestamp);
                },
                CancellationToken.None);

            Assert.Equal(1, count);
            Assert.Equal(new DateTime(2015, 01, 01), state.DestinationExtractedRange.First);
            Assert.Equal(new DateTime(2025, 01, 01), state.DestinationExtractedRange.Last);

        }

        private class ExpandedStatePoco : BaseExtractionStatePoco
        {
            public string TestAuto { get; set; }

            [StateStoreProperty("testy-prop")]
            public string TestProp1 { get; set; }

            [StateStoreProperty("TestyProp")]
            public string TestProp2 { get; set; }

            [StateStoreProperty("magic-property")]
            public SubStatePoco SubObject { get; set; }

            [StateStoreProperty("array")]
            public IEnumerable<string> ArrayProp { get; set; }
        }

        private class SubStatePoco
        {
            [StateStoreProperty("nested-prop")]
            public string NestedProperty { get; set; }
        }

        [Fact]
        public async Task TestNameMapping()
        {
            var services = new ServiceCollection();
            services.AddSingleton(loggerConf);
            services.AddLogger();

            using var provider = services.BuildServiceProvider();
            var config = new StateStoreConfig
            {
                Location = "lite-test.db",
                Database = StateStoreConfig.StorageType.LiteDb
            };
            File.Delete("lite-test.db");
            var logger = provider.GetRequiredService<ILogger<LiteDBStateStore>>();
            var stateStore = new LiteDBStateStore(config, logger);

            var state = new HistoryExtractionState("test-state-1", true, true);
            state.FinalizeRangeInit();

            await stateStore.StoreExtractionState(new[] { state }, "customstates", state =>
                new ExpandedStatePoco
                {
                    FirstTimestamp = state.DestinationExtractedRange.First,
                    LastTimestamp = state.DestinationExtractedRange.Last,
                    Id = state.Id,
                    TestAuto = "testauto",
                    TestProp1 = "testprop1",
                    TestProp2 = "testprop2",
                    SubObject = new SubStatePoco
                    {
                        NestedProperty = "test"
                    },
                    ArrayProp = new [] { "test", "test2" }
                }, CancellationToken.None);

            var col = stateStore.Database.GetCollection("customstates");
            var ret = col.FindAll();

            Assert.Single(ret);

            var retState = ret.First();

            Assert.False(retState.ContainsKey("id"));

            Assert.True(retState.ContainsKey("_id"));
            Assert.True(retState.ContainsKey("first"));
            Assert.True(retState.ContainsKey("last"));
            Assert.True(retState.ContainsKey("test-auto"));
            Assert.True(retState.ContainsKey("testy-prop"));
            Assert.True(retState.ContainsKey("TestyProp"));
            Assert.True(retState.ContainsKey("magic-property"));
            Assert.Equal("test-state-1", retState["_id"]);
            Assert.Equal("testauto", retState["test-auto"]);
            Assert.Equal("testprop1", retState["testy-prop"]);
            Assert.Equal("testprop2", retState["TestyProp"]);
            Assert.Equal("test", retState["magic-property"]["nested-prop"]);
            Assert.Equal(2, retState["array"].AsArray.Count);
            Assert.Equal("test", retState["array"].AsArray[0]);
            Assert.Equal("test2", retState["array"].AsArray[1]);
        }
        [Fact]
        public void TestJsonFromBson()
        {
            var mapper = StateStoreUtils.BuildMapper();

            var obj = new ExpandedStatePoco
            {
                Id = "testid",
                FirstTimestamp = CogniteTime.DateTimeEpoch,
                LastTimestamp = CogniteTime.DateTimeEpoch,
                TestAuto = "testauto",
                TestProp1 = "testprop1",
                TestProp2 = "testprop2",
                SubObject = new SubStatePoco
                {
                    NestedProperty = "nested"
                },
                ArrayProp = new[] { "test", "test2" }
            };

            var raw = StateStoreUtils.BsonToDict(mapper.ToDocument(obj));
            var json = JsonSerializer.Serialize(raw);

            Assert.Equal("{\"test-auto\":\"testauto\",\"testy-prop\":\"testprop1\",\"TestyProp\":\"testprop2\","
                + "\"magic-property\":{\"nested-prop\":\"nested\"},\"array\":[\"test\",\"test2\"],\"first\":0,\"last\":0,\"_id\":\"testid\"}",
                json);

            var recreated = StateStoreUtils.DeserializeViaBson<ExpandedStatePoco>(
                JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(json), mapper);

            Assert.Equal(recreated.Id, obj.Id);
            Assert.Equal(recreated.FirstTimestamp, obj.FirstTimestamp);
            Assert.Equal(recreated.LastTimestamp, obj.LastTimestamp);
            Assert.Equal(recreated.TestAuto, obj.TestAuto);
            Assert.Equal(recreated.TestProp1, obj.TestProp1);
            Assert.Equal(recreated.TestProp2, obj.TestProp2);
            Assert.Equal(recreated.SubObject.NestedProperty, obj.SubObject.NestedProperty);

        }
        private class SubTestDto
        {
            [JsonPropertyName("nested-prop")]
            public string NestedProperty { get; set; }
        }
        private class TestDto
        {
            [JsonPropertyName("first")]
            public long FirstTimestamp { get; set; }
            [JsonPropertyName("last")]
            public long LastTimestamp { get; set; }
            [JsonPropertyName("test-auto")]
            public string TestAuto { get; set; }

            [JsonPropertyName("testy-prop")]
            public string TestProp1 { get; set; }

            [JsonPropertyName("TestyProp")]
            public string TestProp2 { get; set; }

            [JsonPropertyName("magic-property")]
            public SubTestDto SubObject { get; set; }

            [JsonPropertyName("array")]
            public IEnumerable<string> ArrayProp { get; set; }
        }
        private class RawItem
        {
            public string key { get; set; }
            public TestDto columns { get; set; }
        }

        private class RawDeleteItem
        {
            public string key { get; set; }
        }

        private class RawDelete
        {
            public IEnumerable<RawDeleteItem> items { get; set; }
        }

        private class RawItems
        {
            public List<RawItem> items { get; set; }
        }

        private static Dictionary<string, TestDto> rows = new Dictionary<string, TestDto>();
        private static async Task<HttpResponseMessage> mockRawRequestAsync(HttpRequestMessage message, CancellationToken token)
        {
            var uri = message.RequestUri.ToString();
            // Assume that this means that the database or table is wrong
            if (!uri.Contains($"{_host}/api/v1/projects/{_project}/raw/dbs/{_dbName}/tables/{_tableName}/rows"))
            {
                var response = new HttpResponseMessage
                {
                    StatusCode = HttpStatusCode.NotFound,
                    Content = new StringContent(@"""error"": {""code"": 404,""message"": ""No tables named dbname""} }")
                };
                response.Content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
                response.Headers.Add("x-request-id", "1");

                return response;
            }

            if (message.Method == HttpMethod.Post && uri.Contains(
                $"{_host}/api/v1/projects/{_project}/raw/dbs/{_dbName}/tables/{_tableName}/rows/delete"))
            {

                var content = await message.Content.ReadAsStringAsync();
                var items = JsonSerializer.Deserialize<RawDelete>(content);
                foreach (var item in items.items)
                {
                    rows.Remove(item.key);
                }
                var response = new HttpResponseMessage
                {
                    StatusCode = HttpStatusCode.OK,
                    Content = new StringContent("{ }")
                };
                response.Content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
                response.Headers.Add("x-request-id", "1");

                return response;
            }
            if (message.Method == HttpMethod.Post)
            {
                Assert.Contains("ensureParent=true", message.RequestUri.Query);

                var responseBody = "{ }";
                var statusCode = HttpStatusCode.OK;
                var content = await message.Content.ReadAsStringAsync();
                var items = JsonSerializer.Deserialize<RawItems>(content);

                foreach (var item in items.items)
                {
                    rows[item.key] = item.columns;
                }

                var response = new HttpResponseMessage
                {
                    StatusCode = statusCode,
                    Content = new StringContent(responseBody)
                };
                response.Content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
                response.Headers.Add("x-request-id", "1");

                return response;
            }
            else
            {
                var responseBody = JsonSerializer.Serialize(new RawItems
                {
                    items = rows.Select(kvp =>
                        new RawItem
                        {
                            key = kvp.Key,
                            columns = kvp.Value
                        }).ToList()
                });
                var response = new HttpResponseMessage
                {
                    StatusCode = HttpStatusCode.OK,
                    Content = new StringContent(responseBody)
                };
                response.Content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
                response.Headers.Add("x-request-id", "1");

                return response;
            }
        }
    }
}
