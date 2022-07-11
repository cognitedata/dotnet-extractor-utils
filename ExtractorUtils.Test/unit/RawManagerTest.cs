using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Xunit;
using Cognite.Extractor.Utils;
using Xunit.Abstractions;
using Cognite.Extractor.Testing;
using Cognite.Extractor.Common;
using CogniteSdk;



namespace ExtractorUtils.Test.Unit
{
    class MyConfig : BaseConfig
    {
        public RawManagerConfig Manager { get; set; }
    }
    public class RawManagerTest
    {
        private const string _authTenant = "someTenant";
        private const string _project = "someProject";
        private const string _apiKey = "someApiKey";
        private const string _host = "https://test.cognitedata.com";
        private const string _dbName = "testDb";
        private const string _tableName = "testTable";
        private const string _index = "0";

        private readonly ITestOutputHelper _output;
        private static bool _failInsert = false;
        private static bool _failUpdateState = false;

        public RawManagerTest(ITestOutputHelper output)
        {
            _output = output;
        }

        [Fact]
        public async Task TestUploadLogToState()
        {
            string path = "test-upload-log-to-state-config";
            string[] config = { "version: 2",
                                "logger:",
                                "  console:",
                                "    level: verbose",
                                "cognite:",
                               $"  project: {_project}",
                               $"  api-key: {_apiKey}",
                               $"  host: {_host}",
                                "manager:",
                               $"  index: {_index}",
                               $"  database-name: {_dbName}",
                               $"  table-name: {_tableName}"};
            System.IO.File.WriteAllLines(path, config);

            var mocks = TestUtilities.GetMockedHttpClientFactory(mockInsertRowsAsync);
            var mockHttpMessageHandler = mocks.handler;
            var mockFactory = mocks.factory;

            var services = new ServiceCollection();
            services.AddSingleton<IHttpClientFactory>(mockFactory.Object);
            services.AddConfig<MyConfig>(path, 2);
            services.AddTestLogging(_output);
            services.AddCogniteClient("testApp");

            using (var provider = services.BuildServiceProvider())
            {
                var testLogger = provider.GetRequiredService<ILogger<RawManagerTest>>();

                var managerConfig = provider.GetRequiredService<RawManagerConfig>();
                var destination = provider.GetRequiredService<CogniteDestination>();
                var logger = provider.GetRequiredService<ILogger<RawExtractorManager>>();
                var source = new CancellationTokenSource();
                var scheduler = new PeriodicScheduler(source.Token);

                RawExtractorManager extractorManager = new RawExtractorManager(managerConfig, destination, logger, scheduler, source);
                await extractorManager.UploadLogToState();
                Dictionary<string, RawLogData> rows = rowsUploadState;

                Assert.True(rows.Count == 1);
                Assert.True(rows[_index].Active == false);
                Assert.True(rows[_index].TimeStamp < DateTime.UtcNow);

                DateTime prevTimeStamp = rows[_index].TimeStamp;
                extractorManager._state.UpdatedStatus = true;

                await extractorManager.UploadLogToState();

                Assert.True(rows.Count == 1);
                Assert.True(rows[_index].Active == true);
                Assert.True(rows[_index].TimeStamp > prevTimeStamp);

                _failInsert = true;
                prevTimeStamp = rows[_index].TimeStamp;
                extractorManager._state.UpdatedStatus = false;

                await extractorManager.UploadLogToState();

                Assert.True(rows[_index].Active == true);
                Assert.True(rows[_index].TimeStamp == prevTimeStamp);
            }

            System.IO.File.Delete(path);
        }
        [Fact]
        public async Task TestUpdateExtractorState()
        {
            string path = "test-update-extractor-state-config";
            string[] config = { "version: 2",
                                "logger:",
                                "  console:",
                                "    level: verbose",
                                "cognite:",
                               $"  project: {_project}",
                               $"  api-key: {_apiKey}",
                               $"  host: {_host}",
                                "manager:",
                               $"  index: {_index}",
                               $"  database-name: {_dbName}",
                               $"  table-name: {_tableName}"};
            System.IO.File.WriteAllLines(path, config);

            var mocks = TestUtilities.GetMockedHttpClientFactory(mockGetRowsAsync);
            var mockHttpMessageHandler = mocks.handler;
            var mockFactory = mocks.factory;

            var services = new ServiceCollection();
            services.AddSingleton<IHttpClientFactory>(mockFactory.Object);
            services.AddConfig<MyConfig>(path, 2);
            services.AddTestLogging(_output);
            services.AddCogniteClient("testApp");

            using (var provider = services.BuildServiceProvider())
            {
                var testLogger = provider.GetRequiredService<ILogger<RawManagerTest>>();

                var managerConfig = provider.GetRequiredService<RawManagerConfig>();
                var destination = provider.GetRequiredService<CogniteDestination>();
                var logger = provider.GetRequiredService<ILogger<RawExtractorManager>>();
                var source = new CancellationTokenSource();
                var scheduler = new PeriodicScheduler(source.Token);

                RawExtractorManager extractorManager = new RawExtractorManager(managerConfig, destination, logger, scheduler, source);

                Dictionary<string, RawLogData> rows = new Dictionary<string, RawLogData>();

                rows.Add("0", new RawLogData(DateTime.UtcNow, true));
                rows.Add("1", new RawLogData(DateTime.UtcNow.Subtract(new TimeSpan(0, 0, 10)), false));
                rows.Add("2", new RawLogData(DateTime.UtcNow.Subtract(new TimeSpan(0, 0, 30)), false));
                rowsUpdateState = rows;

                Assert.True(extractorManager._state.CurrentState.Count == 0);

                await extractorManager.UpdateExtractorState();

                //Testing that the state has changed 
                Assert.True(extractorManager._state.CurrentState.Count == 3);

                //Checking that each value in the state is the same as in the local dict
                foreach (RawExtractorInstance instance in extractorManager._state.CurrentState)
                {
                    string key = instance.Key.ToString();
                    if (rows.ContainsKey(key))
                    {
                        Assert.True(rows[key].Active == instance.Active);
                        Assert.True(rows[key].TimeStamp == instance.TimeStamp);
                    }
                }

                //Testing updating the active status and timestamp for a given extractor
                int testKey = 1;
                rows[testKey.ToString()] = new RawLogData(DateTime.UtcNow, true);
                rowsUpdateState = rows;

                await extractorManager.UpdateExtractorState();

                foreach (RawExtractorInstance instance in extractorManager._state.CurrentState)
                {
                    if (instance.Key == testKey)
                    {
                        //Checking that the valus has been changed for the given extractor
                        Assert.True(rows[testKey.ToString()].Active == instance.Active);
                        Assert.True(rows[testKey.ToString()].TimeStamp == instance.TimeStamp);
                    }
                }

                //Testing removing an extractor after it has been initialized
                //If an extractor has been initialized but then returns an empty row in the state, it will use the last seen log
                testKey = 2;
                RawLogData logCopy = rows[testKey.ToString()];
                rows.Remove(testKey.ToString());
                rows[_index] = new RawLogData(DateTime.UtcNow, false);

                rowsUpdateState = rows;

                await extractorManager.UpdateExtractorState();

                Assert.True(extractorManager._state.CurrentState.Count == 3);

                foreach (RawExtractorInstance instance in extractorManager._state.CurrentState)
                {
                    if (instance.Key == testKey)
                    {
                        //Checking that the previous value from the state is reused
                        Assert.True(logCopy.Active == instance.Active);
                        Assert.True(logCopy.TimeStamp == instance.TimeStamp);

                    }
                    else if (instance.Key == Int16.Parse(_index))
                    {
                        //Checking that the row at _index has been updated, to make sure that state was changed
                        Assert.True(rows[_index].Active == instance.Active);
                        Assert.True(rows[_index].TimeStamp == instance.TimeStamp);
                    }
                }

                //Inserting the removed extractor back and checking that the new value is used again
                rows[testKey.ToString()] = new RawLogData(DateTime.UtcNow, false);

                rowsUpdateState = rows;

                await extractorManager.UpdateExtractorState();

                foreach (RawExtractorInstance instance in extractorManager._state.CurrentState)
                {
                    if (instance.Key == testKey)
                    {
                        //Checking that the previous value from the state is reused
                        Assert.True(rows[testKey.ToString()].Active == instance.Active);
                        Assert.True(rows[testKey.ToString()].TimeStamp == instance.TimeStamp);

                    }
                }

                _failUpdateState = true;
                rows[_index] = new RawLogData(DateTime.UtcNow, !rows[_index].Active);
                rowsUpdateState = rows;

                await extractorManager.UpdateExtractorState();

                foreach (RawExtractorInstance instance in extractorManager._state.CurrentState)
                {
                    if (instance.Key == Int16.Parse(_index))
                    {
                        //Checking that the row at _index has been updated, to make sure that state was changed
                        Assert.False(rows[_index].Active == instance.Active);
                        Assert.False(rows[_index].TimeStamp == instance.TimeStamp);
                    }
                }
            }

            System.IO.File.Delete(path);
        }
        public async Task TestCheckForMultipleActiveExtractors()
        {
        }
        private class RawItem
        {
            public string key { get; set; }
            public RawLogData columns { get; set; }
        }
        private class RawItems
        {
            public List<RawItem> items { get; set; }
        }
        private static Dictionary<string, RawLogData> rowsUploadState = new Dictionary<string, RawLogData>();
        private static Dictionary<string, RawLogData> rowsUpdateState = new Dictionary<string, RawLogData>();
        private static async Task<HttpResponseMessage> mockInsertRowsAsync(HttpRequestMessage message, CancellationToken token)
        {
            var uri = message.RequestUri.ToString();

            Assert.Contains($"{_host}/api/v1/projects/{_project}/raw/dbs/{_dbName}/tables/{_tableName}/rows", uri);
            Assert.Contains("ensureParent=true", message.RequestUri.Query);

            var responseBody = "{ }";
            var statusCode = HttpStatusCode.OK;

            if (_failInsert)
            {
                statusCode = HttpStatusCode.InternalServerError;
            }
            else
            {
                var content = await message.Content.ReadAsStringAsync();
                var items = JsonSerializer.Deserialize<RawItems>(content,
                new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });

                foreach (var item in items.items) rowsUploadState[_index] = item.columns;
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

        private static async Task<HttpResponseMessage> mockGetRowsAsync(HttpRequestMessage message, CancellationToken token)
        {
            var uri = message.RequestUri.ToString();
            Assert.Contains($"{_host}/api/v1/projects/{_project}/raw/dbs/{_dbName}/tables/{_tableName}/rows", uri);

            var responseBody = "{ }";
            var statusCode = HttpStatusCode.OK;

            if (_failUpdateState)
            {
                statusCode = HttpStatusCode.InternalServerError;
            }
            else
            {
                RawRow<RawLogData> responseRows = new RawRow<RawLogData>();
                var rowList = new List<RawRow<RawLogData>>();

                foreach (KeyValuePair<string, RawLogData> entry in rowsUpdateState)
                {
                    rowList.Add(new RawRow<RawLogData>() { Key = entry.Key, Columns = entry.Value });
                }

                var finalContent = new ItemsWithCursor<RawRow<RawLogData>>
                {
                    Items = rowList
                };
                responseBody = JsonSerializer.Serialize(finalContent,
                new JsonSerializerOptions() { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });
            }

            var response = new HttpResponseMessage
            {
                StatusCode = statusCode,
                Content = new StringContent(responseBody)
            };
            response.Content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
            response.Headers.Add("x-request-id", "1");

            await Task.Delay(100);

            return response;
        }
    }
}
