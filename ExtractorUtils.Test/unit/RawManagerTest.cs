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
            string index = "0";
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
                               $"  index: {index}",
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
                Assert.True(rows[index].Active == false);
                Assert.True(rows[index].TimeStamp < DateTime.UtcNow);

                DateTime prevTimeStamp = rows[index].TimeStamp;
                extractorManager._state.UpdatedStatus = true;

                await extractorManager.UploadLogToState();

                rows = rowsUploadState;

                Assert.True(rows.Count == 1);
                Assert.True(rows[index].Active == true);
                Assert.True(rows[index].TimeStamp > prevTimeStamp);

                _failInsert = true;
                prevTimeStamp = rows[index].TimeStamp;
                extractorManager._state.UpdatedStatus = false;

                await extractorManager.UploadLogToState();

                rows = rowsUploadState;

                Assert.True(rows[index].Active == true);
                Assert.True(rows[index].TimeStamp == prevTimeStamp);
            }

            System.IO.File.Delete(path);
        }
        [Fact]
        public async Task TestUpdateExtractorState()
        {
            string index = "0";
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
                               $"  index: {index}",
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
                rows.Add("3", new RawLogData(DateTime.UtcNow.Subtract(new TimeSpan(0, 0, 50)), false));
                rows.Add("4", new RawLogData(DateTime.UtcNow.Subtract(new TimeSpan(0, 0, 70)), false));
                rowsUpdateState = rows;

                Assert.True(extractorManager._state.CurrentState.Count == 0);

                await extractorManager.UpdateExtractorState();

                //Testing that the state has changed 
                Assert.True(extractorManager._state.CurrentState.Count == 5);

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
                rows[index] = new RawLogData(DateTime.UtcNow, false);

                rowsUpdateState = rows;

                await extractorManager.UpdateExtractorState();

                Assert.True(extractorManager._state.CurrentState.Count == 5);

                foreach (RawExtractorInstance instance in extractorManager._state.CurrentState)
                {
                    if (instance.Key == testKey)
                    {
                        //Checking that the previous value from the state is reused
                        Assert.True(logCopy.Active == instance.Active);
                        Assert.True(logCopy.TimeStamp == instance.TimeStamp);

                    }
                    else if (instance.Key == Int16.Parse(index))
                    {
                        //Checking that the row at _index has been updated, to make sure that state was changed
                        Assert.True(rows[index].Active == instance.Active);
                        Assert.True(rows[index].TimeStamp == instance.TimeStamp);
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
                rows[index] = new RawLogData(DateTime.UtcNow, !rows[index].Active);
                rowsUpdateState = rows;

                await extractorManager.UpdateExtractorState();

                foreach (RawExtractorInstance instance in extractorManager._state.CurrentState)
                {
                    if (instance.Key == Int16.Parse(index))
                    {
                        //Checking that if the method fails the current state will remain unchanged
                        Assert.False(rows[index].Active == instance.Active);
                        Assert.False(rows[index].TimeStamp == instance.TimeStamp);
                    }
                }
            }

            System.IO.File.Delete(path);
        }
        [Fact]
        public void TestCheckForMultipleActiveExtractors()
        {
            string index = "1";
            string path = "test-multiple-active-extractors-config";
            string[] config = { "version: 2",
                                "logger:",
                                "  console:",
                                "    level: verbose",
                                "cognite:",
                               $"  project: {_project}",
                               $"  api-key: {_apiKey}",
                               $"  host: {_host}",
                                "manager:",
                               $"  index: {index}",
                               $"  database-name: {_dbName}",
                               $"  table-name: {_tableName}"};
            System.IO.File.WriteAllLines(path, config);

            var services = new ServiceCollection();
            services.AddConfig<MyConfig>(path, 2);
            services.AddTestLogging(_output);
            services.AddCogniteClient("testApp");

            using (var provider = services.BuildServiceProvider())
            {
                var testLogger = provider.GetRequiredService<ILogger<RawManagerTest>>();

                var managerConfig = provider.GetRequiredService<RawManagerConfig>();
                var destination = provider.GetRequiredService<CogniteDestination>();
                var logger = provider.GetRequiredService<ILogger<RawExtractorManager>>();
                var source1 = new CancellationTokenSource();
                var scheduler = new PeriodicScheduler(source1.Token);

                RawExtractorManager extractorManager = new RawExtractorManager(managerConfig, destination, logger, scheduler, source1);
                extractorManager.InactivityThreshold = new TimeSpan(0, 0, 10);

                List<IExtractorInstance> extractorInstances = new List<IExtractorInstance>();
                extractorInstances.Add(new RawExtractorInstance(0, DateTime.UtcNow, true));
                extractorInstances.Add(new RawExtractorInstance(1, DateTime.UtcNow, true));
                extractorInstances.Add(new RawExtractorInstance(2, DateTime.UtcNow.Subtract(new TimeSpan(0, 0, 50)), false));

                extractorManager._state.CurrentState = extractorInstances;
                extractorManager.CheckForMultipleActiveExtractors();

                Assert.True(source1.IsCancellationRequested);

                var source2 = new CancellationTokenSource();
                extractorManager = new RawExtractorManager(managerConfig, destination, logger, scheduler, source2);

                extractorInstances.Clear();
                extractorInstances.Add(new RawExtractorInstance(0, DateTime.UtcNow, true));
                extractorInstances.Add(new RawExtractorInstance(1, DateTime.UtcNow, false));
                extractorInstances.Add(new RawExtractorInstance(2, DateTime.UtcNow.Subtract(new TimeSpan(0, 0, 50)), true));

                extractorManager._state.CurrentState = extractorInstances;
                extractorManager.CheckForMultipleActiveExtractors();

                Assert.False(source2.IsCancellationRequested);

                var source3 = new CancellationTokenSource();
                extractorManager = new RawExtractorManager(managerConfig, destination, logger, scheduler, source3);

                extractorInstances.Clear();
                extractorInstances.Add(new RawExtractorInstance(0, DateTime.UtcNow, true));
                extractorInstances.Add(new RawExtractorInstance(1, DateTime.UtcNow, true));
                extractorInstances.Add(new RawExtractorInstance(2, DateTime.UtcNow, true));

                extractorManager._state.CurrentState = extractorInstances;
                extractorManager.CheckForMultipleActiveExtractors();

                Assert.True(source3.IsCancellationRequested);

                var source4 = new CancellationTokenSource();
                extractorManager = new RawExtractorManager(managerConfig, destination, logger, scheduler, source4);

                extractorInstances.Clear();
                extractorInstances.Add(new RawExtractorInstance(1, DateTime.UtcNow, true));
                extractorInstances.Add(new RawExtractorInstance(2, DateTime.UtcNow, true));
                extractorInstances.Add(new RawExtractorInstance(3, DateTime.UtcNow, true));

                extractorManager._state.CurrentState = extractorInstances;
                extractorManager.CheckForMultipleActiveExtractors();

                Assert.False(source4.IsCancellationRequested);
            }

            System.IO.File.Delete(path);
        }
        [Fact]
        public void TestShouldBecomeActive()
        {
            string index = "1";
            string path = "test-should-become-active-config";
            string[] config = { "version: 2",
                                "logger:",
                                "  console:",
                                "    level: verbose",
                                "cognite:",
                               $"  project: {_project}",
                               $"  api-key: {_apiKey}",
                               $"  host: {_host}",
                                "manager:",
                               $"  index: {index}",
                               $"  database-name: {_dbName}",
                               $"  table-name: {_tableName}"};
            System.IO.File.WriteAllLines(path, config);

            var services = new ServiceCollection();
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
                extractorManager.InactivityThreshold = new TimeSpan(0, 0, 10);

                List<IExtractorInstance> extractorInstances = new List<IExtractorInstance>();
                extractorInstances.Add(new RawExtractorInstance(1, DateTime.UtcNow, false));
                extractorInstances.Add(new RawExtractorInstance(2, DateTime.UtcNow.Subtract(new TimeSpan(0, 0, 60)), false));
                extractorInstances.Add(new RawExtractorInstance(3, DateTime.UtcNow.Subtract(new TimeSpan(0, 0, 60)), false));
                extractorManager._state.CurrentState = extractorInstances;

                //Extractor 1 will become active because 2 and 3 are not within the threshold
                Assert.True(extractorManager.ShouldBecomeActive());

                extractorInstances.Clear();
                extractorInstances.Add(new RawExtractorInstance(1, DateTime.UtcNow, false));
                extractorInstances.Add(new RawExtractorInstance(2, DateTime.UtcNow, true));
                extractorInstances.Add(new RawExtractorInstance(3, DateTime.UtcNow.Subtract(new TimeSpan(0, 0, 60)), false));
                extractorManager._state.CurrentState = extractorInstances;

                //Extractor 1 will not become active because 2 is already active and within threshold
                Assert.False(extractorManager.ShouldBecomeActive());

                extractorInstances.Clear();
                extractorInstances.Add(new RawExtractorInstance(1, DateTime.UtcNow, false));
                extractorInstances.Add(new RawExtractorInstance(2, DateTime.UtcNow.Subtract(new TimeSpan(0, 0, 60)), true));
                extractorInstances.Add(new RawExtractorInstance(3, DateTime.UtcNow.Subtract(new TimeSpan(0, 0, 60)), true));
                extractorManager._state.CurrentState = extractorInstances;

                //Extractor 1 will become active because 2 and 3 are over the threshold
                Assert.True(extractorManager.ShouldBecomeActive());

                extractorInstances.Clear();
                extractorInstances.Add(new RawExtractorInstance(0, DateTime.UtcNow, false));
                extractorInstances.Add(new RawExtractorInstance(1, DateTime.UtcNow, false));
                extractorInstances.Add(new RawExtractorInstance(2, DateTime.UtcNow.Subtract(new TimeSpan(0, 0, 60)), true));
                extractorManager._state.CurrentState = extractorInstances;

                //Extractor 1 will not become active because 0 has higher priority
                Assert.False(extractorManager.ShouldBecomeActive());

                extractorInstances.Clear();
                extractorManager._state.CurrentState = extractorInstances;

                //Extractor 1 will not become active because the state is empty
                Assert.False(extractorManager.ShouldBecomeActive());
            }

            System.IO.File.Delete(path);
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
            string index = "0";

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

                foreach (var item in items.items) rowsUploadState[index] = item.columns;
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
