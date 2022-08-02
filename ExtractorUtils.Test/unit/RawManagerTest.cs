using System;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text.Json;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Xunit;
using Xunit.Abstractions;
using Cognite.Extractor.Utils;
using Cognite.Extractor.Testing;
using Cognite.Extractor.Common;
using CogniteSdk;

namespace ExtractorUtils.Test.Unit
{
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
            int index = 0;
            string path = "test-upload-log-to-state-config";
            SetupConfig(index, path);

            var mocks = TestUtilities.GetMockedHttpClientFactory(mockInsertRowsAsync);
            var mockHttpMessageHandler = mocks.handler;
            var mockFactory = mocks.factory;

            var services = new ServiceCollection();
            services.AddSingleton<IHttpClientFactory>(mockFactory.Object);
            services.AddConfig<MyTestConfig>(path, 2);
            services.AddTestLogging(_output);
            services.AddCogniteClient("testApp");

            using (var provider = services.BuildServiceProvider())
            {
                var logger = provider.GetRequiredService<ILogger<RawManagerTest>>();
                rows.Clear();

                var extractorManager = CreateRawExtractorManager(provider);

                await extractorManager.UploadLogToState();

                // Checking that the initial log has been inserted into db.
                Assert.True(rows.Count == 1);
                Assert.True(rows[index.ToString()].Active == false);
                Assert.True(rows[index.ToString()].TimeStamp < DateTime.UtcNow);

                // Updating the status.
                DateTime prevTimeStamp = rows[index.ToString()].TimeStamp;
                extractorManager._state.UpdatedStatus = true;

                await extractorManager.UploadLogToState();

                // Testing that the status has been changed.
                Assert.True(rows.Count == 1);
                Assert.True(rows[index.ToString()].Active == true);
                Assert.True(rows[index.ToString()].TimeStamp > prevTimeStamp);

                // Testing making the endpoint return an error.
                _failInsert = true;
                prevTimeStamp = rows[index.ToString()].TimeStamp;
                extractorManager._state.UpdatedStatus = false;

                await extractorManager.UploadLogToState();

                // Checking that the db remains unchanged after the error.
                Assert.True(rows[index.ToString()].Active == true);
                Assert.True(rows[index.ToString()].TimeStamp == prevTimeStamp);
            }

            System.IO.File.Delete(path);
        }

        [Fact]
        public async Task TestUpdateExtractorState()
        {
            int index = 0;
            string path = "test-update-extractor-state-config";
            SetupConfig(index, path);

            var mocks = TestUtilities.GetMockedHttpClientFactory(mockGetRowsAsync);
            var mockHttpMessageHandler = mocks.handler;
            var mockFactory = mocks.factory;

            var services = new ServiceCollection();
            services.AddSingleton<IHttpClientFactory>(mockFactory.Object);
            services.AddConfig<MyTestConfig>(path, 2);
            services.AddTestLogging(_output);
            services.AddCogniteClient("testApp");

            using (var provider = services.BuildServiceProvider())
            {
                var logger = provider.GetRequiredService<ILogger<RawManagerTest>>();

                var extractorManager = CreateRawExtractorManager(provider);

                rows.Clear();
                rows.Add("0", new RawLogData(DateTime.UtcNow, true));
                rows.Add("1", new RawLogData(DateTime.UtcNow.Subtract(new TimeSpan(0, 0, 10)), false));
                rows.Add("2", new RawLogData(DateTime.UtcNow.Subtract(new TimeSpan(0, 0, 30)), false));
                rows.Add("3", new RawLogData(DateTime.UtcNow.Subtract(new TimeSpan(0, 0, 50)), false));

                Assert.True(extractorManager._state.CurrentState.Count == 0);

                await extractorManager.UpdateExtractorState();

                // Testing that the state has changed.
                Assert.True(extractorManager._state.CurrentState.Count == 4);

                // Checking that each value in the state is the same as in the db.
                foreach (RawExtractorInstance instance in extractorManager._state.CurrentState)
                {
                    string key = instance.Index.ToString();
                    if (rows.ContainsKey(key))
                    {
                        Assert.True(rows[key].Active == instance.Active);
                        Assert.True(rows[key].TimeStamp == instance.TimeStamp);
                    }
                }

                // Testing updating the active status and timestamp for a given extractor.
                string testKey = "1";
                rows[testKey] = new RawLogData(DateTime.UtcNow, true);

                await extractorManager.UpdateExtractorState();

                foreach (RawExtractorInstance instance in extractorManager._state.CurrentState)
                {
                    if (instance.Index == Int16.Parse(testKey))
                    {
                        // Checking that the valus has been changed for the given extractor.
                        Assert.True(rows[testKey].Active == instance.Active);
                        Assert.True(rows[testKey].TimeStamp == instance.TimeStamp);
                    }
                }

                // Testing removing an extractor after it has been initialized.
                // If an extractor has been initialized but then returns an empty
                // row in the state it will use the last seen log.
                testKey = "2";
                RawLogData logCopy = rows[testKey];
                rows.Remove(testKey);

                await extractorManager.UpdateExtractorState();

                // Checking that the state still has all the rows.
                Assert.True(extractorManager._state.CurrentState.Count == 4);

                foreach (RawExtractorInstance instance in extractorManager._state.CurrentState)
                {
                    if (instance.Index == Int16.Parse(testKey))
                    {
                        // Checking that the previous value from the state is reused.
                        Assert.True(logCopy.Active == instance.Active);
                        Assert.True(logCopy.TimeStamp == instance.TimeStamp);

                    }
                }

                // Inserting the removed extractor back and checking that the new value is used again.
                rows[testKey] = new RawLogData(DateTime.UtcNow, false);

                await extractorManager.UpdateExtractorState();

                foreach (RawExtractorInstance instance in extractorManager._state.CurrentState)
                {
                    if (instance.Index == Int16.Parse(testKey))
                    {
                        // Checking that the removed value has been replaced.
                        Assert.True(rows[testKey].Active == instance.Active);
                        Assert.True(rows[testKey].TimeStamp == instance.TimeStamp);

                    }
                }

                // Testing making the endpoint return an error.
                _failUpdateState = true;
                testKey = "3";
                rows[testKey] = new RawLogData(DateTime.UtcNow, !rows[testKey].Active);

                await extractorManager.UpdateExtractorState();

                foreach (RawExtractorInstance instance in extractorManager._state.CurrentState)
                {
                    if (instance.Index == Int16.Parse(testKey))
                    {
                        // Checking that if the endpoint fails the current state will remain unchanged.
                        Assert.False(rows[testKey].Active == instance.Active);
                        Assert.False(rows[testKey].TimeStamp == instance.TimeStamp);
                    }
                }
            }

            System.IO.File.Delete(path);
        }

        [Fact]
        public void TestCheckForMultipleActiveExtractors()
        {
            int index = 1;
            string path = "test-multiple-active-extractors-config";
            SetupConfig(index, path);

            var services = new ServiceCollection();
            services.AddConfig<MyTestConfig>(path, 2);
            services.AddTestLogging(_output);
            services.AddCogniteClient("testApp");

            using (var provider = services.BuildServiceProvider())
            {
                var logger = provider.GetRequiredService<ILogger<RawManagerTest>>();

                var source1 = new CancellationTokenSource();
                var extractorManager = CreateRawExtractorManager(provider, source1);

                List<IExtractorInstance> extractorInstances = new List<IExtractorInstance>();
                extractorInstances.Add(new RawExtractorInstance(0, DateTime.UtcNow, true));
                extractorInstances.Add(new RawExtractorInstance(1, DateTime.UtcNow, true));
                extractorInstances.Add(new RawExtractorInstance(2, DateTime.UtcNow.Subtract(new TimeSpan(0, 0, 50)), false));

                extractorManager._state.CurrentState = extractorInstances;
                extractorManager.CheckForMultipleActiveExtractors();

                // Extractor 1 will be cancelled because both 0 and 1 are active, where 0 has higher priority.
                Assert.True(source1.IsCancellationRequested);

                var source2 = new CancellationTokenSource();
                extractorManager = CreateRawExtractorManager(provider, source2);

                extractorInstances.Clear();
                extractorInstances.Add(new RawExtractorInstance(0, DateTime.UtcNow, true));
                extractorInstances.Add(new RawExtractorInstance(1, DateTime.UtcNow.Subtract(new TimeSpan(0, 0, 20)), true));
                extractorInstances.Add(new RawExtractorInstance(2, DateTime.UtcNow.Subtract(new TimeSpan(0, 0, 50)), true));

                extractorManager._state.CurrentState = extractorInstances;
                extractorManager.CheckForMultipleActiveExtractors();

                // Extractor 1 will not be cancelled because it is not active.
                Assert.False(source2.IsCancellationRequested);

                var source3 = new CancellationTokenSource();
                extractorManager = CreateRawExtractorManager(provider, source3);

                extractorInstances.Clear();
                extractorInstances.Add(new RawExtractorInstance(0, DateTime.UtcNow, true));
                extractorInstances.Add(new RawExtractorInstance(1, DateTime.UtcNow, true));
                extractorInstances.Add(new RawExtractorInstance(2, DateTime.UtcNow, true));

                extractorManager._state.CurrentState = extractorInstances;
                extractorManager.CheckForMultipleActiveExtractors();

                // Extractor 1 will be cancelled because there are multiple active extractors and 0 has higher priority.
                Assert.True(source3.IsCancellationRequested);

                var source4 = new CancellationTokenSource();
                extractorManager = CreateRawExtractorManager(provider, source4);

                extractorInstances.Clear();
                extractorInstances.Add(new RawExtractorInstance(1, DateTime.UtcNow, true));
                extractorInstances.Add(new RawExtractorInstance(2, DateTime.UtcNow, true));
                extractorInstances.Add(new RawExtractorInstance(3, DateTime.UtcNow, true));

                extractorManager._state.CurrentState = extractorInstances;
                extractorManager.CheckForMultipleActiveExtractors();

                // Extractor 1 will not be cancelled because it has higher priority than 2 and 3.
                Assert.False(source4.IsCancellationRequested);
            }

            System.IO.File.Delete(path);
        }

        [Fact]
        public void TestShouldBecomeActive()
        {
            int index = 1;
            string path = "test-should-become-active-config";
            SetupConfig(index, path);

            var services = new ServiceCollection();
            services.AddConfig<MyTestConfig>(path, 2);
            services.AddTestLogging(_output);
            services.AddCogniteClient("testApp");

            using (var provider = services.BuildServiceProvider())
            {
                var logger = provider.GetRequiredService<ILogger<RawManagerTest>>();

                var extractorManager = CreateRawExtractorManager(provider);

                List<IExtractorInstance> extractorInstances = new List<IExtractorInstance>();
                extractorInstances.Add(new RawExtractorInstance(1, DateTime.UtcNow, false));
                extractorInstances.Add(new RawExtractorInstance(2, DateTime.UtcNow.Subtract(new TimeSpan(0, 0, 30)), false));
                extractorInstances.Add(new RawExtractorInstance(3, DateTime.UtcNow.Subtract(new TimeSpan(0, 0, 30)), false));
                extractorManager._state.CurrentState = extractorInstances;

                // Extractor 1 will become active because 2 and 3 are not within the time threshold.
                Assert.True(extractorManager.ShouldBecomeActive());

                extractorInstances.Clear();
                extractorInstances.Add(new RawExtractorInstance(1, DateTime.UtcNow, false));
                extractorInstances.Add(new RawExtractorInstance(2, DateTime.UtcNow, true));
                extractorInstances.Add(new RawExtractorInstance(3, DateTime.UtcNow.Subtract(new TimeSpan(0, 0, 30)), false));
                extractorManager._state.CurrentState = extractorInstances;

                // Extractor 1 will not become active because 2 is already active and within the time threshold.
                Assert.False(extractorManager.ShouldBecomeActive());

                extractorInstances.Clear();
                extractorInstances.Add(new RawExtractorInstance(1, DateTime.UtcNow, false));
                extractorInstances.Add(new RawExtractorInstance(2, DateTime.UtcNow.Subtract(new TimeSpan(0, 0, 30)), true));
                extractorInstances.Add(new RawExtractorInstance(3, DateTime.UtcNow.Subtract(new TimeSpan(0, 0, 30)), true));
                extractorManager._state.CurrentState = extractorInstances;

                // Extractor 1 will become active because 2 and 3 are over the time threshold.
                Assert.True(extractorManager.ShouldBecomeActive());

                extractorInstances.Clear();
                extractorInstances.Add(new RawExtractorInstance(0, DateTime.UtcNow, false));
                extractorInstances.Add(new RawExtractorInstance(1, DateTime.UtcNow, false));
                extractorInstances.Add(new RawExtractorInstance(2, DateTime.UtcNow.Subtract(new TimeSpan(0, 0, 30)), true));
                extractorManager._state.CurrentState = extractorInstances;

                // Extractor 1 will not become active because 0 has higher priority.
                Assert.False(extractorManager.ShouldBecomeActive());

                extractorInstances.Clear();
                extractorManager._state.CurrentState = extractorInstances;

                // Extractor 1 will not become active because the state is empty.
                Assert.False(extractorManager.ShouldBecomeActive());
            }

            System.IO.File.Delete(path);
        }

        private void SetupConfig(int index, string path)
        {
            string[] config = {
                    "version: 2",
                    "logger:",
                    "  console:",
                    "    level: verbose",
                    "cognite:",
                    $"  project: {_project}",
                    $"  api-key: {_apiKey}",
                    $"  host: {_host}",
                    "high-availability:",
                    $"  index: {index}",
                    $"  raw:",
                    $"    database-name: {_dbName}",
                    $"    table-name: {_tableName}"};
            System.IO.File.WriteAllLines(path, config);
        }

        private RawHighAvailabilityManager CreateRawExtractorManager(ServiceProvider provider, CancellationTokenSource source = null)
        {
            var managerConfig = provider.GetRequiredService<HighAvailabilityConfig>();
            var destination = provider.GetRequiredService<CogniteDestination>();
            var logger = provider.GetRequiredService<ILogger<RawHighAvailabilityManager>>();
            if (source == null) source = new CancellationTokenSource();
            var scheduler = new PeriodicScheduler(source.Token);
            var inactivityThreshold = new TimeSpan(0, 0, 10);

            RawHighAvailabilityManager extractorManager = new RawHighAvailabilityManager(managerConfig, destination, logger, scheduler, source, inactivityThreshold: inactivityThreshold);

            return extractorManager;
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

        private static Dictionary<string, RawLogData> rows = new Dictionary<string, RawLogData>();

        private static async Task<HttpResponseMessage> mockInsertRowsAsync(HttpRequestMessage message, CancellationToken token)
        {
            var uri = message.RequestUri.ToString();
            int index = 0;

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

                foreach (var item in items.items) rows[index.ToString()] = item.columns;
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

                foreach (KeyValuePair<string, RawLogData> entry in rows)
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

            await Task.Delay(200);

            return response;
        }
    }

    class MyTestConfig : BaseConfig
    {
        public HighAvailabilityConfig HighAvailability { get; set; }
    }
}
