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

        public RawManagerTest(ITestOutputHelper output)
        {
            _output = output;
        }

        [Fact]
        public async Task TestUploadLogToState()
        {
            string path = "test-upload-log-to-state-config";
            string[] config = {  "version: 2",
                                "logger:",
                                "  console:",
                                "    level: verbose",
                                "cognite:",
                               $"  project: {_project}",
                               $"  api-key: {_apiKey}",
                               $"  host: {_host}",
                                "manager:",
                               $"  index: {0}",
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

                rows.Clear();

                RawExtractorManager extractorManager = new RawExtractorManager(managerConfig, destination, logger, scheduler, source);
                await extractorManager.UploadLogToState();

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

        public async Task TestUpdateExtractorState()
        {
            string path = "test-update-extractor-state-config";
            string[] config = {  "version: 2",
                                "logger:",
                                "  console:",
                                "    level: verbose",
                                "cognite:",
                               $"  project: {_project}",
                               $"  api-key: {_apiKey}",
                               $"  host: {_host}",
                                "manager:",
                               $"  index: {0}",
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

                rows.Clear();
                rows.Add("0", new RawLogData(DateTime.UtcNow.Subtract(new TimeSpan(0, 0, 20)), false));
                rows.Add("1", new RawLogData(DateTime.UtcNow.Subtract(new TimeSpan(0, 0, 40)), false));
                rows.Add("2", new RawLogData(DateTime.UtcNow.Subtract(new TimeSpan(0, 0, 50)), true));

                Assert.True(extractorManager._state.CurrentState.Count == 0);

                await extractorManager.UpdateExtractorState();

                Assert.True(extractorManager._state.CurrentState.Count == 3);
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
        private static Dictionary<string, RawLogData> rows = new Dictionary<string, RawLogData>();
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

                foreach (var item in items.items) rows[_index] = item.columns;
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

                foreach (var item in items.items) rows[_index] = item.columns;
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
    }
}
