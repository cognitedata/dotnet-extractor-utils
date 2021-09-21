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
using Moq;
using Moq.Protected;
using Xunit;
using Cognite.Extractor.Logging;
using Cognite.Extractor.Utils;

namespace ExtractorUtils.Test.Unit
{
    public class CdfRawTest
    {

        private const string _authTenant = "someTenant";
        private const string _project = "someProject";
        private const string _apiKey = "someApiKey";
        private const string _host = "https://test.cognitedata.com";
        private const string _dbName = "testDb";
        private const string _tableName = "testTable";

        [Fact]
        public async Task TestInsertRow()
        {
            string path = "test-insert-raw-rows-config.yml";
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
                                "    raw: 2" };
            System.IO.File.WriteAllLines(path, lines);

            var mocks = TestUtilities.GetMockedHttpClientFactory(mockInsertRowsAsync);
            var mockHttpMessageHandler = mocks.handler;
            var mockFactory = mocks.factory;

            // Setup services
            var services = new ServiceCollection();
            services.AddSingleton<IHttpClientFactory>(mockFactory.Object); // inject the mock factory
            services.AddConfig<BaseConfig>(path, 2);
            services.AddLogger();
            services.AddCogniteClient("testApp");
            using (var provider = services.BuildServiceProvider()) {
                var cogniteDestination = provider.GetRequiredService<CogniteDestination>();

                var columns = new Dictionary<string, TestDto>{
                    { "A", new TestDto{ Name = "A", Number = 0} },
                    { "B", new TestDto{ Name = "B", Number = 1} },
                    { "C", new TestDto{ Name = "C", Number = 2} },
                    { "D", new TestDto{ Name = "D", Number = 3} },
                    { "E", new TestDto{ Name = "E", Number = 4} },
                    { "F", new TestDto{ Name = "F", Number = 5} }
                };

                await cogniteDestination.InsertRawRowsAsync(_dbName, _tableName, columns, CancellationToken.None);

                foreach (var kvp in columns)
                {
                    Assert.True(rows.TryGetValue(kvp.Key, out TestDto dto));
                    Assert.Equal(kvp.Value.Name, dto.Name);
                    Assert.Equal(kvp.Value.Number, dto.Number);
                }
            }

            // Verify that the endpoint was called 2 times (2 chunks of max 4 rows)
            mockHttpMessageHandler.Protected()
                .Verify<Task<HttpResponseMessage>>(
                    "SendAsync", 
                    Times.Exactly(2),
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>());


            System.IO.File.Delete(path);
        }

        [Fact]
        public async Task TestUploadQueue()
        {
            string path = "test-raw-queue-config.yml";
            string[] lines = {  "version: 2",
                                "logger:",
                                "  console:",
                                "    level: verbose",
                                "cognite:",
                               $"  project: {_project}",
                               $"  api-key: {_apiKey}",
                               $"  host: {_host}" };
            System.IO.File.WriteAllLines(path, lines);

            var mocks = TestUtilities.GetMockedHttpClientFactory(mockInsertRowsAsync);
            var mockHttpMessageHandler = mocks.handler;
            var mockFactory = mocks.factory;

            var services = new ServiceCollection();
            services.AddSingleton<IHttpClientFactory>(mockFactory.Object); // inject the mock factory
            services.AddConfig<BaseConfig>(path, 2);
            services.AddLogger();
            services.AddCogniteClient("testApp", setLogger: true, setMetrics: false);
            var index = 0;
            using (var source = new CancellationTokenSource())
            using (var provider = services.BuildServiceProvider()) {
                var cogniteDestination = provider.GetRequiredService<CogniteDestination>();
                var logger = provider.GetRequiredService<ILogger<CdfRawTest>>();
                // queue with 1 sec upload interval
                using (var queue = cogniteDestination.CreateRawUploadQueue<TestDto>(_dbName, _tableName, TimeSpan.FromSeconds(1)))
                {
                    var enqueueTask = Task.Run(async () => {
                        while (index < 13)
                        {
                            queue.EnqueueRow($"r{index}", new TestDto {Name = "Test", Number = index});
                            await Task.Delay(100, source.Token);
                            index++;
                        }
                    });
                    var uploadTask = queue.Start(source.Token);

                    // wait for either the enqueue task to finish or the upload task to fail
                    var t = Task.WhenAny(uploadTask, enqueueTask);
                    await t;
                    logger.LogInformation("Enqueueing task completed. Disposing of the upload queue");
                } // disposing the queue will upload any rows left and stop the upload loop
                logger.LogInformation("Upload queue disposed");

                // Verify that the endpoint was called at most 3 times (once per upload interval and once disposing)
                mockHttpMessageHandler.Protected()
                    .Verify<Task<HttpResponseMessage>>(
                        "SendAsync", 
                        Times.AtMost(4),
                        ItExpr.IsAny<HttpRequestMessage>(),
                        ItExpr.IsAny<CancellationToken>());

                // queue with maximum size
                using (var queue = cogniteDestination.CreateRawUploadQueue<TestDto>(_dbName, _tableName, TimeSpan.FromMinutes(10), 5))
                {
                    var enqueueTask = Task.Run(async () => {
                        while (index < 23)
                        {
                            queue.EnqueueRow($"r{index}", new TestDto {Name = "Test", Number = index});
                            await Task.Delay(100, source.Token);
                            index++;
                        }
                    });
                    var uploadTask = queue.Start(source.Token);

                    await enqueueTask;

                    // test cancelling the token;
                    source.Cancel();
                    await uploadTask;
                    Assert.True(uploadTask.IsCompleted);
                    logger.LogInformation("Enqueueing task cancelled. Disposing of the upload queue");
                }
                logger.LogInformation("Upload queue disposed");
                // Verify that the endpoint was called at most 3 more times (once per max size and once disposing)
                mockHttpMessageHandler.Protected()
                    .Verify<Task<HttpResponseMessage>>(
                        "SendAsync", 
                        Times.AtMost(8),
                        ItExpr.IsAny<HttpRequestMessage>(),
                        ItExpr.IsAny<CancellationToken>());

            }

            // verify all rows were sent to CDF
            for (int i = 0; i < index; ++i)
            {
                Assert.True(rows.TryGetValue($"r{i}", out TestDto dto));
                Assert.Equal(i, dto.Number);
            }

            System.IO.File.Delete(path);
        }

        private class TestDto
        {
            public string Name { get; set; }
            public int Number { get; set; }
        }

        private class RawItem
        {
            public string key { get; set; }
            public TestDto columns { get; set; } 
        }

        private class RawItems
        {
            public List<RawItem> items {get; set; }
        }

        private static Dictionary<string, TestDto> rows = new Dictionary<string, TestDto>();
        private static async Task<HttpResponseMessage> mockInsertRowsAsync(HttpRequestMessage message , CancellationToken token) {
            var uri = message.RequestUri.ToString();

            Assert.Contains($"{_host}/api/v1/projects/{_project}/raw/dbs/{_dbName}/tables/{_tableName}/rows", uri);
            Assert.Contains("ensureParent=true", message.RequestUri.Query);

            var responseBody = "{ }";
            var statusCode = HttpStatusCode.OK;
            var content = await message.Content.ReadAsStringAsync();
            var items = JsonSerializer.Deserialize<RawItems>(content);

            foreach (var item in items.items)
            {
                rows.Add(item.key, item.columns);
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