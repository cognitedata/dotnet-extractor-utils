using Cognite.Extractor.Testing;
using Cognite.Extractor.Testing.Mock;
using Cognite.Extractor.Utils;
using CogniteSdk.Beta;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Moq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace ExtractorUtils.Test.Unit
{
    public class StreamRecordTest
    {
        private const string _project = "someProject";
        private const string _host = "https://test.cognitedata.com";

        private readonly ITestOutputHelper _output;
        private readonly List<(string streamId, StreamRecordWrite record)> _ingestedRecords = new List<(string, StreamRecordWrite)>();
        private bool _failInsert;

        public StreamRecordTest(ITestOutputHelper output)
        {
            _output = output;
        }

        [Fact]
        public async Task TestStreamRecordUploadQueueTimed()
        {
            string path = "test-stream-record-queue-timed-config.yml";
            string[] lines = {
                "version: 2",
                "logger:",
                "  console:",
                "    level: verbose",
                "cognite:",
                $"  project: {_project}",
                $"  host: {_host}",
                "  cdf-chunking:",
                "    stream-records: 100",
                "  cdf-throttling:",
                "    stream-records: 2"
            };
            System.IO.File.WriteAllLines(path, lines);

            var mocks = TestUtilities.GetMockedHttpClientFactory(MockStreamRecordsSendAsync);
            var mockFactory = mocks.factory;

            var services = new ServiceCollection();
            services.AddSingleton<IHttpClientFactory>(mockFactory.Object);
            services.AddConfig<BaseConfig>(path, 2);
            services.AddTestLogging(_output);
            services.AddCogniteClient("testApp", setLogger: true, setMetrics: false);

            int recordCount = 0;
            int cbCount = 0;

            using (var source = new CancellationTokenSource())
            using (var provider = services.BuildServiceProvider())
            {
                var cogniteDestination = provider.GetRequiredService<CogniteDestination>();
                var logger = provider.GetRequiredService<ILogger<StreamRecordTest>>();

                // Queue with 1 sec upload interval
                await using (var queue = cogniteDestination.CreateStreamRecordUploadQueue(
                    TimeSpan.FromSeconds(1), 0, res =>
                    {
                        recordCount += res.Uploaded?.Count() ?? 0;
                        cbCount++;
                        return Task.CompletedTask;
                    }))
                {
                    var index = 0;
                    var enqueueTask = Task.Run(async () =>
                    {
                        while (index < 13)
                        {
                            queue.Enqueue("test-stream-1", new StreamRecordWrite());
                            await Task.Delay(100, source.Token);
                            index++;
                        }
                    });

                    var uploadTask = queue.Start(source.Token);
                    var t = Task.WhenAny(uploadTask, enqueueTask);
                    await t;
                    logger.LogInformation("Enqueueing task completed. Disposing of the upload queue");
                }
                logger.LogInformation("Upload queue disposed");

                Assert.Equal(13, recordCount);
                Assert.True(cbCount >= 2, $"Expected at least 2 callbacks, got {cbCount}");
            }

            System.IO.File.Delete(path);
            _ingestedRecords.Clear();
        }

        [Fact]
        public async Task TestStreamRecordUploadQueueMaxSize()
        {
            string path = "test-stream-record-queue-maxsize-config.yml";
            string[] lines = {
                "version: 2",
                "logger:",
                "  console:",
                "    level: verbose",
                "cognite:",
                $"  project: {_project}",
                $"  host: {_host}",
                "  cdf-chunking:",
                "    stream-records: 100",
                "  cdf-throttling:",
                "    stream-records: 2"
            };
            System.IO.File.WriteAllLines(path, lines);

            var mocks = TestUtilities.GetMockedHttpClientFactory(MockStreamRecordsSendAsync);
            var mockFactory = mocks.factory;

            var services = new ServiceCollection();
            services.AddSingleton<IHttpClientFactory>(mockFactory.Object);
            services.AddConfig<BaseConfig>(path, 2);
            services.AddTestLogging(_output);
            services.AddCogniteClient("testApp", setLogger: true, setMetrics: false);

            int recordCount = 0;
            int cbCount = 0;

            using (var source = new CancellationTokenSource())
            using (var provider = services.BuildServiceProvider())
            {
                var cogniteDestination = provider.GetRequiredService<CogniteDestination>();
                var logger = provider.GetRequiredService<ILogger<StreamRecordTest>>();

                // Queue with max size of 5 (long interval to ensure size-based triggering)
                await using (var queue = cogniteDestination.CreateStreamRecordUploadQueue(
                    TimeSpan.FromMinutes(10), 5, res =>
                    {
                        recordCount += res.Uploaded?.Count() ?? 0;
                        cbCount++;
                        return Task.CompletedTask;
                    }))
                {
                    var index = 0;
                    var enqueueTask = Task.Run(async () =>
                    {
                        while (index < 23)
                        {
                            queue.Enqueue("test-stream-1", new StreamRecordWrite());
                            await Task.Delay(100, source.Token);
                            index++;
                        }
                    });

                    var uploadTask = queue.Start(source.Token);
                    await enqueueTask;

                    // Cancel the token
                    source.Cancel();
                    await uploadTask;
                    Assert.True(uploadTask.IsCompleted);
                    logger.LogInformation("Enqueueing task cancelled. Disposing of the upload queue");
                }
                logger.LogInformation("Upload queue disposed");

                Assert.Equal(23, recordCount);
                // With max size of 5 and 23 records, we should have at least 4 callbacks (23/5 = 4.6)
                Assert.True(cbCount >= 4, $"Expected at least 4 callbacks, got {cbCount}");
            }

            System.IO.File.Delete(path);
            _ingestedRecords.Clear();
        }

        [Fact]
        public async Task TestStreamRecordUploadQueueMultipleStreams()
        {
            string path = "test-stream-record-queue-multistream-config.yml";
            string[] lines = {
                "version: 2",
                "logger:",
                "  console:",
                "    level: verbose",
                "cognite:",
                $"  project: {_project}",
                $"  host: {_host}",
                "  cdf-chunking:",
                "    stream-records: 100",
                "  cdf-throttling:",
                "    stream-records: 2"
            };
            System.IO.File.WriteAllLines(path, lines);

            var mocks = TestUtilities.GetMockedHttpClientFactory(MockStreamRecordsSendAsync);
            var mockFactory = mocks.factory;

            var services = new ServiceCollection();
            services.AddSingleton<IHttpClientFactory>(mockFactory.Object);
            services.AddConfig<BaseConfig>(path, 2);
            services.AddTestLogging(_output);
            services.AddCogniteClient("testApp", setLogger: true, setMetrics: false);

            using (var source = new CancellationTokenSource())
            using (var provider = services.BuildServiceProvider())
            {
                var cogniteDestination = provider.GetRequiredService<CogniteDestination>();
                var logger = provider.GetRequiredService<ILogger<StreamRecordTest>>();

                await using (var queue = cogniteDestination.CreateStreamRecordUploadQueue(
                    TimeSpan.Zero, 0, null))
                {
                    // Enqueue records to multiple streams
                    queue.Enqueue("stream-a", new StreamRecordWrite());
                    queue.Enqueue("stream-b", new StreamRecordWrite());
                    queue.Enqueue("stream-a", new StreamRecordWrite());
                    queue.Enqueue("stream-c", new StreamRecordWrite());
                    queue.Enqueue("stream-b", new StreamRecordWrite());

                    // Trigger manually
                    var result = await queue.Trigger(CancellationToken.None);

                    Assert.NotNull(result.Uploaded);
                    Assert.Equal(5, result.Uploaded.Count());

                    // Verify records were grouped correctly by stream
                    var streamARecords = _ingestedRecords.Where(r => r.streamId == "stream-a").ToList();
                    var streamBRecords = _ingestedRecords.Where(r => r.streamId == "stream-b").ToList();
                    var streamCRecords = _ingestedRecords.Where(r => r.streamId == "stream-c").ToList();

                    Assert.Equal(2, streamARecords.Count);
                    Assert.Equal(2, streamBRecords.Count);
                    Assert.Single(streamCRecords);
                }
            }

            System.IO.File.Delete(path);
            _ingestedRecords.Clear();
        }

        [Fact]
        public async Task TestStreamRecordUploadQueueEnqueueMultiple()
        {
            string path = "test-stream-record-queue-enqueue-multi-config.yml";
            string[] lines = {
                "version: 2",
                "logger:",
                "  console:",
                "    level: verbose",
                "cognite:",
                $"  project: {_project}",
                $"  host: {_host}",
                "  cdf-chunking:",
                "    stream-records: 100",
                "  cdf-throttling:",
                "    stream-records: 2"
            };
            System.IO.File.WriteAllLines(path, lines);

            var mocks = TestUtilities.GetMockedHttpClientFactory(MockStreamRecordsSendAsync);
            var mockFactory = mocks.factory;

            var services = new ServiceCollection();
            services.AddSingleton<IHttpClientFactory>(mockFactory.Object);
            services.AddConfig<BaseConfig>(path, 2);
            services.AddTestLogging(_output);
            services.AddCogniteClient("testApp", setLogger: true, setMetrics: false);

            using (var provider = services.BuildServiceProvider())
            {
                var cogniteDestination = provider.GetRequiredService<CogniteDestination>();

                await using (var queue = cogniteDestination.CreateStreamRecordUploadQueue(
                    TimeSpan.Zero, 0, null))
                {
                    // Enqueue multiple records at once
                    var records = new List<StreamRecordWrite>
                    {
                        new StreamRecordWrite(),
                        new StreamRecordWrite(),
                        new StreamRecordWrite()
                    };
                    queue.Enqueue("test-stream", records);

                    var result = await queue.Trigger(CancellationToken.None);

                    Assert.NotNull(result.Uploaded);
                    Assert.Equal(3, result.Uploaded.Count());
                    Assert.Equal(3, _ingestedRecords.Count);
                }
            }

            System.IO.File.Delete(path);
            _ingestedRecords.Clear();
        }

        [Fact]
        public async Task TestStreamRecordUploadQueueError()
        {
            string path = "test-stream-record-queue-error-config.yml";
            string[] lines = {
                "version: 2",
                "logger:",
                "  console:",
                "    level: verbose",
                "cognite:",
                $"  project: {_project}",
                $"  host: {_host}",
                "  cdf-chunking:",
                "    stream-records: 100",
                "  cdf-throttling:",
                "    stream-records: 2"
            };
            System.IO.File.WriteAllLines(path, lines);

            var mocks = TestUtilities.GetMockedHttpClientFactory(MockStreamRecordsSendAsync);
            var mockFactory = mocks.factory;

            var services = new ServiceCollection();
            services.AddSingleton<IHttpClientFactory>(mockFactory.Object);
            services.AddConfig<BaseConfig>(path, 2);
            services.AddTestLogging(_output);
            services.AddCogniteClient("testApp", setLogger: true, setMetrics: false);

            using (var provider = services.BuildServiceProvider())
            {
                var cogniteDestination = provider.GetRequiredService<CogniteDestination>();

                await using (var queue = cogniteDestination.CreateStreamRecordUploadQueue(
                    TimeSpan.Zero, 0, null))
                {
                    queue.Enqueue("test-stream", new StreamRecordWrite());

                    _failInsert = true;
                    var result = await queue.Trigger(CancellationToken.None);
                    _failInsert = false;

                    Assert.True(result.IsFailed);
                    Assert.NotNull(result.Exception);
                }
            }

            System.IO.File.Delete(path);
            _ingestedRecords.Clear();
        }

        [Fact]
        public async Task TestStreamRecordUploadQueueEmptyUpload()
        {
            string path = "test-stream-record-queue-empty-config.yml";
            string[] lines = {
                "version: 2",
                "logger:",
                "  console:",
                "    level: verbose",
                "cognite:",
                $"  project: {_project}",
                $"  host: {_host}",
                "  cdf-chunking:",
                "    stream-records: 100",
                "  cdf-throttling:",
                "    stream-records: 2"
            };
            System.IO.File.WriteAllLines(path, lines);

            var mocks = TestUtilities.GetMockedHttpClientFactory(MockStreamRecordsSendAsync);
            var mockFactory = mocks.factory;

            var services = new ServiceCollection();
            services.AddSingleton<IHttpClientFactory>(mockFactory.Object);
            services.AddConfig<BaseConfig>(path, 2);
            services.AddTestLogging(_output);
            services.AddCogniteClient("testApp", setLogger: true, setMetrics: false);

            using (var provider = services.BuildServiceProvider())
            {
                var cogniteDestination = provider.GetRequiredService<CogniteDestination>();

                await using (var queue = cogniteDestination.CreateStreamRecordUploadQueue(
                    TimeSpan.Zero, 0, null))
                {
                    // Trigger without enqueueing anything
                    var result = await queue.Trigger(CancellationToken.None);

                    Assert.False(result.IsFailed);
                    Assert.NotNull(result.Uploaded);
                    Assert.Empty(result.Uploaded);
                }
            }

            System.IO.File.Delete(path);
        }

        [Fact]
        public async Task TestStreamRecordUploadQueuePartialFailure()
        {
            // This test simulates a scenario where one stream fails but others succeed
            string path = "test-stream-record-queue-partial-failure-config.yml";
            string[] lines = {
                "version: 2",
                "logger:",
                "  console:",
                "    level: verbose",
                "cognite:",
                $"  project: {_project}",
                $"  host: {_host}",
                "  cdf-chunking:",
                "    stream-records: 100",
                "  cdf-throttling:",
                "    stream-records: 2"
            };
            System.IO.File.WriteAllLines(path, lines);

            // Use a special mock that fails for specific streams
            var mocks = TestUtilities.GetMockedHttpClientFactory(MockStreamRecordsPartialFailureSendAsync);
            var mockFactory = mocks.factory;

            var services = new ServiceCollection();
            services.AddSingleton<IHttpClientFactory>(mockFactory.Object);
            services.AddConfig<BaseConfig>(path, 2);
            services.AddTestLogging(_output);
            services.AddCogniteClient("testApp", setLogger: true, setMetrics: false);

            using (var provider = services.BuildServiceProvider())
            {
                var cogniteDestination = provider.GetRequiredService<CogniteDestination>();

                await using (var queue = cogniteDestination.CreateStreamRecordUploadQueue(
                    TimeSpan.Zero, 0, null))
                {
                    // Enqueue to both a working and a failing stream
                    queue.Enqueue("working-stream", new StreamRecordWrite());
                    queue.Enqueue("failing-stream", new StreamRecordWrite());

                    var result = await queue.Trigger(CancellationToken.None);

                    // Should have partial success
                    Assert.False(result.IsFailed); // Not a complete failure
                    Assert.NotNull(result.Uploaded);
                    Assert.Single(result.Uploaded);
                    Assert.NotNull(result.Failed);
                    Assert.Single(result.Failed);
                }
            }

            System.IO.File.Delete(path);
            _ingestedRecords.Clear();
        }

        #region Mock

        private async Task<HttpResponseMessage> MockStreamRecordsSendAsync(
            HttpRequestMessage message,
            CancellationToken token)
        {
            var uri = message.RequestUri?.ToString() ?? "";
            var responseBody = "";

            if (_failInsert && uri.Contains("/streams/"))
            {
                var failResponse = new { error = new { code = 500, message = "Something went wrong" } };
                responseBody = System.Text.Json.JsonSerializer.Serialize(failResponse);
                var fail = new HttpResponseMessage
                {
                    StatusCode = HttpStatusCode.InternalServerError,
                    Content = new StringContent(responseBody)
                };
                fail.Content.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue("application/json");
                fail.Headers.Add("x-request-id", "1");
                return fail;
            }

            if (uri.Contains("/token/inspect"))
            {
                var inspectResponse = new
                {
                    projects = new[] { new { projectUrlName = _project } }
                };
                responseBody = System.Text.Json.JsonSerializer.Serialize(inspectResponse);
                var msg = new HttpResponseMessage
                {
                    StatusCode = HttpStatusCode.OK,
                    Content = new StringContent(responseBody)
                };
                msg.Content.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue("application/json");
                msg.Headers.Add("x-request-id", "1");
                return msg;
            }

            // Handle stream records ingest (POST to /streams/{streamId}/records)
            if (uri.Contains("/streams/") && uri.Contains("/records"))
            {
                // Extract stream ID from URL (e.g., /streams/{streamId}/records)
                var streamIdMatch = System.Text.RegularExpressions.Regex.Match(uri, @"/streams/([^/]+)/records");
                var streamId = streamIdMatch.Success ? streamIdMatch.Groups[1].Value : "unknown";

                var content = await message.Content!.ReadAsStringAsync();
                var records = System.Text.Json.JsonSerializer.Deserialize<StreamRecordWriteWrapper>(content,
                    new System.Text.Json.JsonSerializerOptions { PropertyNameCaseInsensitive = true });

                if (records?.Items != null)
                {
                    foreach (var record in records.Items)
                    {
                        _ingestedRecords.Add((streamId, record));
                    }
                }

                var successResponse = new { };
                responseBody = System.Text.Json.JsonSerializer.Serialize(successResponse);
                var response = new HttpResponseMessage
                {
                    StatusCode = HttpStatusCode.OK,
                    Content = new StringContent(responseBody)
                };
                response.Content.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue("application/json");
                response.Headers.Add("x-request-id", "1");
                return response;
            }

            // Default response for unknown endpoints
            var defaultResponse = new HttpResponseMessage
            {
                StatusCode = HttpStatusCode.NotFound,
                Content = new StringContent("{\"error\":{\"code\":404,\"message\":\"Not found\"}}")
            };
            defaultResponse.Content.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue("application/json");
            defaultResponse.Headers.Add("x-request-id", "1");
            return defaultResponse;
        }

        private async Task<HttpResponseMessage> MockStreamRecordsPartialFailureSendAsync(
            HttpRequestMessage message,
            CancellationToken token)
        {
            var uri = message.RequestUri?.ToString() ?? "";

            if (uri.Contains("/token/inspect"))
            {
                var inspectResponse = new
                {
                    projects = new[] { new { projectUrlName = _project } }
                };
                var responseBody = System.Text.Json.JsonSerializer.Serialize(inspectResponse);
                var msg = new HttpResponseMessage
                {
                    StatusCode = HttpStatusCode.OK,
                    Content = new StringContent(responseBody)
                };
                msg.Content.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue("application/json");
                msg.Headers.Add("x-request-id", "1");
                return msg;
            }

            if (uri.Contains("/streams/") && uri.Contains("/records"))
            {
                // Fail for "failing-stream"
                if (uri.Contains("failing-stream"))
                {
                    var failResponse = new { error = new { code = 400, message = "Stream not found" } };
                    var failResponseBody = System.Text.Json.JsonSerializer.Serialize(failResponse);
                    var fail = new HttpResponseMessage
                    {
                        StatusCode = HttpStatusCode.BadRequest,
                        Content = new StringContent(failResponseBody)
                    };
                    fail.Content.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue("application/json");
                    fail.Headers.Add("x-request-id", "1");
                    return fail;
                }

                // Success for other streams
                var streamIdMatch = System.Text.RegularExpressions.Regex.Match(uri, @"/streams/([^/]+)/records");
                var streamId = streamIdMatch.Success ? streamIdMatch.Groups[1].Value : "unknown";

                var content = await message.Content!.ReadAsStringAsync();
                var records = System.Text.Json.JsonSerializer.Deserialize<StreamRecordWriteWrapper>(content,
                    new System.Text.Json.JsonSerializerOptions { PropertyNameCaseInsensitive = true });

                if (records?.Items != null)
                {
                    foreach (var record in records.Items)
                    {
                        _ingestedRecords.Add((streamId, record));
                    }
                }

                var successResponse = new { };
                var responseBody = System.Text.Json.JsonSerializer.Serialize(successResponse);
                var response = new HttpResponseMessage
                {
                    StatusCode = HttpStatusCode.OK,
                    Content = new StringContent(responseBody)
                };
                response.Content.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue("application/json");
                response.Headers.Add("x-request-id", "1");
                return response;
            }

            var defaultResponse = new HttpResponseMessage
            {
                StatusCode = HttpStatusCode.NotFound,
                Content = new StringContent("{\"error\":{\"code\":404,\"message\":\"Not found\"}}")
            };
            defaultResponse.Content.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue("application/json");
            defaultResponse.Headers.Add("x-request-id", "1");
            return defaultResponse;
        }

        private class StreamRecordWriteWrapper
        {
            public List<StreamRecordWrite>? Items { get; set; }
        }

        #endregion
    }
}
