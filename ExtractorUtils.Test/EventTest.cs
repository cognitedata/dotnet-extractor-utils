using Cognite.Extensions;
using Cognite.Extractor.Common;
using Cognite.Extractor.Logging;
using Cognite.Extractor.Utils;
using CogniteSdk;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Dynamic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace ExtractorUtils.Test
{
    public class EventTest
    {
        private const string _project = "someProject";
        private const string _apiKey = "someApiKey";
        private const string _host = "https://test.cognitedata.com";

        private bool _failInsert;

        [Theory]
        [InlineData("id1", "id2")]
        [InlineData("id3", "id4", "id5", "id6", "id7")]
        [InlineData("missing1", "missing2")]
        [InlineData("id8", "id9", "missing3", "id10", "missing4")]
        [InlineData("duplicated1", "duplicated2")]
        [InlineData("id11", "id12", "duplicated3", "id13", "duplicated4")]
        [InlineData("id14", "missing5", "id15", "duplicated5", "missing6", "duplicated6")]
        [InlineData("id16", "id17", "missing7", "duplicated7-2", "duplicated8-4", "duplicated9-3")]
        /// <summary>
        /// External ids starting with 'id' exist in the mocked endpoint.
        /// External ids starting with 'missing' do not exist, but can be successfully created.
        /// External ids starting with 'duplicated' do not exist, and fail during creation as duplicated.
        /// Duplicated with a suffix '-N', where N is an int will be reported by the endpoint as duplicated
        /// a total of N times.
        /// </summary>
        /// <param name="ids"></param>
        /// <returns></returns>
        public async Task TestEnsureEvents(params string[] ids)
        {
            string path = "test-ensure-events-config.yml";
            string[] lines = {  "version: 2",
                                "logger:",
                                "  console:",
                                "    level: verbose",
                                "cognite:",
                               $"  project: {_project}",
                               $"  api-key: {_apiKey}",
                               $"  host: {_host}",
                                "  cdf-chunking:",
                                "    events: 2",
                                "  cdf-throttling:",
                                "    events: 2" };
            System.IO.File.WriteAllLines(path, lines);

            var mocks = TestUtilities.GetMockedHttpClientFactory(mockEnsureEventsSendAsync);
            var mockHttpMessageHandler = mocks.handler;
            var mockFactory = mocks.factory;

            // Setup services
            var services = new ServiceCollection();
            services.AddSingleton<IHttpClientFactory>(mockFactory.Object); // inject the mock factory
            services.AddConfig<BaseConfig>(path, 2);
            services.AddLogger();
            services.AddCogniteClient("testApp");
            using (var provider = services.BuildServiceProvider())
            {
                var cogniteDestination = provider.GetRequiredService<CogniteDestination>();

                Func<IEnumerable<string>, IEnumerable<EventCreate>> createFunction =
                    (ids) => {
                        var toCreate = new List<EventCreate>();
                        foreach (var id in ids)
                        {
                            toCreate.Add(new EventCreate
                            {
                                ExternalId = id
                            });
                        }
                        return toCreate;
                    };
                var ts = await cogniteDestination.GetOrCreateEventsAsync(
                    ids,
                    createFunction,
                    RetryMode.OnErrorKeepDuplicates,
                    true,
                    CancellationToken.None
                );
                Assert.Equal(ids.Count(), ts.Results.Where(t => ids.Contains(t.ExternalId)).Count());
                foreach (var t in ts.Results)
                {
                    _ensuredEvents.Remove(t.ExternalId, out _);
                }

                var newEvents = createFunction(ids);
                using (var source = new CancellationTokenSource(5_000))
                {
                    // a timeout would fail the test
                    await cogniteDestination.EnsureEventsExistsAsync(newEvents, RetryMode.OnFatal, true, source.Token);
                }
                Assert.Equal(ids.Count(), _ensuredEvents
                    .Where(kvp => ids.Contains(kvp.Key)).Count());
            }

            System.IO.File.Delete(path);
        }

        [Fact]
        public async Task TestUploadQueue()
        {
            string path = "test-event-queue-config.yml";
            string[] lines = {  "version: 2",
                                "logger:",
                                "  console:",
                                "    level: verbose",
                                "cognite:",
                               $"  project: {_project}",
                               $"  api-key: {_apiKey}",
                               $"  host: {_host}",
                                "  cdf-chunking:",
                                "    events: 2",
                                "  cdf-throttling:",
                                "    events: 2" };
            System.IO.File.WriteAllLines(path, lines);

            var mocks = TestUtilities.GetMockedHttpClientFactory(mockEnsureEventsSendAsync);
            var mockHttpMessageHandler = mocks.handler;
            var mockFactory = mocks.factory;

            var services = new ServiceCollection();
            services.AddSingleton<IHttpClientFactory>(mockFactory.Object); // inject the mock factory
            services.AddConfig<BaseConfig>(path, 2);
            services.AddLogger();
            services.AddCogniteClient("testApp", true, false);
            var index = 0;

            int evtCount = 0;
            int cbCount = 0;

            using (var source = new CancellationTokenSource())
            using (var provider = services.BuildServiceProvider())
            {
                var cogniteDestination = provider.GetRequiredService<CogniteDestination>();
                var logger = provider.GetRequiredService<ILogger<EventTest>>();
                // queue with 1 sec upload interval
                using (var queue = cogniteDestination.CreateEventUploadQueue(TimeSpan.FromSeconds(1), 0, res =>
                {
                    evtCount += res.Uploaded?.Count() ?? 0;
                    cbCount++;
                    return Task.CompletedTask;
                }))
                {
                    var enqueueTask = Task.Run(async () => {
                        while (index < 13)
                        {
                            queue.Enqueue(new EventCreate
                            {
                                ExternalId = "id " + index,
                                StartTime = DateTime.UtcNow.ToUnixTimeMilliseconds(),
                                EndTime = DateTime.UtcNow.ToUnixTimeMilliseconds()
                            });
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

                Assert.Equal(13, evtCount);
                Assert.True(cbCount <= 3);
                cbCount = 0;

                // queue with maximum size
                using (var queue = cogniteDestination.CreateEventUploadQueue(TimeSpan.FromMinutes(10), 5, res =>
                {
                    evtCount += res.Uploaded?.Count() ?? 0;
                    cbCount++;
                    return Task.CompletedTask;
                }))
                {
                    var enqueueTask = Task.Run(async () => {
                        while (index < 23)
                        {
                            queue.Enqueue(new EventCreate
                            {
                                ExternalId = "id " + index,
                                StartTime = DateTime.UtcNow.ToUnixTimeMilliseconds(),
                                EndTime = DateTime.UtcNow.ToUnixTimeMilliseconds()
                            });
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

                Assert.Equal(23, evtCount);
                Assert.Equal(3, cbCount);
            }

            System.IO.File.Delete(path);
        }

        [Fact]
        public async Task TestUploadQueueBuffer()
        {
            string path = "test-event-queue-buffer-config.yml";
            string[] lines = {  "version: 2",
                                "logger:",
                                "  console:",
                                "    level: verbose",
                                "cognite:",
                               $"  project: {_project}",
                               $"  api-key: {_apiKey}",
                               $"  host: {_host}",
                                "  cdf-chunking:",
                                "    events: 2",
                                "  cdf-throttling:",
                                "    events: 2" };
            System.IO.File.WriteAllLines(path, lines);

            var mocks = TestUtilities.GetMockedHttpClientFactory(mockEnsureEventsSendAsync);
            var mockHttpMessageHandler = mocks.handler;
            var mockFactory = mocks.factory;

            var services = new ServiceCollection();
            services.AddSingleton<IHttpClientFactory>(mockFactory.Object); // inject the mock factory
            services.AddConfig<BaseConfig>(path, 2);
            services.AddLogger();
            services.AddCogniteClient("testApp", true, false);

            System.IO.File.Create("event-buffer.bin").Close();

            using (var source = new CancellationTokenSource())
            using (var provider = services.BuildServiceProvider())
            {
                var cogniteDestination = provider.GetRequiredService<CogniteDestination>();
                var logger = provider.GetRequiredService<ILogger<EventTest>>();
                using (var queue = cogniteDestination.CreateEventUploadQueue(TimeSpan.Zero, 0, null, "event-buffer.bin"))
                {
                    var _ = queue.Start(source.Token);
                    for (int i = 0; i < 10; i++)
                    {
                        queue.Enqueue(new EventCreate
                        {
                            ExternalId = "id " + i,
                            StartTime = DateTime.UtcNow.ToUnixTimeMilliseconds(),
                            EndTime = DateTime.UtcNow.ToUnixTimeMilliseconds()
                        });
                    }
                    _failInsert = true;
                    Assert.Equal(0, new FileInfo("event-buffer.bin").Length);
                    await queue.Trigger(CancellationToken.None);
                    Assert.True(new FileInfo("event-buffer.bin").Length > 0);
                    Assert.Empty(_ensuredEvents);
                    await queue.Trigger(CancellationToken.None);
                    Assert.True(new FileInfo("event-buffer.bin").Length > 0);
                    Assert.Empty(_ensuredEvents);
                    _failInsert = false;
                    await queue.Trigger(CancellationToken.None);
                    Assert.Equal(0, new FileInfo("event-buffer.bin").Length);
                    Assert.Equal(10, _ensuredEvents.Count);
                    logger.LogInformation("Disposing of the upload queue");
                }
                logger.LogInformation("Upload queue disposed");
            }
            System.IO.File.Delete("event-buffer.bin");
            System.IO.File.Delete(path);
        }


        #region mock
        private ConcurrentDictionary<string, int> _ensuredEvents = new ConcurrentDictionary<string, int>();

        private async Task<HttpResponseMessage> mockEnsureEventsSendAsync(
            HttpRequestMessage message,
            CancellationToken token)
        {
            var uri = message.RequestUri.ToString();
            var responseBody = "";

            if (_failInsert)
            {
                dynamic failResponse = new ExpandoObject();
                failResponse.error = new ExpandoObject();
                failResponse.error.code = 500;
                failResponse.error.message = "Something went wrong";

                responseBody = JsonConvert.SerializeObject(failResponse);
                var fail = new HttpResponseMessage
                {
                    StatusCode = HttpStatusCode.InternalServerError,
                    Content = new StringContent(responseBody)
                };
                fail.Content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
                fail.Headers.Add("x-request-id", "1");
                return fail;
            }

            if (uri.Contains("/login/status"))
            {
                dynamic loginResponse = new ExpandoObject();
                loginResponse.data = new ExpandoObject();
                loginResponse.data.user = "user";
                loginResponse.data.project = _project;
                loginResponse.data.loggedIn = true;
                loginResponse.data.projectId = 1;

                responseBody = JsonConvert.SerializeObject(loginResponse);
                var login = new HttpResponseMessage
                {
                    StatusCode = HttpStatusCode.OK,
                    Content = new StringContent(responseBody)
                };
                login.Content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
                login.Headers.Add("x-request-id", "1");
                return login;
            }

            var statusCode = HttpStatusCode.OK;

            var content = await message.Content.ReadAsStringAsync();
            var ids = JsonConvert.DeserializeObject<dynamic>(content);
            IEnumerable<dynamic> items = ids.items;



            if (uri.Contains("/events/byids"))
            {
                Assert.True((bool)ids.ignoreUnknownIds);

                dynamic result = new ExpandoObject();
                result.items = new List<ExpandoObject>();

                foreach (var item in items)
                {
                    string id = item.externalId;
                    var ensured = _ensuredEvents.TryGetValue(id, out int countdown) && countdown <= 0;
                    if (ensured || id.StartsWith("id"))
                    {
                        dynamic eventData = new ExpandoObject();
                        eventData.externalId = id;
                        result.items.Add(eventData);
                        _ensuredEvents.TryAdd(id, 0);
                    }
                }
                responseBody = JsonConvert.SerializeObject(result);
            }
            else
            {
                dynamic duplicateData = new ExpandoObject();
                duplicateData.error = new ExpandoObject();
                duplicateData.error.code = 409;
                duplicateData.error.message = "ExternalIds duplicated";
                duplicateData.error.duplicated = new List<ExpandoObject>();

                dynamic result = new ExpandoObject();
                result.items = new List<ExpandoObject>();

                foreach (var item in items)
                {
                    string id = item.externalId;
                    var hasValue = _ensuredEvents.TryGetValue(id, out int countdown);
                    if ((!hasValue || countdown > 0) && id.StartsWith("duplicated"))
                    {
                        var splittedId = id.Split('-');
                        var count = splittedId.Count() == 2 ? int.Parse(splittedId[1]) - 1 : 0;
                        dynamic duplicatedId = new ExpandoObject();
                        duplicatedId.externalId = id;
                        duplicateData.error.duplicated.Add(duplicatedId);
                        _ensuredEvents[id] = hasValue ? countdown - 1 : count;
                    }
                    else
                    {
                        dynamic eventData = new ExpandoObject();
                        eventData.externalId = id;
                        result.items.Add(eventData);
                        _ensuredEvents.TryAdd(id, 0);
                    }

                }
                if (duplicateData.error.duplicated.Count > 0)
                {
                    responseBody = JsonConvert.SerializeObject(duplicateData);
                    statusCode = HttpStatusCode.Conflict;
                }
                else
                {
                    responseBody = JsonConvert.SerializeObject(result);
                }
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
        #endregion
    }
}
