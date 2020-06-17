using Cognite.Extractor.Logging;
using Cognite.Extractor.Utils;
using CogniteSdk;
using Microsoft.Extensions.DependencyInjection;
using Newtonsoft.Json;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Dynamic;
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
                    CancellationToken.None
                );
                Assert.Equal(ids.Count(), ts.Where(t => ids.Contains(t.ExternalId)).Count());
                foreach (var t in ts)
                {
                    _ensuredEvents.Remove(t.ExternalId, out _);
                }

                var newEvents = createFunction(ids);
                using (var source = new CancellationTokenSource(5_000))
                {
                    // a timeout would fail the test
                    await cogniteDestination.EnsureEventsExistsAsync(newEvents, true, source.Token);
                }
                Assert.Equal(ids.Count(), _ensuredEvents
                    .Where(kvp => ids.Contains(kvp.Key)).Count());
            }

            System.IO.File.Delete(path);
        }

        #region mock
        private static ConcurrentDictionary<string, int> _ensuredEvents = new ConcurrentDictionary<string, int>();

        private static async Task<HttpResponseMessage> mockEnsureEventsSendAsync(
            HttpRequestMessage message,
            CancellationToken token)
        {
            var uri = message.RequestUri.ToString();
            var responseBody = "";
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
