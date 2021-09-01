using Cognite.Extensions;
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

namespace ExtractorUtils.Test.Unit
{
    public class SequenceTest
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
        public async Task TestEnsureSequences(params string[] ids)
        {
            string path = "test-ensure-sequences-config.yml";
            string[] lines = {  "version: 2",
                                "logger:",
                                "  console:",
                                "    level: verbose",
                                "cognite:",
                               $"  project: {_project}",
                               $"  api-key: {_apiKey}",
                               $"  host: {_host}",
                                "  cdf-chunking:",
                                "    sequences: 2",
                                "  cdf-throttling:",
                                "    sequences: 2" };
            System.IO.File.WriteAllLines(path, lines);

            var mocks = TestUtilities.GetMockedHttpClientFactory(mockEnsureSequencesSendAsync);
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

                Func<IEnumerable<string>, IEnumerable<SequenceCreate>> createFunction =
                    (ids) => {
                        var toCreate = new List<SequenceCreate>();
                        foreach (var id in ids)
                        {
                            toCreate.Add(new SequenceCreate
                            {
                                ExternalId = id,
                                Name = id,
                                Columns = new List<SequenceColumnWrite>
                                {
                                    new SequenceColumnWrite
                                    {
                                        ExternalId = "col"
                                    }
                                }
                            });
                        }
                        return toCreate;
                    };
                var ts = await cogniteDestination.GetOrCreateSequencesAsync(
                    ids,
                    createFunction,
                    RetryMode.OnErrorKeepDuplicates,
                    SanitationMode.Remove,
                    CancellationToken.None
                );
                Assert.Equal(ids.Count(), ts.Results.Where(t => ids.Contains(t.ExternalId)).Count());
                foreach (var t in ts.Results)
                {
                    _ensuredSequences.Remove(t.ExternalId, out _);
                }

                var newSequences = createFunction(ids);
                using (var source = new CancellationTokenSource(5_000))
                {
                    // a timeout would fail the test
                    await cogniteDestination.EnsureSequencesExistsAsync(newSequences, RetryMode.OnFatal, SanitationMode.Remove, source.Token);
                }
                Assert.Equal(ids.Count(), _ensuredSequences
                    .Where(kvp => ids.Contains(kvp.Key)).Count());
            }
        }

        #region mock
        private static ConcurrentDictionary<string, int> _ensuredSequences = new ConcurrentDictionary<string, int>();

        private static async Task<HttpResponseMessage> mockEnsureSequencesSendAsync(
            HttpRequestMessage message,
            CancellationToken token)
        {
            var uri = message.RequestUri.ToString();
            var responseBody = "";
            var statusCode = HttpStatusCode.OK;

            var content = await message.Content.ReadAsStringAsync();
            var data = JsonConvert.DeserializeObject<dynamic>(content);
            IEnumerable<dynamic> items = data.items;

            if (uri.Contains("/sequences/byids"))
            {
                Assert.True((bool)data.ignoreUnknownIds);

                dynamic result = new ExpandoObject();
                result.items = new List<ExpandoObject>();

                foreach (var item in items)
                {
                    string id = item.externalId;
                    var ensured = _ensuredSequences.TryGetValue(id, out int countdown) && countdown <= 0;
                    if (ensured || id.StartsWith("id"))
                    {
                        dynamic seqData = new ExpandoObject();
                        seqData.externalId = id;
                        result.items.Add(seqData);
                        _ensuredSequences.TryAdd(id, 0);
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
                    var hasValue = _ensuredSequences.TryGetValue(id, out int countdown);
                    if ((!hasValue || countdown > 0) && id.StartsWith("duplicated"))
                    {
                        var splitId = id.Split('-');
                        var count = splitId.Length == 2 ? int.Parse(splitId[1]) - 1 : 0;
                        dynamic duplicatedId = new ExpandoObject();
                        duplicatedId.externalId = id;
                        duplicateData.error.duplicated.Add(duplicatedId);
                        _ensuredSequences[id] = hasValue ? countdown - 1 : count;
                    }
                    else
                    {
                        dynamic seqData = new ExpandoObject();
                        seqData.externalId = id;
                        result.items.Add(seqData);
                        _ensuredSequences.TryAdd(id, 0);
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
