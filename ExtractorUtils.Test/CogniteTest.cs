using System.Dynamic;
using System.Linq;
using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using CogniteSdk;
using Microsoft.Extensions.DependencyInjection;
using Moq;
using Moq.Protected;
using Xunit;
using Newtonsoft.Json;
using System.Net.Http.Headers;

namespace ExtractorUtils.Test
{
    public class CogniteTest
    {
        private const string _authTenant = "someTenant";
        private const string _project = "someProject";
        private const string _apiKey = "someApiKey";
        private const string _host = "https://test.cognitedata.com";
        private static int _tokenCounter = 0;

        private static Task<HttpResponseMessage> mockAuthSendAsync(HttpRequestMessage message , CancellationToken token) {
            // Verify endpoint and method
            Assert.Equal($@"https://login.microsoftonline.com/{_authTenant}/oauth2/v2.0/token", message.RequestUri.ToString());
            Assert.Equal(HttpMethod.Post, message.Method);

            if (_tokenCounter == 2) //third call fails
            {
                var errorReply = "{" + Environment.NewLine + 
                                $"  \"error\": \"invalid_scope\",{Environment.NewLine}" +
                                 "}";
                var errorResponse = new HttpResponseMessage
                {
                    StatusCode = HttpStatusCode.BadRequest,
                    Content = new StringContent(errorReply)
                    
                };
                return Task.FromResult(errorResponse);
            }
            
            // build expected response
            var reply = "{" + Environment.NewLine + 
                       $"  \"token_type\": \"Bearer\",{Environment.NewLine}" +
                       $"  \"expires_in\": 1,{Environment.NewLine}" +
                       $"  \"access_token\": \"token{_tokenCounter}\"{Environment.NewLine}" +
                        "}";
            _tokenCounter++;
            // Return 200
            var response = new HttpResponseMessage
            {
                StatusCode = HttpStatusCode.OK,
                Content = new StringContent(reply)
                
            };
            
            return Task.FromResult(response);
        }

        [Fact]
        public async Task TestAuthenticator()
        {
            var clientId = "someId";
            string path = "test-authenticator-config.yml";
            string[] lines = {  "version: 2",
                                "logger:",
                                "  console:",
                                "    level: verbose",
                                "cognite:",
                                "  idp-authentication:",
                               $"    client-id: {clientId}",
                               $"    tenant: {_authTenant}",
                                "    secret: thisIsASecret",
                                "    scope: thisIsAScope",
                                "    min-ttl: 0" };
            System.IO.File.WriteAllLines(path, lines);

            var mocks = TestUtilities.GetMockedHttpClientFactory(mockAuthSendAsync);
            var mockHttpMessageHandler = mocks.handler;
            var mockFactory = mocks.factory;

            // Setup services
            var services = new ServiceCollection();
            services.AddSingleton<IHttpClientFactory>(mockFactory.Object); // inject the mock factory
            services.AddConfig<BaseConfig>(path, 2);
            services.AddLogger();
            services.AddHttpClient<Authenticator>();
            using (var provider = services.BuildServiceProvider()) {
                var auth = provider.GetRequiredService<Authenticator>();
                var token = await auth.GetToken();
                Assert.Equal("token0", token);
                await Task.Delay(100);
                token = await auth.GetToken(); // same token
                Assert.Equal("token0", token);
                await Task.Delay(1000); // token expired
                token = await auth.GetToken(); // new token
                Assert.Equal("token1", token);
                await Task.Delay(1100); // token expired
                token = await auth.GetToken(); // failed, returns null
                Assert.Null(token);
            }

            // Verify that the authentication endpoint was called 3 times
            mockHttpMessageHandler.Protected()
                .Verify<Task<HttpResponseMessage>>(
                    "SendAsync", 
                    Times.Exactly(3),
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>());

            System.IO.File.Delete(path);
        }

        private static Task<HttpResponseMessage> mockCogniteSendAsync(HttpRequestMessage message , CancellationToken token) {
            
            var reply = "";
            Assert.True(message.RequestUri.ToString() == $@"{_host}/api/v1/projects/{_project}/timeseries/list" ||
                        message.RequestUri.ToString() == $@"{_host}/login/status");
            if (message.RequestUri.ToString() == $@"{_host}/login/status")
            {
                Assert.Equal(HttpMethod.Get, message.Method);
                message.Headers.TryGetValues("api-key", out IEnumerable<string> keys);
                var loggedIn = keys.Contains(_apiKey) ? "true" : "false";
                reply = "{" + Environment.NewLine + 
                        "  \"data\": {" +  Environment.NewLine +
                       $"    \"user\": \"testuser\",{Environment.NewLine}" +
                       $"    \"loggedIn\": {loggedIn},{Environment.NewLine}" +
                       $"    \"project\": \"{_project}\"" + Environment.NewLine +
                        "  }" + Environment.NewLine +
                        "}";
            }
            else
            {
                Assert.Equal(HttpMethod.Post, message.Method);
                message.Headers.TryGetValues("api-key", out IEnumerable<string> keys);
                Assert.Contains(_apiKey, keys);
                message.Headers.TryGetValues("x-cdp-app", out IEnumerable<string> apps);
                Assert.Contains("testApp", apps);
                reply = "{" + Environment.NewLine + 
                        "  \"items\": [ ]" +  Environment.NewLine +
                        "}";
            }
            

            // Return 200
            var response = new HttpResponseMessage
            {
                StatusCode = HttpStatusCode.OK,
                Content = new StringContent(reply)
                
            };
            
            return Task.FromResult(response);
        }

        [Fact]
        public async Task TestCogniteClient()
        {
            string path = "test-cognite-client-config.yml";
            string[] lines = {  "version: 2",
                                "logger:",
                                "  console:",
                                "    level: verbose",
                                "cognite:",
                               $"  project: {_project}",
                               $"  api-key: {_apiKey}",
                               $"  host: {_host}" };
            System.IO.File.WriteAllLines(path, lines);

            var mocks = TestUtilities.GetMockedHttpClientFactory(mockCogniteSendAsync);
            var mockHttpMessageHandler = mocks.handler;
            var mockFactory = mocks.factory;

            // Setup services
            var services = new ServiceCollection();
            services.AddSingleton<IHttpClientFactory>(mockFactory.Object); // inject the mock factory
            services.AddConfig<BaseConfig>(path, 2);
            services.AddLogger();
            services.AddCogniteClient("testApp", true, true);
            using (var provider = services.BuildServiceProvider()) {
                var config = provider.GetRequiredService<CogniteConfig>();
                var cogClient = provider.GetRequiredService<Client>();
                var ex = await Assert.ThrowsAsync<CogniteUtilsException>(() => cogClient.TestCogniteConfig(null, CancellationToken.None));
                Assert.Contains("configuration missing", ex.Message);

                config.Project = null;
                ex = await Assert.ThrowsAsync<CogniteUtilsException>(() => cogClient.TestCogniteConfig(config, CancellationToken.None));
                Assert.Contains("project is not configured", ex.Message);

                config.Project = "Bogus";
                ex = await Assert.ThrowsAsync<CogniteUtilsException>(() => cogClient.TestCogniteConfig(config, CancellationToken.None));
                Assert.Contains("not associated with project Bogus", ex.Message);
                config.Project = _project;

                await cogClient.TestCogniteConfig(config, CancellationToken.None);

                var loginStatus = await cogClient.Login.StatusAsync(CancellationToken.None);
                Assert.True(loginStatus.LoggedIn);
                Assert.Equal("testuser", loginStatus.User);
                Assert.Equal(_project, loginStatus.Project);

                var options = new TimeSeriesQuery()
                {
                    Limit = 1
                };
                var ts = await cogClient.TimeSeries.ListAsync(options);
                Assert.Empty(ts.Items);
            }

            // Verify that the authentication endpoint was called 2 times
            mockHttpMessageHandler.Protected()
                .Verify<Task<HttpResponseMessage>>(
                    "SendAsync", 
                    Times.Exactly(4), // 2 time trying to test the client and 2 times when using the client
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>());

            System.IO.File.Delete(path);
        }

        [Fact]
        public async Task TestInvalidApiKeyClient()
        {
            string path = "test-ensure-time-series-config.yml";
            string[] lines = {  "version: 2",
                                "cognite:",
                               $"  project: {_project}",
                               $"  api-key: otherApiKey",
                               $"  host: {_host}" };
            System.IO.File.WriteAllLines(path, lines);

            var mocks = TestUtilities.GetMockedHttpClientFactory(mockCogniteSendAsync);
            var mockHttpMessageHandler = mocks.handler;
            var mockFactory = mocks.factory;

            // Setup services
            var services = new ServiceCollection();
            services.AddSingleton<IHttpClientFactory>(mockFactory.Object); // inject the mock factory
            services.AddConfig<BaseConfig>(path, 2);
            services.AddCogniteClient("testApp");
            using (var provider = services.BuildServiceProvider()) {
                var config = provider.GetRequiredService<CogniteConfig>();
                var cogClient = provider.GetRequiredService<Client>();
                var ex = await Assert.ThrowsAsync<CogniteUtilsException>(() => cogClient.TestCogniteConfig(config, CancellationToken.None));
                Assert.Contains("credentials are invalid", ex.Message);
            }

            // Verify that the authentication endpoint was called 2 times
            mockHttpMessageHandler.Protected()
                .Verify<Task<HttpResponseMessage>>(
                    "SendAsync", 
                    Times.Exactly(1),
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>());

            System.IO.File.Delete(path);
        }

        [Theory]
        [InlineData("id1", "id2")]
        [InlineData("id1", "id2", "id3", "id4", "id5")]
        [InlineData("missing1", "missing2")]
        [InlineData("id1", "id2", "missing1", "id4", "missing2")]
        [InlineData("duplicated1", "duplicated2")]
        [InlineData("id1", "id2", "duplicated1", "id4", "duplicated2")]
        [InlineData("id1", "missing1", "id2", "duplicated1", "missing2", "duplicated2")]
        [InlineData("id1", "id2", "missing1", "duplicated1-2", "duplicated2-4", "duplicated3-3")]
        /// <summary>
        /// External ids starting with 'id' exist in the mocked endpoint.
        /// External ids starting with 'missing' do not exist, but can be successfully created.
        /// External ids starting with 'duplicated' do not exist, and fail during creation as duplicated.
        /// Duplicated with a suffix '-N', where N is an int will be reported by the endpoint as duplicated
        /// a total of N times.
        /// </summary>
        /// <param name="ids"></param>
        /// <returns></returns>
        public async Task TestEnsureTimeSeries(params string[] ids)
        {
            string path = "test-cognite-invalid-client-config.yml";
            string[] lines = {  "version: 2",
                                "cognite:",
                               $"  project: {_project}",
                               $"  api-key: {_apiKey}",
                               $"  host: {_host}" };
            System.IO.File.WriteAllLines(path, lines);

            var mocks = TestUtilities.GetMockedHttpClientFactory(mockEnsureTimeSeriesSendAsync);
            var mockHttpMessageHandler = mocks.handler;
            var mockFactory = mocks.factory;

            // Setup services
            var services = new ServiceCollection();
            services.AddSingleton<IHttpClientFactory>(mockFactory.Object); // inject the mock factory
            services.AddConfig<BaseConfig>(path, 2);
            services.AddCogniteClient("testApp");
            using (var provider = services.BuildServiceProvider()) {
                var config = provider.GetRequiredService<CogniteConfig>();
                var cogClient = provider.GetRequiredService<Client>();
                
                Func<IEnumerable<string>, IEnumerable<TimeSeriesCreate>> createFunction = 
                    (ids) => {
                        var toCreate = new List<TimeSeriesCreate>();
                        foreach (var id in ids)
                        {
                            toCreate.Add(new TimeSeriesCreate
                            {
                                ExternalId = id

                            });
                        }
                        return toCreate;
                    };
                _ensuredTimeSeries.Clear();
                var ts = await cogClient.EnsureTimeSeries(
                    ids,
                    createFunction, 
                    2, 
                    2,
                    CancellationToken.None
                );
                Assert.Equal(ids.Count(), ts.Where(t => ids.Contains(t.ExternalId)).Count());
            }

            System.IO.File.Delete(path);
        }

        private static Dictionary<string, int> _ensuredTimeSeries = new Dictionary<string, int>();

        private static async Task<HttpResponseMessage> mockEnsureTimeSeriesSendAsync(HttpRequestMessage message , CancellationToken token) {
            var uri = message.RequestUri.ToString();
            var responseBody = "";
            var statusCode = HttpStatusCode.OK;

            var content = await message.Content.ReadAsStringAsync();
            var ids = JsonConvert.DeserializeObject<dynamic>(content);
            IEnumerable<dynamic> items = ids.items;


            if (uri.Contains("/timeseries/byids"))
            {
                dynamic missingData = new ExpandoObject();
                missingData.error = new ExpandoObject();
                missingData.error.code = 400;
                missingData.error.message = "Ids not found";
                missingData.error.missing = new List<ExpandoObject>();

                dynamic result = new ExpandoObject();
                result.items = new List<ExpandoObject>();

                foreach (var item in items)
                {
                    string id = item.externalId;
                    var ensured = _ensuredTimeSeries.TryGetValue(id, out int countdown) && countdown <= 0;
                    if (!ensured && !id.StartsWith("id")) {
                        dynamic missingId = new ExpandoObject();
                        missingId.externalId = id;
                        missingData.error.missing.Add(missingId);
                    }
                    else
                    {
                        dynamic tsData = new ExpandoObject();
                        tsData.externalId = id;
                        result.items.Add(tsData);
                        _ensuredTimeSeries.TryAdd(id, 0);
                    }

                }
                if (missingData.error.missing.Count > 0)
                {
                    responseBody = JsonConvert.SerializeObject(missingData);
                    statusCode = HttpStatusCode.BadRequest;
                }
                else
                {
                    responseBody = JsonConvert.SerializeObject(result);
                }                
            }
            else {
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
                    var hasValue = _ensuredTimeSeries.TryGetValue(id, out int countdown);
                    if ((!hasValue || countdown > 0) && id.StartsWith("duplicated")) {
                        var splittedId = id.Split('-');
                        var count = splittedId.Count() == 2 ? int.Parse(splittedId[1]) - 1 : 0;
                        dynamic duplicatedId = new ExpandoObject();
                        duplicatedId.externalId = id;
                        duplicateData.error.duplicated.Add(duplicatedId);
                        _ensuredTimeSeries[id] = hasValue ? countdown - 1 : count;
                    }
                    else
                    {
                        dynamic tsData = new ExpandoObject();
                        tsData.externalId = id;
                        result.items.Add(tsData);
                        _ensuredTimeSeries.TryAdd(id, 0);
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

    }
}