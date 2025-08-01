using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;
using CogniteSdk;
using Microsoft.Extensions.DependencyInjection;
using Moq;
using Moq.Protected;
using Newtonsoft.Json;
using Xunit;
using Cognite.Extractor.Utils;
using Microsoft.Extensions.Logging;
using Polly;
using System.Collections.Concurrent;
using Cognite.Extensions;
using Xunit.Abstractions;
using Cognite.Extractor.Testing;

namespace ExtractorUtils.Test.Unit
{
    public class CogniteTest
    {
        private const string _authTenant = "someTenant";
        private const string _project = "someProject";
        private const string _host = "https://test.cognitedata.com";
#pragma warning disable CA1805 // Do not initialize unnecessarily
        private static int _tokenCounter = 0;
#pragma warning restore CA1805 // Do not initialize unnecessarily

        private readonly ITestOutputHelper _output;
        public CogniteTest(ITestOutputHelper output)
        {
            _output = output;
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
                                "    implementation: Basic",
                               $"    client-id: {clientId}",
                                "    token-url: http://example.url/token",
                                "    secret: thisIsASecret",
                                "    scopes: ",
                                "      - thisIsAScope",
                                "    min-ttl: 0" };
            System.IO.File.WriteAllLines(path, lines);

            var mocks = TestUtilities.GetMockedHttpClientFactory(mockAuthSendAsync);
            var mockHttpMessageHandler = mocks.handler;
            var mockFactory = mocks.factory;

            // Setup services
            var services = new ServiceCollection();
            services.AddSingleton<IHttpClientFactory>(mockFactory.Object); // inject the mock factory
            var config = services.AddConfig<BaseConfig>(path, 2);
            services.AddSingleton<AuthenticatorConfig>(config.Cognite.IdpAuthentication);
            services.AddTestLogging(_output);
            services.AddTransient<IAuthenticator>(provider =>
            {
                var conf = provider.GetRequiredService<CogniteConfig>();
                var logger = provider.GetRequiredService<ILogger<IAuthenticator>>();
                var clientFactory = provider.GetRequiredService<IHttpClientFactory>();

                return new Authenticator(conf.IdpAuthentication, clientFactory.CreateClient("AuthenticatorClient"), logger);
            });
            using (var provider = services.BuildServiceProvider())
            {
                var auth = provider.GetRequiredService<IAuthenticator>();
                var token = await auth.GetToken();
                Assert.Equal("token0", token);
                token = await auth.GetToken(); // same token
                Assert.Equal("token0", token);
                await Task.Delay(2000); // token expired
                token = await auth.GetToken(); // new token
                Assert.Equal("token1", token);
                await Task.Delay(2100); // token expired
                await Assert.ThrowsAsync<CogniteUtilsException>(() => auth.GetToken()); // failed, returns null
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

        [Fact]
        public async Task TestClientRetry()
        {
            string path = "test-cognite-retry-config.yml";
            string[] lines = {  "version: 2",
                                "logger:",
                                "  console:",
                                "    level: verbose",
                                "cognite:",
                               $"  project: {_project}",
                               $"  host: {_host}",
                                "  cdf-retries:",
                                "    max-retries: 3",
                                "    timeout: 10000" };
            System.IO.File.WriteAllLines(path, lines);

            var mocks = TestUtilities.GetMockedHttpClientFactory(mockCogniteAssetsRetryAsync);
            var mockHttpMessageHandler = mocks.handler;
            var mockFactory = mocks.factory;
            _sendRetries = 0;
            // Setup services
            var services = new ServiceCollection();
            services.AddConfig<BaseConfig>(path, 2);
            services.AddTestLogging(_output);
            IAsyncPolicy<HttpResponseMessage> retryPolicy;
            IAsyncPolicy<HttpResponseMessage> timeoutPolicy;
            using (var provider = services.BuildServiceProvider())
            {
                var logger = provider.GetRequiredService<ILogger<CogniteDestination>>();
                var config = provider.GetRequiredService<BaseConfig>().Cognite.CdfRetries;
                retryPolicy = CogniteExtensions.GetRetryPolicy(logger, config.MaxRetries, config.MaxDelay);
                timeoutPolicy = CogniteExtensions.GetTimeoutPolicy(config.Timeout);
            }

            services.AddHttpClient<Client.Builder>()
                .ConfigurePrimaryHttpMessageHandler(() => new HttpMessageHandlerStub(mockCogniteAssetsRetryAsync))
                .AddPolicyHandler(retryPolicy)
                .AddPolicyHandler(timeoutPolicy);

            services.AddCogniteClient("testApp", setLogger: true, setMetrics: true, setHttpClient: false);
            using (var provider = services.BuildServiceProvider())
            {
                var cogniteDestination = provider.GetRequiredService<CogniteDestination>();

                await cogniteDestination.CogniteClient.Assets.ListAsync(new AssetQuery());
            }

        }

        [Fact]
        public async Task TestClientAuthRetry()
        {
            string path = "test-cognite-retry-auth-config.yml";
            string[] lines = {  "version: 2",
                                "logger:",
                                "  console:",
                                "    level: verbose",
                                "cognite:",
                               $"  project: {_project}",
                               $"  host: {_host}",
                                "  cdf-retries:",
                                "    max-retries: 3",
                                "    timeout: 10000",
                                "  idp-authentication:",
                                "    implementation: Basic",
                                "    client-id: someId",
                                "    token-url: http://example.url/token",
                                "    secret: thisIsASecret",
                                "    scopes: ",
                                "      - thisIsAScope"
                             };
            System.IO.File.WriteAllLines(path, lines);

            var mocks = TestUtilities.GetMockedHttpClientFactory(mockAuthRetryAsync);
            var mockHttpMessageHandler = mocks.handler;
            var mockFactory = mocks.factory;
            _sendRetries = 0;
            _tokenCounter = 0;
            // Setup services
            var services = new ServiceCollection();
            var config = services.AddConfig<BaseConfig>(path, 2);
            services.AddSingleton(config.Cognite.IdpAuthentication);
            services.AddTestLogging(_output);
            services.AddHttpClient<Client.Builder>()
                .ConfigurePrimaryHttpMessageHandler(() => new HttpMessageHandlerStub(mockAuthRetryAsync))
                .ConfigureCogniteHttpClientHandlers()
                .AddHttpMessageHandler(provider => new AuthenticatorDelegatingHandler(provider.GetRequiredService<IAuthenticator>()));
            services.AddHttpClient("AuthenticatorClient")
                .ConfigurePrimaryHttpMessageHandler(() => new HttpMessageHandlerStub(mockAuthRetryAsync))
                .ConfigureCogniteHttpClientHandlers();

            services.AddCogniteClient("testApp", setLogger: true, setMetrics: true, setHttpClient: false);
            using var provider = services.BuildServiceProvider();
            var cogniteDestination = provider.GetRequiredService<CogniteDestination>();
            await cogniteDestination.CogniteClient.Assets.ListAsync(new AssetQuery());

            // First we hit the auth endpoint 3 times, to get an initial valid token.
            // Next, we hit the assets endpoint once, it fails, and when retrying we need to fetch a new token.
            // This means we hit the auth endpoint 6 times in total.
            Assert.Equal(5, _tokenCounter);
            Assert.Equal(3, _sendRetries);

            _tokenCounter = 0;
            _sendRetries = 0;
            config.Cognite.CdfRetries.MaxRetries = 1; // Set max retries to 1.
            // Try again, this time it should fail.
            await Assert.ThrowsAsync<CogniteUtilsException>(() => cogniteDestination.CogniteClient.Assets.ListAsync(new AssetQuery()));
            // We should have hit the auth endpoint 2 times, and the assets endpoint 0 times.
            Assert.Equal(2, _tokenCounter);
            Assert.Equal(0, _sendRetries);
        }

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
        public async Task TestEnsureTimeSeries(params string[] ids)
        {
            string path = "test-ensure-time-series-config.yml";
            string[] lines = {  "version: 2",
                                "logger:",
                                "  console:",
                                "    level: verbose",
                                "cognite:",
                               $"  project: {_project}",
                               $"  host: {_host}",
                                "  cdf-chunking:",
                                "    time-series: 2",
                                "  cdf-throttling:",
                                "    time-series: 2" };
            System.IO.File.WriteAllLines(path, lines);

            var mocks = TestUtilities.GetMockedHttpClientFactory(MockEnsureTimeSeriesSendAsync);
            var mockHttpMessageHandler = mocks.handler;
            var mockFactory = mocks.factory;

            // Setup services
            var services = new ServiceCollection();
            services.AddSingleton<IHttpClientFactory>(mockFactory.Object); // inject the mock factory
            services.AddConfig<BaseConfig>(path, 2);
            services.AddTestLogging(_output);
            services.AddCogniteClient("testApp");
            using (var provider = services.BuildServiceProvider())
            {
                var cogniteDestination = provider.GetRequiredService<CogniteDestination>();

                Func<IEnumerable<string>, IEnumerable<TimeSeriesCreate>> createFunction =
                    (idxs) =>
                    {
                        var toCreate = new List<TimeSeriesCreate>();
                        foreach (var id in idxs)
                        {
                            toCreate.Add(new TimeSeriesCreate
                            {
                                ExternalId = id

                            });
                        }
                        return toCreate;
                    };
                var ts = await cogniteDestination.GetOrCreateTimeSeriesAsync(
                    ids,
                    createFunction,
                    RetryMode.OnErrorKeepDuplicates,
                    SanitationMode.Remove,
                    CancellationToken.None
                );
                Assert.Equal(ids?.Length, ts.Results.Where(t => ids.Contains(t.ExternalId)).Count());
                foreach (var t in ts.Results)
                {
                    _ensuredTimeSeries.TryRemove(t.ExternalId, out _);
                }

                var newTs = createFunction(ids);
                using (var source = new CancellationTokenSource(5_000))
                {
                    // a timeout would fail the test
                    await cogniteDestination.EnsureTimeSeriesExistsAsync(newTs, RetryMode.OnFatal, SanitationMode.Remove, source.Token);
                }
                Assert.Equal(ids.Length, _ensuredTimeSeries
                    .Where(kvp => ids.Contains(kvp.Key)).Count());
            }

            System.IO.File.Delete(path);
        }

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
        public async Task TestEnsureAssets(params string[] ids)
        {
            string path = "test-ensure-assets-config.yml";
            string[] lines = {  "version: 2",
                                "logger:",
                                "  console:",
                                "    level: verbose",
                                "cognite:",
                               $"  project: {_project}",
                               $"  host: {_host}",
                                "  cdf-chunking:",
                                "    assets: 2",
                                "  cdf-throttling:",
                                "    assets: 2" };
            System.IO.File.WriteAllLines(path, lines);

            var mocks = TestUtilities.GetMockedHttpClientFactory(mockEnsureAssetsSendAsync);
            var mockHttpMessageHandler = mocks.handler;
            var mockFactory = mocks.factory;

            // Setup services
            var services = new ServiceCollection();
            services.AddSingleton<IHttpClientFactory>(mockFactory.Object); // inject the mock factory
            services.AddConfig<BaseConfig>(path, 2);
            services.AddTestLogging(_output);
            services.AddCogniteClient("testApp");
            using (var provider = services.BuildServiceProvider())
            {
                var cogniteDestination = provider.GetRequiredService<CogniteDestination>();

                Func<IEnumerable<string>, IEnumerable<AssetCreate>> createFunction =
                    (idx) =>
                    {
                        var toCreate = new List<AssetCreate>();
                        foreach (var id in idx)
                        {
                            toCreate.Add(new AssetCreate
                            {
                                ExternalId = id,
                                Name = id
                            });
                        }
                        return toCreate;
                    };
                var ts = await cogniteDestination.GetOrCreateAssetsAsync(
                    ids,
                    createFunction,
                    RetryMode.OnErrorKeepDuplicates,
                    SanitationMode.Remove,
                    CancellationToken.None
                );
                Assert.Equal(ids?.Length, ts.Results.Where(t => ids.Contains(t.ExternalId)).Count());
                foreach (var t in ts.Results)
                {
                    _ensuredAssets.Remove(t.ExternalId, out _);
                }

                var newAssets = createFunction(ids);
                using (var source = new CancellationTokenSource(5_000))
                {
                    // a timeout would fail the test
                    await cogniteDestination.EnsureAssetsExistsAsync(newAssets, RetryMode.OnFatal, SanitationMode.Remove, source.Token);
                }
                Assert.Equal(ids.Length, _ensuredAssets
                    .Where(kvp => ids.Contains(kvp.Key)).Count());
            }

            System.IO.File.Delete(path);
        }

        #region mock
        private static ConcurrentDictionary<string, int> _ensuredTimeSeries = new ConcurrentDictionary<string, int>();

        internal static async Task<HttpResponseMessage> MockEnsureTimeSeriesSendAsync(
            HttpRequestMessage message,
            CancellationToken token)
        {
            var uri = message.RequestUri.ToString();
            var responseBody = "";
            var statusCode = HttpStatusCode.OK;

            var content = await message.Content.ReadAsStringAsync(token);
            var ids = JsonConvert.DeserializeObject<dynamic>(content);
            IEnumerable<dynamic> items = ids.items;

            if (uri.Contains("/timeseries/byids"))
            {
                Assert.True((bool)ids.ignoreUnknownIds);

                dynamic result = new ExpandoObject();
                result.items = new List<ExpandoObject>();

                foreach (var item in items)
                {
                    string id = item.externalId;
                    var ensured = _ensuredTimeSeries.TryGetValue(id, out int countdown) && countdown <= 0;
                    if (ensured || id.StartsWith("id"))
                    {
                        dynamic tsData = new ExpandoObject();
                        tsData.externalId = id;
                        tsData.isString = id.Contains("String") ? true : false;
                        result.items.Add(tsData);
                        _ensuredTimeSeries.TryAdd(id, 0);
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
                    var hasValue = _ensuredTimeSeries.TryGetValue(id, out int countdown);
                    if ((!hasValue || countdown > 0) && id.StartsWith("duplicated"))
                    {
                        var splittedId = id.Split('-');
                        var count = splittedId.Length == 2 ? int.Parse(splittedId[1]) - 1 : 0;
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

        private static ConcurrentDictionary<string, int> _ensuredAssets = new ConcurrentDictionary<string, int>();

        private static async Task<HttpResponseMessage> mockEnsureAssetsSendAsync(
            HttpRequestMessage message,
            CancellationToken token)
        {
            var uri = message.RequestUri.ToString();
            var responseBody = "";
            var statusCode = HttpStatusCode.OK;

            var content = await message.Content.ReadAsStringAsync(token);
            var ids = JsonConvert.DeserializeObject<dynamic>(content);
            IEnumerable<dynamic> items = ids.items;


            if (uri.Contains("/assets/byids"))
            {
                Assert.True((bool)ids.ignoreUnknownIds);

                dynamic result = new ExpandoObject();
                result.items = new List<ExpandoObject>();

                foreach (var item in items)
                {
                    string id = item.externalId;
                    var ensured = _ensuredAssets.TryGetValue(id, out int countdown) && countdown <= 0;
                    if (ensured || id.StartsWith("id"))
                    {
                        dynamic assetData = new ExpandoObject();
                        assetData.externalId = id;
                        result.items.Add(assetData);
                        _ensuredAssets.TryAdd(id, 0);
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
                    var hasValue = _ensuredAssets.TryGetValue(id, out int countdown);
                    if ((!hasValue || countdown > 0) && id.StartsWith("duplicated"))
                    {
                        var splittedId = id.Split('-');
                        var count = splittedId.Length == 2 ? int.Parse(splittedId[1]) - 1 : 0;
                        dynamic duplicatedId = new ExpandoObject();
                        duplicatedId.externalId = id;
                        duplicateData.error.duplicated.Add(duplicatedId);
                        _ensuredAssets[id] = hasValue ? countdown - 1 : count;
                    }
                    else
                    {
                        dynamic assetData = new ExpandoObject();
                        assetData.externalId = id;
                        result.items.Add(assetData);
                        _ensuredAssets.TryAdd(id, 0);
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

        private static Task<HttpResponseMessage> mockAuthSendAsync(HttpRequestMessage message, CancellationToken token)
        {
            // Verify endpoint and method
            Assert.Equal($@"http://example.url/token", message.RequestUri.ToString());
            Assert.Equal(HttpMethod.Post, message.Method);

            if (_tokenCounter == 2) //third call fails
            {
                var errorReply = "{" + Environment.NewLine +
                                $"  \"error\": \"invalid_scope\"{Environment.NewLine}" +
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

        private static int _sendRetries;

        private static Task<HttpResponseMessage> mockCogniteAssetsRetryAsync(HttpRequestMessage message, CancellationToken token)
        {
            Assert.Equal($"{_host}/api/v1/projects/{_project}/assets/list", message.RequestUri.ToString());
            Assert.Equal(HttpMethod.Post, message.Method);
            if (_sendRetries++ < 2)
            {
                var errReply = "{" + Environment.NewLine +
                    "  \"error\": {" + Environment.NewLine +
                    "    \"code\": 500," + Environment.NewLine +
                    "    \"message\": \"Internal server error\"" + Environment.NewLine +
                    "  }" + Environment.NewLine +
                    "}";
                var response = new HttpResponseMessage
                {
                    StatusCode = HttpStatusCode.InternalServerError,
                    Content = new StringContent(errReply)
                };
                return Task.FromResult(response);
            }
            var reply = @"{""items"":[]}";

            return Task.FromResult(new HttpResponseMessage
            {
                StatusCode = HttpStatusCode.OK,
                Content = new StringContent(reply)
            });

        }

        private static Task<HttpResponseMessage> mockAuthRetryAsync(HttpRequestMessage message, CancellationToken token)
        {
            var uri = message.RequestUri.ToString();
            if (uri.Contains("/token"))
            {
                // First two token requests fail.
                if (_tokenCounter < 2)
                {
                    _tokenCounter++;
                    var errorReply = "{" + Environment.NewLine +
                                $"  \"error\": \"something_wrong\"{Environment.NewLine}" +
                                 "}";
                    var errorResponse = new HttpResponseMessage
                    {
                        StatusCode = HttpStatusCode.InternalServerError,
                        Content = new StringContent(errorReply)
                    };
                    return Task.FromResult(errorResponse);
                }
                // build expected response
                var reply = "{" + Environment.NewLine +
                           $"  \"token_type\": \"Bearer\",{Environment.NewLine}" +
                           $"  \"expires_in\": 0,{Environment.NewLine}" +
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
            else
            {
                // First two requests fail.
                if (_sendRetries < 2)
                {
                    _sendRetries++;
                    var errReply = "{" + Environment.NewLine +
                    "  \"error\": {" + Environment.NewLine +
                    "    \"code\": 500," + Environment.NewLine +
                    "    \"message\": \"Internal server error\"" + Environment.NewLine +
                    "  }" + Environment.NewLine +
                    "}";
                    var response = new HttpResponseMessage
                    {
                        StatusCode = HttpStatusCode.InternalServerError,
                        Content = new StringContent(errReply)
                    };
                    return Task.FromResult(response);
                }

                _sendRetries++;
                var reply = @"{""items"":[]}";

                return Task.FromResult(new HttpResponseMessage
                {
                    StatusCode = HttpStatusCode.OK,
                    Content = new StringContent(reply)
                });
            }
        }

        public class HttpMessageHandlerStub : HttpMessageHandler
        {
            private readonly Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> _sendAsync;

            public HttpMessageHandlerStub(Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> sendAsync)
            {
                _sendAsync = sendAsync;
            }

            protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
            {
                return await _sendAsync(request, cancellationToken);
            }
        }

        #endregion
    }
}