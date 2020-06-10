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
using Com.Cognite.V1.Timeseries.Proto;
using Microsoft.Extensions.DependencyInjection;
using Moq;
using Moq.Protected;
using Newtonsoft.Json;
using Xunit;
using Cognite.Extractor.Logging;
using Cognite.Extractor.Utils;

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
                var cogniteDestination = provider.GetRequiredService<CogniteDestination>();
                var ex = await Assert.ThrowsAsync<CogniteUtilsException>(() => cogniteDestination.CogniteClient.TestCogniteConfig(null, CancellationToken.None));
                Assert.Contains("configuration missing", ex.Message);

                config.Project = null;
                ex = await Assert.ThrowsAsync<CogniteUtilsException>(() => cogniteDestination.CogniteClient.TestCogniteConfig(config, CancellationToken.None));
                Assert.Contains("project is not configured", ex.Message);

                config.Project = "Bogus";
                ex = await Assert.ThrowsAsync<CogniteUtilsException>(() => cogniteDestination.CogniteClient.TestCogniteConfig(config, CancellationToken.None));
                Assert.Contains("not associated with project Bogus", ex.Message);
                config.Project = _project;

                await cogniteDestination.TestCogniteConfig(CancellationToken.None);

                var loginStatus = await cogniteDestination.CogniteClient.Login.StatusAsync(CancellationToken.None);
                Assert.True(loginStatus.LoggedIn);
                Assert.Equal("testuser", loginStatus.User);
                Assert.Equal(_project, loginStatus.Project);

                var options = new TimeSeriesQuery()
                {
                    Limit = 1
                };
                var ts = await cogniteDestination.CogniteClient.TimeSeries.ListAsync(options);
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
            string path = "test-cognite-invalid-client-config.yml";
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
                               $"  api-key: {_apiKey}",
                               $"  host: {_host}",
                                "  cdf-chunking:",
                                "    time-series: 2",
                                "  cdf-throttling:",
                                "    time-series: 2" };
            System.IO.File.WriteAllLines(path, lines);

            var mocks = TestUtilities.GetMockedHttpClientFactory(mockEnsureTimeSeriesSendAsync);
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
                var ts = await cogniteDestination.GetOrCreateTimeSeriesAsync(
                    ids,
                    createFunction,
                    CancellationToken.None
                );
                Assert.Equal(ids.Count(), ts.Where(t => ids.Contains(t.ExternalId)).Count());
                foreach (var t in ts)
                {
                    _ensuredTimeSeries.Remove(t.ExternalId);
                }

                var newTs = createFunction(ids);
                using (var source = new CancellationTokenSource(5_000))
                {
                    // a timeout would fail the test
                    await cogniteDestination.EnsureTimeSeriesExistsAsync(newTs, source.Token);
                }
                Assert.Equal(ids.Count(), _ensuredTimeSeries
                    .Where(kvp => ids.Contains(kvp.Key)).Count());
            }

            System.IO.File.Delete(path);
        }

        private static Dictionary<string, List<Datapoint>> _createdDataPoints = new Dictionary<string, List<Datapoint>>();
        
        [Fact]
        public async Task TestInsertDataPoints()
        {
            string path = "test-insert-data-points-config.yml";
            string[] lines = {  "version: 2",
                                "logger:",
                                "  console:",
                                "    level: verbose",
                                "cognite:",
                               $"  project: {_project}",
                               $"  api-key: {_apiKey}",
                               $"  host: {_host}",
                                "  cdf-chunking:",
                                "    data-points: 4",
                                "    data-point-time-series: 2",
                                "  cdf-throttling:",
                                "    data-points: 2" };
            System.IO.File.WriteAllLines(path, lines);

            var mocks = TestUtilities.GetMockedHttpClientFactory(mockInsertDataPointsAsync);
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

                double[] doublePoints = { 0.0, 1.1, 2.2, double.NaN, 3.3, 4.4, double.NaN, 5.5, double.NegativeInfinity };
                string[] stringPoints = { "0", null, "1", new string('!', CogniteUtils.StringLengthMax), new string('2', CogniteUtils.StringLengthMax + 1), "3"};
                
                var datapoints = new Dictionary<Identity, IEnumerable<Datapoint>>() {
                    { new Identity("A"), new Datapoint[] { new Datapoint(DateTime.UtcNow, "1")}},
                    { new Identity("A"), new Datapoint[] { new Datapoint(DateTime.UtcNow, "2")}},
                    { new Identity(1), doublePoints.Select(d => new Datapoint(DateTime.UtcNow, d))},
                    { new Identity(2), stringPoints.Select(s => new Datapoint(DateTime.UtcNow, s))},
                    { new Identity(3), new Datapoint[] { } },
                    { new Identity(4), new Datapoint[] { new Datapoint(CogniteTime.DateTimeEpoch, 1), new Datapoint(DateTime.MaxValue, 1)}}
                };
                _createdDataPoints.Clear();
                await cogniteDestination.InsertDataPointsAsync(
                    datapoints,
                    CancellationToken.None);
                Assert.False(_createdDataPoints.ContainsKey(3 + "")); // No data points
                Assert.False(_createdDataPoints.ContainsKey(4 + "")); // Invalid timestamps
                Assert.Equal(6, _createdDataPoints[1 + ""].Count());
                Assert.Equal(2, _createdDataPoints["A"].Count());
                Assert.Empty(_createdDataPoints[1 + ""]
                    .Where(dp => dp.NumericValue == null || dp.NumericValue == double.NaN || dp.NumericValue == double.NegativeInfinity));
                Assert.Equal(5, _createdDataPoints[2 + ""].Count());
                Assert.Empty(_createdDataPoints[2 + ""]
                    .Where(dp => dp.StringValue == null || dp.StringValue.Length > CogniteUtils.StringLengthMax));

                _createdDataPoints.Clear();
                datapoints = new Dictionary<Identity, IEnumerable<Datapoint>>() {
                    { new Identity("idMissing1"), new Datapoint[] { new Datapoint(DateTime.UtcNow, "1")}},
                    { new Identity("idNumeric1"), new Datapoint[] { new Datapoint(DateTime.UtcNow, 1)}},
                    { new Identity("idNumeric2"), new Datapoint[] { new Datapoint(DateTime.UtcNow, 1)}},
                    { new Identity(-1), doublePoints.Select(d => new Datapoint(DateTime.UtcNow, d)).Take(2)},
                    { new Identity("idMismatchedString1"), new Datapoint[] { new Datapoint(DateTime.UtcNow, 1)}},
                    { new Identity("idString1"), new Datapoint[] { new Datapoint(DateTime.UtcNow, "1")}},
                    { new Identity("idMismatched2"), new Datapoint[] { new Datapoint(DateTime.UtcNow, "1")}}
                };
                var errors = await cogniteDestination.InsertDataPointsIgnoreErrorsAsync(
                    datapoints,
                    CancellationToken.None);
                var comparer = new IdentityComparer();
                Assert.Contains(new Identity("idMissing1"), errors.IdsNotFound, comparer);
                Assert.Contains(new Identity(-1), errors.IdsNotFound, comparer);
                Assert.Contains(new Identity("idMismatchedString1"), errors.IdsWithMismatchedData, comparer);
                Assert.Contains(new Identity("idMismatched2"), errors.IdsWithMismatchedData, comparer);
                Assert.Single(_createdDataPoints["idNumeric1"]);
                Assert.Single(_createdDataPoints["idNumeric2"]);
                Assert.Single(_createdDataPoints["idString1"]);
            }

            System.IO.File.Delete(path);
        }

        private static Dictionary<string, int> _ensuredTimeSeries = new Dictionary<string, int>();

        private static async Task<HttpResponseMessage> mockEnsureTimeSeriesSendAsync(
            HttpRequestMessage message, 
            CancellationToken token) {
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
                        tsData.isString = id.Contains("String") ? true : false;
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

        private static async Task<HttpResponseMessage> mockInsertDataPointsAsync(HttpRequestMessage message , CancellationToken token) {
            var uri = message.RequestUri.ToString();

            if (uri.Contains("/timeseries/byids"))
            {
                return await mockEnsureTimeSeriesSendAsync(message, token);
            }
            Assert.Contains($"{_project}/timeseries/data", uri);

            var responseBody = "{ }";
            var statusCode = HttpStatusCode.OK;
            var bytes = await message.Content.ReadAsByteArrayAsync();
            var data = DataPointInsertionRequest.Parser.ParseFrom(bytes);
            Assert.True(data.Items.Count <= 2); // data-points-time-series chunk size
            Assert.True(data.Items
                .Select(i => i.DatapointTypeCase == DataPointInsertionItem.DatapointTypeOneofCase.NumericDatapoints ?
                        i.NumericDatapoints.Datapoints.Count : i.StringDatapoints.Datapoints.Count)
                .Sum() <= 4); // data-points chunk size
            
            dynamic missingResponse = new ExpandoObject();
            missingResponse.error = new ExpandoObject();
            missingResponse.error.missing = new List<ExpandoObject>();
            missingResponse.error.code = 400;
            missingResponse.error.message = "Time series ids not found";

            dynamic mismatchedResponse = new ExpandoObject();
            mismatchedResponse.error = new ExpandoObject();
            mismatchedResponse.error.code = 400;
            mismatchedResponse.error.message = "";

            foreach (var item in data.Items)
            {
                if (item.Id < 0 || item.ExternalId.StartsWith("idMissing"))
                {
                    dynamic id = new ExpandoObject();
                    if (!string.IsNullOrEmpty(item.ExternalId)) id.externalId = item.ExternalId;
                    else id.id = item.Id;
                    missingResponse.error.missing.Add(id);
                }
                else if (item.ExternalId.StartsWith("idMismatched"))
                {
                    if (item.NumericDatapoints != null)
                    {
                        mismatchedResponse.error.message = "Expected string value for datapoint";
                    }
                    else {
                        mismatchedResponse.error.message = "Expected numeric value for datapoint";
                    }
                    break;
                }
            }
            if (!string.IsNullOrEmpty(mismatchedResponse.error.message))
            {
                statusCode = HttpStatusCode.BadRequest;
                responseBody = JsonConvert.SerializeObject(mismatchedResponse);
            }
            else if (missingResponse.error.missing.Count > 0) {
                statusCode = HttpStatusCode.BadRequest;
                responseBody = JsonConvert.SerializeObject(missingResponse);
            }
            else 
            {
                foreach (var item in data.Items)
                {
                    var sId = string.IsNullOrEmpty(item.ExternalId) ? item.Id + "" : item.ExternalId;
                    if (!_createdDataPoints.TryGetValue(sId, out List<Datapoint> dps))
                    {
                        dps = new List<Datapoint>();
                        _createdDataPoints.TryAdd(sId, dps);
                    }
                    if (item.NumericDatapoints != null)
                    {
                        foreach (var dp in item.NumericDatapoints.Datapoints)
                        {
                            dps.Add(new Datapoint(CogniteTime.FromUnixTimeMilliseconds(dp.Timestamp), dp.Value));
                        }
                    }
                    else if (item.StringDatapoints != null)
                    {
                        foreach (var dp in item.StringDatapoints?.Datapoints)
                        {
                            dps.Add(new Datapoint(CogniteTime.FromUnixTimeMilliseconds(dp.Timestamp), dp.Value));
                        }
                    }
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