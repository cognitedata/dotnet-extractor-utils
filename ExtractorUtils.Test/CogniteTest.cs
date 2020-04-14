using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using CogniteSdk;
using Microsoft.Extensions.DependencyInjection;
using Moq;
using Moq.Protected;
using Xunit;

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

            var mockHttpMessageHandler = new Mock<HttpMessageHandler>();
            mockHttpMessageHandler.Protected()
                .Setup<Task<HttpResponseMessage>>("SendAsync", 
                                                  ItExpr.IsAny<HttpRequestMessage>(), 
                                                  ItExpr.IsAny<CancellationToken>())
                .Returns<HttpRequestMessage, CancellationToken>(mockAuthSendAsync);
            var client = new HttpClient(mockHttpMessageHandler.Object);
            var mockFactory = new Mock<IHttpClientFactory>();
            mockFactory.Setup(_ => _.CreateClient(It.IsAny<string>())).Returns(client);

            // Setup services
            var services = new ServiceCollection();
            services.AddSingleton<IHttpClientFactory>(mockFactory.Object); // inject the mock factory
            services.AddConfig<BaseConfig>(path, new List<int>() { 2 });
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

            // Verify that the authentication endpoint was called 2 times
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
                reply = "{" + Environment.NewLine + 
                        "  \"data\": {" +  Environment.NewLine +
                       $"    \"user\": \"testuser\",{Environment.NewLine}" +
                       $"    \"loggedIn\": true{Environment.NewLine}" +
                        "  }" + Environment.NewLine +
                        "}";
            }
            else {
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

            var mockHttpMessageHandler = new Mock<HttpMessageHandler>();
            mockHttpMessageHandler.Protected()
                .Setup<Task<HttpResponseMessage>>("SendAsync", 
                                                  ItExpr.IsAny<HttpRequestMessage>(), 
                                                  ItExpr.IsAny<CancellationToken>())
                .Returns<HttpRequestMessage, CancellationToken>(mockCogniteSendAsync);
            var client = new HttpClient(mockHttpMessageHandler.Object);
            var mockFactory = new Mock<IHttpClientFactory>();
            mockFactory.Setup(_ => _.CreateClient(It.IsAny<string>())).Returns(client);

            // Setup services
            var services = new ServiceCollection();
            services.AddSingleton<IHttpClientFactory>(mockFactory.Object); // inject the mock factory
            services.AddConfig<BaseConfig>(path, new List<int>() { 2 });
            services.AddLogger();
            services.AddCogniteClient("testApp");
            using (var provider = services.BuildServiceProvider()) {
                var cogClient = provider.GetRequiredService<Client>();
                var loginStatus = await cogClient.Login.StatusAsync(CancellationToken.None);
                Assert.True(loginStatus.LoggedIn);
                Assert.Equal("testuser", loginStatus.User);

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
                    Times.Exactly(2),
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>());

            System.IO.File.Delete(path);
        }
    }
}