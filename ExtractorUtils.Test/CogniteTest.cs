using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Moq;
using Moq.Protected;
using Xunit;

namespace ExtractorUtils.Test
{

    
    public class CogniteTest
    {
        private const string _authTenant = "someTenant";
        private static int _tokenCounter = 0;
        private static Task<HttpResponseMessage> mockSendAsync(HttpRequestMessage message , CancellationToken token) {
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
            var clientInt = "someId";
            string path = "test-authenticator-config.yml";
            string[] lines = {  "version: 2",
                                "logger:",
                                "  console:",
                                "    level: verbose",
                                "cognite:",
                                "  idp-authentication:",
                               $"    client-id: {clientInt}",
                               $"    tenant: {_authTenant}",
                                "    secret: thisIsASecret",
                                "    scope: thisIsAScope",
                                "    min-ttl: 0" };
            File.WriteAllLines(path, lines);

            var mockHttpMessageHandler = new Mock<HttpMessageHandler>();
            mockHttpMessageHandler.Protected()
                .Setup<Task<HttpResponseMessage>>("SendAsync", 
                                                  ItExpr.IsAny<HttpRequestMessage>(), 
                                                  ItExpr.IsAny<CancellationToken>())
                .Returns<HttpRequestMessage, CancellationToken>(mockSendAsync);
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
            
            File.Delete(path);
        }
    }
}