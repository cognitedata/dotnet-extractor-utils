using System;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extensions.Unstable;
using Cognite.Extractor.Configuration;
using Cognite.Extractor.Utils.Unstable;
using Microsoft.Extensions.DependencyInjection;
using Xunit;
using Xunit.Abstractions;
using Cognite.Extractor.Testing;
using Cognite.ExtractorUtils.Unstable;
using Cognite.Extensions;
using Cognite.Extractor.Utils;
using Cognite.ExtractorUtils.Unstable.Configuration;

namespace ExtractorUtils.Test.Unit.Unstable
{
    public class ConnectionConfigTests
    {
        private static int _tokenCounter;

        private readonly ITestOutputHelper _output;
        public ConnectionConfigTests(ITestOutputHelper output)
        {
            _output = output;
        }

        [Fact]
        public async Task TestGetClient()
        {
            var config = new ConnectionConfig
            {
                Project = "project",
                BaseUrl = "https://greenfield.cognitedata.com",
                Authentication = new ClientCredentialsConfig
                {
                    ClientId = "someId",
                    ClientSecret = "thisIsASecret",
                    Scopes = new Cognite.Common.ListOrSpaceSeparated("https://greenfield.cognitedata.com/.default"),
                    MinTtl = "60s",
                    Resource = "resource",
                    Audience = "audience",
                    TokenUrl = "http://example.url/token",
                }
            };

            var baseCogniteConfig = new BaseCogniteConfig();

            var services = new ServiceCollection();
            services.AddConfig(config, typeof(ConnectionConfig));
            services.AddConfig(baseCogniteConfig, typeof(BaseCogniteConfig));
            var mocks = TestUtilities.GetMockedHttpClientFactory(mockAuthSendAsync);
            var mockHttpMessageHandler = mocks.handler;
            var mockFactory = mocks.factory;
            services.AddSingleton(mockFactory.Object);
            services.AddTestLogging(_output);
            DestinationUtilsUnstable.AddCogniteClient(services, "myApp", null, setLogger: true, setMetrics: true, setHttpClient: true);
            using var provider = services.BuildServiceProvider();

            var auth = provider.GetRequiredService<IAuthenticator>();
            var token = await auth.GetToken();
            Assert.Equal("token0", token);

            provider.GetRequiredService<CogniteDestination>();
            provider.GetRequiredService<CogniteDestinationWithIDM>();
        }


        private static Task<HttpResponseMessage> mockAuthSendAsync(HttpRequestMessage message, CancellationToken token)
        {
            // Verify endpoint and method
            Assert.Equal($@"http://example.url/token", message.RequestUri.ToString());
            Assert.Equal(HttpMethod.Post, message.Method);

            // build expected response
            var reply = "{" + Environment.NewLine +
                       $"  \"token_type\": \"Bearer\",{Environment.NewLine}" +
                       $"  \"expires_in\": 2,{Environment.NewLine}" +
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
    }
}