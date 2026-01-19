using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using Cognite.Extractor.Configuration;
using Cognite.Extractor.Logging;
using Cognite.Extractor.Testing;
using Cognite.Extractor.Testing.Mock;
using Cognite.Extractor.Utils;
using CogniteSdk;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;
using Xunit.Sdk;

namespace ExtractorUtils.Test
{
    /// <summary>
    /// Tests for the Cognite.Testing library.
    /// </summary>
    public class SelfTest
    {
        private readonly ITestOutputHelper _output;
        public SelfTest(ITestOutputHelper output)
        {
            _output = output;
        }

        private BaseConfig GetConfig()
        {
            return new BaseConfig
            {
                Cognite = new CogniteConfig
                {
                    Project = "test-project",
                    IdpAuthentication = new Cognite.Extensions.AuthenticatorConfig
                    {
                        ClientId = "test-client-id",
                        Secret = "test-secret",
                        TokenUrl = "https://cognite.com/test/token"
                    },
                    CdfRetries = new RetryConfig
                    {
                        MaxRetries = 0,
                    }
                },
                Logger = new LoggerConfig
                {
                    Console = new ConsoleConfig
                    {
                        Level = "debug",
                    }
                }
            };
        }

        private ServiceProvider GetCdfMockProvider()
        {
            var services = new ServiceCollection();
            services.AddConfig(GetConfig(), [typeof(CogniteConfig), typeof(LoggerConfig)]);
            services.AddTestLogging(_output);
            CdfMock.RegisterHttpClient(services);
            services.AddCogniteClient("myApp");
            return services.BuildServiceProvider();
        }

        [Fact]
        public async Task TestCdfMock()
        {
            using var provider = GetCdfMockProvider();
            provider.GetRequiredService<ILogger<CdfMock>>();

            var mock = provider.GetRequiredService<CdfMock>();
            mock.AddMatcher(new SimpleMatcher("post", "^/api/v1/projects/[a-zA-Z0-9\\-]+/assets/list$", (ctx, token) =>
            {
                return ctx.CreateJsonResponse(new ItemsWithoutCursor<Asset>
                {
                    Items = new List<Asset>
                    {
                        new Asset { Id = 1, Name = "Asset1" },
                        new Asset { Id = 2, Name = "Asset2" }
                    }
                });
            }, Times.Once()));
            mock.AddTokenEndpoint(Times.Once());

            var client = provider.GetRequiredService<Client>();
            var assets = await client.Assets.ListAsync(new AssetQuery(), TestContext.Current.CancellationToken);
            Assert.NotNull(assets);
            Assert.Equal(2, assets.Items.Count());
            Assert.Equal("Asset1", assets.Items.ElementAt(0).Name);
            Assert.Equal("Asset2", assets.Items.ElementAt(1).Name);
        }

        [Fact]
        public void TestCdfMockExpectedRequestCount()
        {
            Assert.Throws<TrueException>(() =>
            {
                using var provider = GetCdfMockProvider();

                var mock = provider.GetRequiredService<CdfMock>();
                mock.AddTokenEndpoint(Times.Exactly(2));
            });
        }

        [Fact]
        public async Task TestCdfMockBlockedRequest()
        {
            using var provider = GetCdfMockProvider();
            var mock = provider.GetRequiredService<CdfMock>();
            mock.AddTokenEndpoint(Times.Once());
            mock.AddMatcher(new SimpleMatcher("post", "^/api/v1/projects/[a-zA-Z0-9\\-]+/assets/list$", (ctx, token) =>
            {
                return ctx.CreateJsonResponse(new ItemsWithoutCursor<Asset>
                {
                    Items = new List<Asset>
                    {
                        new Asset { Id = 1, Name = "Asset1" },
                        new Asset { Id = 2, Name = "Asset2" }
                    }
                });
            }, Times.Once()));
            mock.GetMatcher(HttpMethod.Post, "/api/v1/projects/project/assets/list").ForceErrorStatus = 500;
            var client = provider.GetRequiredService<Client>();
            var exc = await Assert.ThrowsAsync<ResponseException>(async () => await client.Assets.ListAsync(new AssetQuery(), TestContext.Current.CancellationToken));
            Assert.Equal(500, exc.Code);

            mock.RejectAllMessages = true;
            exc = await Assert.ThrowsAsync<ResponseException>(async () => await client.Assets.ListAsync(new AssetQuery(), TestContext.Current.CancellationToken));
            Assert.Equal(503, exc.Code);
        }
    }
}