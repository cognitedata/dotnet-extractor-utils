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
using Cognite.Extensions;
using Cognite.Extractor.Utils;
using Cognite.Extractor.Utils.Unstable.Configuration;
using CogniteSdk.Alpha;
using System.Net.Http.Headers;
using CogniteSdk;
using Microsoft.Extensions.Logging;
using System.IO;
using ExtractorUtils.Test.unit.Unstable;
using Cognite.Extractor.Common;
using System.Dynamic;
using Newtonsoft.Json;

namespace ExtractorUtils.Test.Unit.Unstable
{
    public class ConnectionConfigTests
    {
        private int _tokenCounter;

        private readonly ITestOutputHelper _output;
        public ConnectionConfigTests(ITestOutputHelper output)
        {
            _output = output;
        }

        private ConnectionConfig GetConfig()
        {
            return new ConnectionConfig
            {
                Project = "project",
                BaseUrl = "https://greenfield.cognitedata.com",
                Integration = "test-integration",
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
        }

        [Fact]
        public async Task TestGetClient()
        {
            var config = GetConfig();
            _tokenCounter = 0;

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
            services.AddCogniteDestination();
            using var provider = services.BuildServiceProvider();

            var auth = provider.GetRequiredService<IAuthenticator>();
            var token = await auth.GetToken();
            Assert.Equal("token0", token);

            provider.GetRequiredService<CogniteDestination>();
            provider.GetRequiredService<CogniteDestinationWithIDM>();
        }


        private Task<HttpResponseMessage> mockAuthSendAsync(HttpRequestMessage message, CancellationToken token)
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

        class MyFancyConfig : VersionedConfig
        {
            public int Foo { get; set; }
            public string Bar { get; set; }

            public override void GenerateDefaults()
            {
            }
        }

        private (ConfigSource<MyFancyConfig>, DummySink) GetRemoteConfigSource(string configPath)
        {
            var config = GetConfig();
            _tokenCounter = 0;

            var services = new ServiceCollection();
            services.AddConfig(config, typeof(ConnectionConfig));
            var mocks = TestUtilities.GetMockedHttpClientFactory(mockGetConfig);
            var mockHttpMessageHandler = mocks.handler;
            var mockFactory = mocks.factory;
            services.AddSingleton(mockFactory.Object);
            services.AddTestLogging(_output);
            DestinationUtilsUnstable.AddCogniteClient(services, "myApp", null, setLogger: true, setMetrics: true, setHttpClient: true);
            var provider = services.BuildServiceProvider();

            Directory.CreateDirectory(configPath);

            var configFile = configPath + "/config.yml";

            var source = new RemoteConfigSource<MyFancyConfig>(
                provider.GetRequiredService<Client>(),
                provider.GetRequiredService<ILogger<ConfigSource<MyFancyConfig>>>(),
                "test-integration",
                configFile,
                true);

            var reporter = new DummySink();

            return (source, reporter);
        }

        private (ConfigSource<MyFancyConfig>, DummySink) GetLocalConfigSource(string configPath)
        {
            var services = new ServiceCollection();
            services.AddTestLogging(_output);
            var provider = services.BuildServiceProvider();
            Directory.CreateDirectory(configPath);

            var configFile = configPath + "/config.yml";
            var source = new LocalConfigSource<MyFancyConfig>(
                provider.GetRequiredService<ILogger<ConfigSource<MyFancyConfig>>>(),
                configFile
            );

            return (source, new DummySink());
        }

        [Fact]
        public async Task TestConfigSource()
        {
            var config = GetConfig();

            var configPath = TestUtils.AlphaNumericPrefix("dotnet_extractor_test") + "_config";
            var (source, reporter) = GetLocalConfigSource(configPath);
            var configFile = (source as LocalConfigSource<MyFancyConfig>).ConfigFilePath;

            // Try to load a new config when one doesn't exist.
            await Assert.ThrowsAnyAsync<Exception>(async () => await source.ResolveConfig(null, reporter, CancellationToken.None));

            // Write an invalid local file.
            System.IO.File.WriteAllText(configFile, @"
foo: 123
baz: test
            ");
            await Assert.ThrowsAsync<ConfigurationException>(async () => await source.ResolveConfig(null, reporter, CancellationToken.None));

            // 2 start, 2 end.
            Assert.Equal(4, reporter.Errors.Count);

            await Assert.ThrowsAsync<ConfigurationException>(async () => await source.ResolveConfig(null, reporter, CancellationToken.None));
            // Nothing has changed, no new errors.
            Assert.Equal(4, reporter.Errors.Count);

            // Write a valid local file.
            System.IO.File.WriteAllText(configFile, @"
foo: 123
bar: test
            ");
            var isNew = await source.ResolveConfig(null, reporter, CancellationToken.None);
            Assert.True(isNew);

            isNew = await source.ResolveConfig(null, reporter, CancellationToken.None);
            Assert.False(isNew);
            Assert.Equal(123, source.Config.Foo);
            Assert.Equal("test", source.Config.Bar);

            (source, reporter) = GetRemoteConfigSource(configPath);

            // Fail to fetch remote config
            await Assert.ThrowsAnyAsync<Exception>(async () => await source.ResolveConfig(null, reporter, CancellationToken.None));
            // Another 2 error reports.
            Assert.Equal(2, reporter.Errors.Count);

            // Fail to fetch again with the same error.
            await Assert.ThrowsAnyAsync<Exception>(async () => await source.ResolveConfig(null, reporter, CancellationToken.None));
            // No new reports.
            Assert.Equal(2, reporter.Errors.Count);

            _responseRevision = new ConfigRevision
            {
                ExternalId = "test-integration",
                Config = @"
foo: 321
bar: test
",
                Revision = 1,
            };

            Assert.Equal(2, _getConfigCount);

            isNew = await source.ResolveConfig(null, reporter, CancellationToken.None);
            Assert.True(isNew);
            Assert.Equal(321, source.Config.Foo);
            Assert.Equal("test", source.Config.Bar);

            isNew = await source.ResolveConfig(1, reporter, CancellationToken.None);
            Assert.False(isNew);
            // Only one new request
            Assert.Equal(3, _getConfigCount);

            Assert.True(System.IO.File.Exists(configPath + "/_temp_config.yml"));

            Directory.Delete(configPath, true);
        }

        [Fact]
        public async Task TestBufferConfigFile()
        {
            var config = GetConfig();
            var configPath = TestUtils.AlphaNumericPrefix("dotnet_extractor_test") + "_config";
            var (source, reporter) = GetRemoteConfigSource(configPath);
            var bufferFile = configPath + "/_temp_config.yml";

            var okRevision = new ConfigRevision
            {
                ExternalId = "test-integration",
                Config = @"
foo: 321
bar: test
",
                Revision = 1,
            };
            _responseRevision = okRevision;

            var isNew = await source.ResolveConfig(null, reporter, CancellationToken.None);
            Assert.True(isNew);
            Assert.True(System.IO.File.Exists(bufferFile));

            // We can load the config from the buffer file.
            _responseRevision = null;
            isNew = await source.ResolveConfig(null, reporter, CancellationToken.None);
            Assert.True(isNew);

            // Make the file write protected. Now loading remote config should fail until we delete it.
            System.IO.File.SetAttributes(bufferFile, FileAttributes.ReadOnly);
            _responseRevision = okRevision;
            await Assert.ThrowsAnyAsync<Exception>(async () => await source.ResolveConfig(null, reporter, CancellationToken.None));

            // Delete the buffer file, loading remote config should now fail.
            System.IO.File.SetAttributes(bufferFile, FileAttributes.Normal);
            System.IO.File.Delete(bufferFile);
            _responseRevision = null;
            await Assert.ThrowsAnyAsync<Exception>(async () => await source.ResolveConfig(null, reporter, CancellationToken.None));

            Directory.Delete(configPath, true);
        }

        private ConfigRevision _responseRevision;
        private int _getConfigCount;

        private async Task<HttpResponseMessage> mockGetConfig(HttpRequestMessage message, CancellationToken token)
        {
            var uri = message.RequestUri.ToString();
            if (uri == "http://example.url/token") return await mockAuthSendAsync(message, token);

            Assert.Contains("/integrations/config", uri);
            _getConfigCount++;

            if (_responseRevision == null)
            {
                dynamic res = new ExpandoObject();
                res.error = new ExpandoObject();
                res.error.code = 400;
                res.error.message = "Something went wrong";
                return new HttpResponseMessage
                {
                    StatusCode = HttpStatusCode.BadRequest,
                    Content = new StringContent(JsonConvert.SerializeObject(res)),
                };
            }

            var resBody = System.Text.Json.JsonSerializer.Serialize(_responseRevision, Oryx.Cognite.Common.jsonOptions);
            var fresponse = new HttpResponseMessage
            {
                StatusCode = HttpStatusCode.OK,
                Content = new StringContent(resBody)
            };
            fresponse.Content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
            fresponse.Headers.Add("x-request-id", "1");

            return fresponse;
        }
    }
}