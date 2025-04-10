using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extractor.Testing;
using Cognite.Extractor.Utils.Unstable.Runtime;
using Cognite.Extractor.Utils.Unstable.Configuration;
using Cognite.Extractor.Configuration;
using ExtractorUtils.Test.Unit.Unstable;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Xunit;
using Xunit.Abstractions;
using System.Net.Http;
using System;
using System.Net;
using Newtonsoft.Json;
using System.Collections.Generic;
using CogniteSdk.Alpha;
using System.Dynamic;
using System.Net.Http.Headers;
using Cognite.Extractor.Utils.Unstable.Tasks;
using CogniteSdk;
using Cognite.Extractor.Utils.Unstable;
using Cognite.Extensions.Unstable;

namespace ExtractorUtils.Test.unit.Unstable
{
    public class RuntimeTest
    {
        private readonly ITestOutputHelper _output;

        public RuntimeTest(ITestOutputHelper output)
        {
            _output = output;
        }

        private string GetConnectionConfig()
        {
            return @"
project: project
base-url: https://api.cognitedata.com
integration: test-integration
authentication:
  type: client-credentials
  client-id: someId
  client-secret: thisIsASecret
  token-url: http://example.url/token
  scopes:
    - https://greenfield.cognitedata.com/.default
";
        }

        [Fact(Timeout = 5000)]
        public async Task TestRuntimeBuilder()
        {
            var builder = new ExtractorRuntimeBuilder<DummyConfig, DummyExtractor>("dotnetutilstest", "utilstest/1");
            builder.ConfigSource = ConfigSourceType.Local;
            var services = new ServiceCollection();
            services.AddTestLogging(_output);
            builder.StartupLogger = services.BuildServiceProvider().GetRequiredService<ILogger<RuntimeTest>>();

            var configPath = TestUtils.AlphaNumericPrefix("dotnet_extractor_test") + "_config";
            builder.ConfigFolder = configPath;
            Directory.CreateDirectory(configPath);
            System.IO.File.WriteAllText(configPath + "/connection.yml", GetConnectionConfig());

            var runtime = await builder.MakeRuntime(CancellationToken.None);

            var configSource = runtime.GetType().GetField("_configSource", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            Assert.IsType<LocalConfigSource<DummyConfig>>(configSource.GetValue(runtime));

            Directory.Delete(configPath, true);
        }

        [Fact(Timeout = 5000)]
        public async Task TestRuntimeBuilderRemote()
        {
            var builder = new ExtractorRuntimeBuilder<DummyConfig, DummyExtractor>("dotnetutilstest", "utilstest/1");
            builder.ConfigSource = ConfigSourceType.Remote;
            var services = new ServiceCollection();
            services.AddTestLogging(_output);
            builder.StartupLogger = services.BuildServiceProvider().GetRequiredService<ILogger<RuntimeTest>>();

            var configPath = TestUtils.AlphaNumericPrefix("dotnet_extractor_test") + "_config";
            builder.ConfigFolder = configPath;
            Directory.CreateDirectory(configPath);
            System.IO.File.WriteAllText(configPath + "/connection.yml", GetConnectionConfig());

            var runtime = await builder.MakeRuntime(CancellationToken.None);

            var configSource = runtime.GetType().GetField("_configSource", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            Assert.IsType<RemoteConfigSource<DummyConfig>>(configSource.GetValue(runtime));

            Directory.Delete(configPath, true);
        }

        [Fact(Timeout = 5000)]
        public async Task TestRuntimeBuilderStatic()
        {
            var builder = new ExtractorRuntimeBuilder<DummyConfig, DummyExtractor>("dotnetutilstest", "utilstest/1");
            builder.ConfigSource = ConfigSourceType.Remote;
            builder.NoConnection = true;
            builder.ExternalConfig = new DummyConfig();
            var services = new ServiceCollection();
            services.AddTestLogging(_output);
            builder.StartupLogger = services.BuildServiceProvider().GetRequiredService<ILogger<RuntimeTest>>();

            var runtime = await builder.MakeRuntime(CancellationToken.None);

            var configSource = runtime.GetType().GetField("_configSource", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            Assert.IsType<StaticConfigSource<DummyConfig>>(configSource.GetValue(runtime));
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

        [Fact(Timeout = 5000)]
        public async Task TestBootstrapSinks()
        {
            // Sink that doesn't do anything with the errors except log them
            var services = new ServiceCollection();
            services.AddTestLogging(_output);
            var sink = new LogIntegrationSink(services.BuildServiceProvider().GetRequiredService<ILogger<RuntimeTest>>());
            await sink.Flush(CancellationToken.None);
            using var e = new ExtractorError(ErrorLevel.error, "test", sink, "details", null, DateTime.UtcNow);
            e.Instant();
            sink.ReportTaskStart("task");
            sink.ReportTaskEnd("task");

            // Sink that only allows reporting errors, for bootstrap during startup.
            services.AddSingleton(TestUtilities.GetMockedHttpClientFactory(mockRequestsAsync).factory.Object);
            services.AddCogniteClient("myApp");
            services.AddConfig(GetConfig(), typeof(ConnectionConfig));
            var provider = services.BuildServiceProvider();
            var sink2 = new BootstrapErrorReporter(
                provider.GetRequiredService<Client>(),
                "test-integration",
                provider.GetRequiredService<ILogger<RuntimeTest>>()
            );
            using var e2 = new ExtractorError(ErrorLevel.error, "test", sink2, "details", null, DateTime.UtcNow);
            e.Instant();

            await sink2.Flush(CancellationToken.None);
            Assert.Single(errors);

            Assert.Equal("test", (string)errors[0].description);
            Assert.Equal("details", (string)errors[0].details);

            Assert.Throws<InvalidOperationException>(() => sink2.ReportTaskStart("task"));
            Assert.Throws<InvalidOperationException>(() => sink2.ReportTaskEnd("task"));
            await Assert.ThrowsAsync<InvalidOperationException>(async () => await sink2.RunPeriodicCheckIn(CancellationToken.None, new StartupRequest()));
        }

        private List<dynamic> taskEvents = new();
        private List<dynamic> errors = new();
        private List<dynamic> startupRequests = new();
        private int _checkInCount;
        private int _startupCount;
        private ConfigRevision _responseRevision;

        private async Task<HttpResponseMessage> mockRequestsAsync(
            HttpRequestMessage message,
            CancellationToken token
        )
        {
            var uri = message.RequestUri.ToString();
            if (uri == "http://example.url/token")
            {
                var reply = "{" + Environment.NewLine +
                       $"  \"token_type\": \"Bearer\",{Environment.NewLine}" +
                       $"  \"expires_in\": 2,{Environment.NewLine}" +
                       $"  \"access_token\": \"token\"{Environment.NewLine}" +
                        "}";
                // Return 200
                var response = new HttpResponseMessage
                {
                    StatusCode = HttpStatusCode.OK,
                    Content = new StringContent(reply)
                };

                return response;
            }
            else if (uri.Contains("/checkin"))
            {
                var content = await message.Content.ReadAsStringAsync(token);
                _output.WriteLine(content);
                var data = JsonConvert.DeserializeObject<dynamic>(content);
                Assert.Equal("test-integration", (string)data.externalId);

                if (data.taskEvents != null)
                {
                    taskEvents.AddRange(data.taskEvents);
                }
                if (data.errors != null)
                {
                    errors.AddRange(data.errors);
                }
                _checkInCount++;
            }
            else if (uri.Contains("/integrations/config"))
            {
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

                var cresBody = System.Text.Json.JsonSerializer.Serialize(_responseRevision, Oryx.Cognite.Common.jsonOptions);
                var cresponse = new HttpResponseMessage
                {
                    StatusCode = HttpStatusCode.OK,
                    Content = new StringContent(cresBody)
                };
                cresponse.Content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
                cresponse.Headers.Add("x-request-id", "1");

                return cresponse;
            }
            else
            {
                Assert.Contains("/startup", uri);
                var content = await message.Content.ReadAsStringAsync(token);
                _output.WriteLine(content);
                var data = JsonConvert.DeserializeObject<dynamic>(content);
                Assert.Equal("test-integration", (string)data.externalId);

                startupRequests.Add(data);

                _startupCount++;
            }

            dynamic resData = new ExpandoObject();
            resData.lastConfigRevision = _responseRevision?.Revision;
            resData.externalId = "test-integration";
            var resBody = JsonConvert.SerializeObject(resData);
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