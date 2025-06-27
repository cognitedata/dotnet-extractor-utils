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
using Cognite.Extractor.Common;
using System;
using System.Net.Http;
using System.Net;
using Newtonsoft.Json;
using System.Collections.Generic;
using System.Dynamic;
using System.Net.Http.Headers;
using CogniteSdk.Alpha;
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
integration:
    external-id: test-integration
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

        [Fact(Timeout = 5000)]
        public async Task TestRuntime()
        {
            var builder = new ExtractorRuntimeBuilder<DummyConfig, DummyExtractor>("dotnetutilstest", "utilstest/1");
            builder.NoConnection = true;
            // Use an external config so we don't need a real config source.
            builder.ExternalConfig = new DummyConfig();
            var services = new ServiceCollection();
            services.AddTestLogging(_output);
            builder.StartupLogger = services.BuildServiceProvider().GetRequiredService<ILogger<RuntimeTest>>();
            builder.AddLogger = false;
            builder.ExternalServices = services;

            using var evt = new ManualResetEventSlim(false);

            DummyExtractor extractor = null;
            builder.OnCreateExtractor = (_, ext) =>
            {
                ext.InitAction = (_) =>
                {
                    evt.Set();
                    extractor = ext;
                };
            };

            using var source = new CancellationTokenSource();

            var runtime = await builder.MakeRuntime(source.Token);
            var runTask = runtime.Run();

            Assert.True(await CommonUtils.WaitAsync(evt.WaitHandle, TimeSpan.FromSeconds(5), source.Token));

            Assert.NotNull(extractor);

            source.Cancel();

            await runTask;
        }

        private ExtractorRuntimeBuilder<DummyConfig, DummyExtractor> CreateMockRuntimeBuilder()
        {
            var builder = new ExtractorRuntimeBuilder<DummyConfig, DummyExtractor>("dotnetutilstest", "utilstest/1");
            // builder.SetupHttpClient = false;
            _responseRevision = new ConfigRevision
            {
                Revision = 1,
                Config = "foo: bar"
            };
            var configPath = TestUtils.AlphaNumericPrefix("dotnet_extractor_test") + "_config";
            builder.ConfigFolder = configPath;
            Directory.CreateDirectory(configPath);
            System.IO.File.WriteAllText(configPath + "/connection.yml", GetConnectionConfig());

            var services = new ServiceCollection();
            services.AddTestLogging(_output);
            builder.StartupLogger = services.BuildServiceProvider().GetRequiredService<ILogger<RuntimeTest>>();

            services.AddTransient(p =>
            {
                var mocks = TestUtilities.GetMockedHttpClientFactory(mockRequestsAsync);
                return mocks.factory.Object;
            });

            builder.ExternalServices = services;
            builder.AddLogger = false;

            return builder;
        }

        [Fact(Timeout = 5000)]
        public async Task TestRuntimeRestartNewConfig()
        {
            var builder = CreateMockRuntimeBuilder();

            using var evt = new ManualResetEventSlim(false);

            DummyExtractor extractor = null;
            builder.OnCreateExtractor = (_, ext) =>
            {
                ext.InitAction = (_) =>
                {
                    evt.Set();
                    extractor = ext;
                };
            };

            using var source = new CancellationTokenSource();

            var runtime = await builder.MakeRuntime(source.Token);
            var runTask = runtime.Run();

            Assert.True(await CommonUtils.WaitAsync(evt.WaitHandle, TimeSpan.FromSeconds(5), source.Token));
            Assert.NotNull(extractor);

            // Wait for a startup to be reported.
            await TestUtils.WaitForCondition(() => _startupCount == 1, 5);

            // Update the config revision and the extractor should be restarted.
            var oldExtractor = extractor;
            extractor = null;
            evt.Reset();
            _responseRevision = new ConfigRevision
            {
                Revision = 2,
                Config = "foo: baz"
            };

            // Flush the sink to speed things along
            await oldExtractor.Sink.Flush(source.Token);

            Assert.True(await CommonUtils.WaitAsync(evt.WaitHandle, TimeSpan.FromSeconds(5), source.Token));
            Assert.NotNull(extractor);

            // Wait for another startup to be reported.
            await TestUtils.WaitForCondition(() => _startupCount == 2, 5);

            // Finally, shut down the extractor.
            source.Cancel();

            await runTask;
        }

        [Fact(Timeout = 5000)]
        public async Task TestRuntimeRestartExtractorCrash()
        {
            var builder = CreateMockRuntimeBuilder();

            builder.MaxBackoff = 100;
            builder.BackoffBase = 10;

            using var evt = new ManualResetEventSlim(false);

            DummyExtractor extractor = null;
            int shouldExplode = 2;
            builder.OnCreateExtractor = (_, ext) =>
            {
                ext.InitAction = (_) =>
                {
                    if (shouldExplode-- > 0)
                    {
                        throw new Exception("Boom!");
                    }

                    evt.Set();
                    extractor = ext;
                };
            };

            // Extractor should start even when it crashes on init.
            using var source = new CancellationTokenSource();

            var runtime = await builder.MakeRuntime(source.Token);
            var runTask = runtime.Run();

            Assert.True(await CommonUtils.WaitAsync(evt.WaitHandle, TimeSpan.FromSeconds(5), source.Token));
            Assert.NotNull(extractor);

            // Finally, shut down the extractor.
            source.Cancel();

            await runTask;

            // We should have restarted twice, so we should have two errors.
            Assert.Equal(2, errors.Count);
            Assert.Contains("Failed to initialize extractor: Boom!", (string)errors[0].description);
        }

        [Fact(Timeout = 5000)]
        public async Task TestRuntimeRestartBadConfig()
        {
            var builder = CreateMockRuntimeBuilder();

            builder.MaxBackoff = 100;
            builder.BackoffBase = 10;

            _responseRevision = new ConfigRevision
            {
                Revision = 2,
                Config = "wrong: bar"
            };

            using var evt = new ManualResetEventSlim(false);
            DummyExtractor extractor = null;
            builder.OnCreateExtractor = (_, ext) =>
            {
                ext.InitAction = (_) =>
                {
                    evt.Set();
                    extractor = ext;
                };
            };

            using var source = new CancellationTokenSource();

            var runtime = await builder.MakeRuntime(source.Token);
            _getConfigEvent.Reset();
            var waitTask = CommonUtils.WaitAsync(_getConfigEvent.WaitHandle, TimeSpan.FromSeconds(5), source.Token);
            var runTask = runtime.Run();

            // Wait for a config to be retrieved.
            Assert.True(await waitTask);
            // The extractor should not be created, as the config is bad.
            // Wait a little bit.
            Assert.False(await CommonUtils.WaitAsync(evt.WaitHandle, TimeSpan.FromMilliseconds(200), source.Token));
            Assert.Null(extractor);

            // Set a good config revision.
            _responseRevision = new ConfigRevision
            {
                Revision = 2,
                Config = "foo: bar"
            };

            Assert.True(await CommonUtils.WaitAsync(evt.WaitHandle, TimeSpan.FromSeconds(5), source.Token));
            Assert.NotNull(extractor);

            // Finally, shut down the extractor.
            source.Cancel();

            await runTask;


            // We should have exactly one error reported, since we crashed.
            Assert.Single(errors);
            Assert.Contains("Failed to parse configuration file from CDF", (string)errors[0].description);
        }

        private ConnectionConfig GetConfig()
        {
            return new ConnectionConfig
            {
                Project = "project",
                BaseUrl = "https://greenfield.cognitedata.com",
                Integration = new IntegrationConfig
                {
                    ExternalId = "test-integration"
                },
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
        public async Task TestLogSink()
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
        }

        [Fact(Timeout = 5000)]
        public async Task TestBootstrapSink()
        {
            var services = new ServiceCollection();
            services.AddTestLogging(_output);

            // Sink that only allows reporting errors, for bootstrap during startup.
            services.AddSingleton(TestUtilities.GetMockedHttpClientFactory(mockRequestsAsync).factory.Object);
            services.AddCogniteClient("myApp");
            services.AddConfig(GetConfig(), typeof(ConnectionConfig));
            var provider = services.BuildServiceProvider();
            var sink = new BootstrapErrorReporter(
                provider.GetRequiredService<Client>(),
                "test-integration",
                provider.GetRequiredService<ILogger<RuntimeTest>>()
            );
            using var e = new ExtractorError(ErrorLevel.error, "test", sink, "details", null, DateTime.UtcNow);
            e.Instant();

            await sink.Flush(CancellationToken.None);
            Assert.Single(errors);

            Assert.Equal("test", (string)errors[0].description);
            Assert.Equal("details", (string)errors[0].details);

            Assert.Throws<InvalidOperationException>(() => sink.ReportTaskStart("task"));
            Assert.Throws<InvalidOperationException>(() => sink.ReportTaskEnd("task"));
            await Assert.ThrowsAsync<InvalidOperationException>(async () => await sink.RunPeriodicCheckIn(CancellationToken.None, new StartupRequest()));
        }

        private List<dynamic> taskEvents = new();
        private List<dynamic> errors = new();
        private List<dynamic> startupRequests = new();
        private int _checkInCount;
        private int _startupCount;
        private ConfigRevision _responseRevision;

        private ManualResetEventSlim _getConfigEvent = new ManualResetEventSlim(false);

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

                _getConfigEvent.Set();

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