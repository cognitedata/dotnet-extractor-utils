using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extensions;
using Cognite.Extensions.Unstable;
using Cognite.Extractor.Common;
using Cognite.Extractor.Configuration;
using Cognite.Extractor.Testing;
using Cognite.Extractor.Utils;
using Cognite.Extractor.Utils.Unstable;
using Cognite.Extractor.Utils.Unstable.Configuration;
using CogniteSdk;
using CogniteSdk.Alpha;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Xunit;
using Xunit.Abstractions;

namespace ExtractorUtils.Test.Unit.Unstable
{
    public class CdfMappingTests
    {
        private readonly ITestOutputHelper _output;

        public CdfMappingTests(ITestOutputHelper output)
        {
            _output = output;
        }

        private const string Yaml = @"
cdf-mapping:
  - task-name: task-1
    type: space_routing
    destination: /timeseries/update
    parameters:
      ""abcd.*"": space-1
      ""def.*"": space-2
  - task-name: task-2
    type: location_routing
    parameters:
      nested:
        list:
          - 1
          - true
          - key: value
  - task-name: task-3
    type: custom
    destination: /timeseries/update
    fields:
      externalId: input.id
      space: ""if(regex_match(input.name, 'nor.*'), 'norway', 'sweden')""
  - task-name: task-4
    type: no_parameters
";

        private static readonly ExtractorId extractorId = new ExtractorId
        {
            ExternalId = "cognite-opcua",
            Version = "1.0.0",
        };

        [Fact]
        public void TestParseConfig()
        {
            var config = ConfigurationUtils.ReadString<BaseCogniteConfig>(Yaml);
            var mapping = config.CdfMapping!;
            Assert.Equal(4, mapping.Count);
            Assert.Null(CdfMappingItem.Validate(mapping));

            Assert.Equal("space_routing", mapping[0].Type);
            Assert.Equal("/timeseries/update", mapping[0].Destination);
            Assert.IsType<JsonObject>(mapping[0].Parameters!.Value);
            Assert.Equal(CdfMappingItem.CustomType, mapping[2].Type);
            Assert.Equal(2, mapping[2].Fields!.Count);
            Assert.Null(mapping[3].Parameters);
            Assert.Null(mapping[3].Destination);
            Assert.Null(mapping[3].Fields);

            Assert.Null(ConfigurationUtils.ReadString<BaseCogniteConfig>("nan-replacement: 0").CdfMapping);
        }

        [Theory]
        [InlineData(null, "space_routing", null, false, false, "task-name")]
        [InlineData("  ", "space_routing", null, false, false, "task-name")]
        [InlineData("t", null, null, false, false, "type")]
        [InlineData("t", "custom", null, true, false, "destination")]
        [InlineData("t", "custom", "/dest", false, false, "fields")]
        [InlineData("t", "custom", "/dest", true, true, "parameters is not allowed")]
        [InlineData("t", "custom", "/dest", true, false, null)]
        [InlineData("t", "space_routing", "/dest", false, true, null)]
        [InlineData("t", "space_routing", "", false, false, "destination must not be empty")]
        [InlineData("t", "space_routing", null, true, false, "fields is only allowed")]
        [InlineData("t", "space_routing", null, false, true, null)]
        [InlineData("t", "space_routing", null, false, false, null)]
        public void TestValidate(string? taskName, string? type, string? destination, bool fields, bool parameters, string? error)
        {
            var item = new CdfMappingItem
            {
                TaskName = taskName,
                Type = type,
                Destination = destination,
                Fields = fields ? new Dictionary<string, string> { ["externalId"] = "input.id" } : null,
                Parameters = parameters ? new CdfMappingParameters { Value = new JsonObject { ["a"] = "b" } } : null,
            };
            var result = CdfMappingItem.Validate(new[] { item });
            if (error == null)
            {
                Assert.Null(result);
            }
            else
            {
                Assert.NotNull(result);
                Assert.Contains(error, result, StringComparison.Ordinal);
                Assert.Contains("cognite.cdf-mapping[0]", result, StringComparison.Ordinal);
            }
        }

        [Fact]
        public void TestValidateEdgeCases()
        {
            Assert.Null(CdfMappingItem.Validate(null));
            Assert.Null(CdfMappingItem.Validate(Array.Empty<CdfMappingItem>()));

            var result = CdfMappingItem.Validate(new CdfMappingItem?[] { null });
            Assert.Equal("cognite.cdf-mapping[0] must not be empty", result);

            result = CdfMappingItem.Validate(new[]
            {
                new CdfMappingItem { TaskName = "t", Type = "custom", Destination = "/dest", Fields = new Dictionary<string, string>() }
            });
            Assert.Equal("Missing required field cognite.cdf-mapping[0].fields", result);

            result = CdfMappingItem.Validate(new[]
            {
                new CdfMappingItem { TaskName = "t", Type = "space_routing", Parameters = new CdfMappingParameters { Value = JsonValue.Create("scalar") } }
            });
            Assert.Equal("cognite.cdf-mapping[0].parameters must be a mapping", result);

            // Index is reported for the failing item
            result = CdfMappingItem.Validate(new[]
            {
                new CdfMappingItem { TaskName = "t", Type = "space_routing" },
                new CdfMappingItem { TaskName = "t" }
            });
            Assert.Equal("Missing required field cognite.cdf-mapping[1].type", result);
        }

        [Fact]
        public void TestBuildSetupRequest()
        {
            var config = ConfigurationUtils.ReadString<BaseCogniteConfig>(Yaml);
            var request = CdfWriterExtensions.BuildSetupRequest(config.CdfMapping!, "my-integration", extractorId);

            var options = new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
                DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull,
            };
            var json = JsonSerializer.Serialize(request, options);
            var expected = JsonNode.Parse(@"{
                ""integrationId"": ""my-integration"",
                ""extractorType"": ""cognite-opcua"",
                ""extractorVersion"": ""1.0.0"",
                ""items"": [
                    {
                        ""taskName"": ""task-1"",
                        ""type"": ""space_routing"",
                        ""destination"": ""/timeseries/update"",
                        ""parameters"": { ""abcd.*"": ""space-1"", ""def.*"": ""space-2"" }
                    },
                    {
                        ""taskName"": ""task-2"",
                        ""type"": ""location_routing"",
                        ""parameters"": { ""nested"": { ""list"": [ 1, true, { ""key"": ""value"" } ] } }
                    },
                    {
                        ""taskName"": ""task-3"",
                        ""type"": ""custom"",
                        ""destination"": ""/timeseries/update"",
                        ""fields"": {
                            ""externalId"": ""input.id"",
                            ""space"": ""if(regex_match(input.name, 'nor.*'), 'norway', 'sweden')""
                        }
                    },
                    {
                        ""taskName"": ""task-4"",
                        ""type"": ""no_parameters""
                    }
                ]
            }");
            Assert.True(JsonNode.DeepEquals(expected, JsonNode.Parse(json)), json);
        }

        [Fact]
        public void TestParameterScalarTypes()
        {
            var config = ConfigurationUtils.ReadString<BaseCogniteConfig>(@"
cdf-mapping:
  - task-name: task-1
    type: space_routing
    parameters:
      threshold: 5
      ratio: 1.5
      enabled: true
      quoted: ""5""
      plain: text
      empty: ~
      ~: null-key
");
            var parameters = config.CdfMapping!.Single().Parameters!.Value!.AsObject();
            Assert.Equal(5L, (long?)parameters["threshold"]);
            Assert.Equal(1.5, (double?)parameters["ratio"]);
            Assert.True((bool?)parameters["enabled"]);
            Assert.Equal("5", (string?)parameters["quoted"]);
            Assert.Equal("text", (string?)parameters["plain"]);
            Assert.Null(parameters["empty"]);
            Assert.Equal("null-key", (string?)parameters["~"]);
        }

        private sealed class SetupServer
        {
            public HttpStatusCode Status { get; set; } = HttpStatusCode.OK;
            public string? LastUrl { get; private set; }
            public string? LastBody { get; private set; }
            public int Calls { get; private set; }

            public async Task<HttpResponseMessage> Send(HttpRequestMessage message, CancellationToken token)
            {
                var uri = message.RequestUri!.ToString();
                if (uri == "http://example.url/token")
                {
                    return new HttpResponseMessage
                    {
                        StatusCode = HttpStatusCode.OK,
                        Content = new StringContent("{\"token_type\":\"Bearer\",\"expires_in\":3600,\"access_token\":\"token\"}"),
                    };
                }
                Calls++;
                LastUrl = uri;
                LastBody = await message.Content!.ReadAsStringAsync(token);
                return new HttpResponseMessage
                {
                    StatusCode = Status,
                    Content = new StringContent(Status == HttpStatusCode.OK
                        ? "{}"
                        : "{\"error\":{\"code\":" + (int)Status + ",\"message\":\"test failure\"}}",
                        System.Text.Encoding.UTF8, "application/json"),
                };
            }
        }

        private (ServiceProvider provider, CogniteDestination destination, SetupServer server) GetDestination()
        {
            var config = new ConnectionConfig
            {
                Project = "project",
                BaseUrl = "https://greenfield.cognitedata.com",
                Integration = new IntegrationConfig { ExternalId = "test-integration" },
                Authentication = new ClientCredentialsConfig
                {
                    ClientId = "someId",
                    ClientSecret = "thisIsASecret",
                    Scopes = new Cognite.Common.ListOrSpaceSeparated("https://greenfield.cognitedata.com/.default"),
                    MinTtl = "60s",
                    TokenUrl = "http://example.url/token",
                },
                CdfConnection = new CdfConnectionConfig
                {
                    Retries = new Cognite.Extractor.Utils.Unstable.Configuration.RetryConfig { MaxRetries = 0, MaxBackoff = "10ms" },
                },
            };
            var server = new SetupServer();
            var services = new ServiceCollection();
            services.AddConfig(config, typeof(ConnectionConfig));
            var mocks = TestUtilities.GetMockedHttpClientFactory(server.Send);
            services.AddSingleton(mocks.factory.Object);
            services.AddTestLogging(_output);
            DestinationUtilsUnstable.AddCogniteClient(services, "myApp", null, setLogger: true, setMetrics: true, setHttpClient: true);
            var provider = services.BuildServiceProvider();
            var destination = new CogniteDestination(
                provider.GetRequiredService<Client>(),
                provider.GetRequiredService<ILogger<CogniteDestination>>(),
                new BaseCogniteConfig(),
                "project");
            return (provider, destination, server);
        }

        [Fact]
        public async Task TestSetupConfig()
        {
            var (provider, destination, server) = GetDestination();
            using var p = provider;
            var items = ConfigurationUtils.ReadString<BaseCogniteConfig>(Yaml).CdfMapping!;

            await destination.SetupConfig("my-integration", extractorId, items, CancellationToken.None);

            Assert.Equal(1, server.Calls);
            Assert.Equal("https://greenfield.cognitedata.com/api/v1/projects/project/cdfwriter/setup", server.LastUrl);
            var body = JsonNode.Parse(server.LastBody!)!;
            Assert.Equal("my-integration", (string?)body["integrationId"]);
            Assert.Equal("cognite-opcua", (string?)body["extractorType"]);
            Assert.Equal("1.0.0", (string?)body["extractorVersion"]);
            Assert.Equal(4, body["items"]!.AsArray().Count);
            Assert.Equal("space-1", (string?)body["items"]![0]!["parameters"]!["abcd.*"]);
            Assert.False(body["items"]![3]!.AsObject().ContainsKey("parameters"));
        }

        [Theory]
        [InlineData(HttpStatusCode.BadRequest, typeof(ConfigurationException))]
        [InlineData(HttpStatusCode.Forbidden, typeof(ConfigurationException))]
        [InlineData(HttpStatusCode.NotFound, typeof(ConfigurationException))]
        [InlineData(HttpStatusCode.RequestTimeout, typeof(CogniteUtilsException))]
        [InlineData(HttpStatusCode.TooManyRequests, typeof(CogniteUtilsException))]
        [InlineData(HttpStatusCode.InternalServerError, typeof(CogniteUtilsException))]
        public async Task TestSetupConfigFailures(HttpStatusCode status, Type expected)
        {
            var (provider, destination, server) = GetDestination();
            using var p = provider;
            server.Status = status;
            var items = new[] { new CdfMappingItem { TaskName = "t", Type = "space_routing" } };

            var exc = await Assert.ThrowsAsync(expected, () =>
                destination.SetupConfig("my-integration", extractorId, items, CancellationToken.None));
            Assert.Contains("cdf-mapping", exc.Message, StringComparison.Ordinal);
            Assert.Contains("test failure", exc.Message, StringComparison.Ordinal);
        }

        [Fact]
        public async Task TestSetupConfigArguments()
        {
            var (provider, destination, server) = GetDestination();
            using var p = provider;
            var items = new[] { new CdfMappingItem { TaskName = "t", Type = "space_routing" } };

            await Assert.ThrowsAsync<ArgumentException>(() =>
                destination.SetupConfig("", extractorId, items, CancellationToken.None));
            await Assert.ThrowsAsync<ArgumentException>(() =>
                destination.SetupConfig("i", new ExtractorId { ExternalId = "x" }, items, CancellationToken.None));
            await Assert.ThrowsAsync<ArgumentNullException>(() =>
                destination.SetupConfig("i", extractorId, null!, CancellationToken.None));
            Assert.Equal(0, server.Calls);
        }
    }
}
