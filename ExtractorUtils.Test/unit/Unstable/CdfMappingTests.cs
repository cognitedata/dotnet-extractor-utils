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
  integration-id: my-integration
  extractor-type: cognite-opcua
  extractor-version: 1.0.0
  mappings:
    - external-id: map-1
      name: ""Space Routing""
      type: space_routing
      parameters:
        ""abcd.*"": space-1
        ""def.*"": space-2
    - external-id: map-2
      name: ""Location Routing""
      type: location_routing
      parameters:
        nested:
          list:
            - 1
            - true
            - key: value
    - external-id: map-3
      name: ""Custom Mapping""
      type: custom
    - external-id: map-4
      name: ""No Parameters""
      type: no_parameters
  bindings:
    - task-ids: [task-1]
      destinations: [/timeseries/update]
      mapping-id: map-1
    - task-ids: [task-2]
      destinations: [/timeseries/update]
      mapping-id: map-2
    - task-ids: [task-3]
      destinations: [/timeseries/update]
      mapping-id: map-3
    - destinations: [/timeseries/update]
      mapping-id: map-4
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
            Assert.NotNull(mapping);
            Assert.Equal("my-integration", mapping.IntegrationId);
            Assert.Equal("cognite-opcua", mapping.ExtractorType);
            Assert.Equal("1.0.0", mapping.ExtractorVersion);
            Assert.Equal(4, mapping.Mappings!.Count);
            Assert.Equal(4, mapping.Bindings!.Count);
            Assert.Null(CdfMappingConfig.Validate(mapping));

            Assert.Equal("space_routing", mapping.Mappings[0].Type);
            Assert.IsType<JsonObject>(mapping.Mappings[0].Parameters!.Value);
            Assert.Equal("custom", mapping.Mappings[2].Type);
            Assert.Null(mapping.Mappings[3].Parameters);

            Assert.Null(ConfigurationUtils.ReadString<BaseCogniteConfig>("nan-replacement: 0").CdfMapping);
        }

        [Theory]
        [InlineData(null, null, null, "integration-id")]
        [InlineData("i", null, null, "extractor-type")]
        [InlineData("i", "t", null, "extractor-version")]
        [InlineData("i", "t", "v", "mappings")]
        public void TestValidate(string? integrationId, string? extractorType, string? extractorVersion, string? expectedError)
        {
            var hasAllRequiredFields = !string.IsNullOrEmpty(integrationId) && !string.IsNullOrEmpty(extractorType) && !string.IsNullOrEmpty(extractorVersion);

            var config = new CdfMappingConfig
            {
                IntegrationId = integrationId,
                ExtractorType = extractorType,
                ExtractorVersion = extractorVersion,
                Mappings = hasAllRequiredFields ? null : new List<CdfMapping> { new CdfMapping { ExternalId = "m1", Name = "M1", Type = "t" } },
                Bindings = hasAllRequiredFields ? null : new List<CdfMappingBinding> { new CdfMappingBinding { MappingId = "m1", Destinations = new List<string> { "/d" } } },
            };
            var result = CdfMappingConfig.Validate(config);
            Assert.NotNull(result);
            Assert.Contains(expectedError, result, StringComparison.Ordinal);
        }

        [Fact]
        public void TestValidateSucceeds()
        {
            var config = new CdfMappingConfig
            {
                IntegrationId = "i",
                ExtractorType = "t",
                ExtractorVersion = "v",
                Mappings = new List<CdfMapping> { new CdfMapping { ExternalId = "m1", Name = "M1", Type = "t" } },
                Bindings = new List<CdfMappingBinding> { new CdfMappingBinding { MappingId = "m1", Destinations = new List<string> { "/d" } } },
            };
            var result = CdfMappingConfig.Validate(config);
            Assert.Null(result);
        }

        [Fact]
        public void TestValidateEdgeCases()
        {
            Assert.Null(CdfMappingConfig.Validate(null));

            // Duplicate mapping IDs
            var result = CdfMappingConfig.Validate(new CdfMappingConfig
            {
                IntegrationId = "i",
                ExtractorType = "t",
                ExtractorVersion = "v",
                Mappings = new List<CdfMapping>
                {
                    new CdfMapping { ExternalId = "m1", Name = "M1", Type = "t" },
                    new CdfMapping { ExternalId = "m1", Name = "M2", Type = "t" }
                },
                Bindings = new List<CdfMappingBinding>
                {
                    new CdfMappingBinding { MappingId = "m1", Destinations = new List<string> { "/d" } }
                }
            });
            Assert.Contains("not unique", result!, StringComparison.Ordinal);

            // Dangling binding reference
            result = CdfMappingConfig.Validate(new CdfMappingConfig
            {
                IntegrationId = "i",
                ExtractorType = "t",
                ExtractorVersion = "v",
                Mappings = new List<CdfMapping>
                {
                    new CdfMapping { ExternalId = "m1", Name = "M1", Type = "t" }
                },
                Bindings = new List<CdfMappingBinding>
                {
                    new CdfMappingBinding { MappingId = "m2", Destinations = new List<string> { "/d" } }
                }
            });
            Assert.Contains("does not reference an existing mapping", result!, StringComparison.Ordinal);

            // Empty destinations
            result = CdfMappingConfig.Validate(new CdfMappingConfig
            {
                IntegrationId = "i",
                ExtractorType = "t",
                ExtractorVersion = "v",
                Mappings = new List<CdfMapping>
                {
                    new CdfMapping { ExternalId = "m1", Name = "M1", Type = "t" }
                },
                Bindings = new List<CdfMappingBinding>
                {
                    new CdfMappingBinding { MappingId = "m1", Destinations = new List<string> { "" } }
                }
            });
            Assert.Contains("contains an empty value", result!, StringComparison.Ordinal);
        }

        [Fact]
        public void TestBuildSetupRequest()
        {
            var config = ConfigurationUtils.ReadString<BaseCogniteConfig>(Yaml);
            var request = CdfWriterExtensions.BuildSetupRequest(config.CdfMapping!);

            Assert.Equal("my-integration", request.IntegrationId);
            Assert.Equal("cognite-opcua", request.ExtractorType);
            Assert.Equal("1.0.0", request.ExtractorVersion);
            Assert.NotNull(request.Items);
            var items = request.Items.ToList();
            Assert.Equal(4, items.Count);

            // Verify each item is derived from the correct mapping and binding
            Assert.Equal("space_routing", items[0].Type);
            Assert.Equal("/timeseries/update", items[0].Destination);

            Assert.Equal("location_routing", items[1].Type);
            Assert.Equal("/timeseries/update", items[1].Destination);

            Assert.Equal("custom", items[2].Type);
            Assert.Equal("/timeseries/update", items[2].Destination);

            Assert.Equal("no_parameters", items[3].Type);
            Assert.Equal("/timeseries/update", items[3].Destination);
        }

        [Fact]
        public void TestParameterScalarTypes()
        {
            var config = ConfigurationUtils.ReadString<BaseCogniteConfig>(@"
cdf-mapping:
  integration-id: test
  extractor-type: test
  extractor-version: 1.0
  mappings:
    - external-id: m1
      name: Test
      type: space_routing
      parameters:
        threshold: 5
        ratio: 1.5
        enabled: true
        quoted: ""5""
        plain: text
        empty: ~
        ~: null-key
  bindings:
    - mapping-id: m1
      destinations: [/test]
");
            var parameters = config.CdfMapping!.Mappings!.Single().Parameters!.Value!.AsObject();
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
            var config = ConfigurationUtils.ReadString<BaseCogniteConfig>(Yaml).CdfMapping!;

            await destination.SetupConfig(config, CancellationToken.None);

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
            var config = new CdfMappingConfig
            {
                IntegrationId = "i",
                ExtractorType = "t",
                ExtractorVersion = "v",
                Mappings = new List<CdfMapping> { new CdfMapping { ExternalId = "m", Name = "M", Type = "space_routing" } },
                Bindings = new List<CdfMappingBinding> { new CdfMappingBinding { MappingId = "m", Destinations = new List<string> { "/d" } } }
            };

            var exc = await Assert.ThrowsAsync(expected, () =>
                destination.SetupConfig(config, CancellationToken.None));
            Assert.Contains("cdf-mapping", exc.Message, StringComparison.Ordinal);
            Assert.Contains("test failure", exc.Message, StringComparison.Ordinal);
        }

        [Fact]
        public async Task TestSetupConfigArguments()
        {
            var (provider, destination, server) = GetDestination();
            using var p = provider;

            var invalidConfig1 = new CdfMappingConfig
            {
                IntegrationId = "",
                ExtractorType = "t",
                ExtractorVersion = "v",
                Mappings = new List<CdfMapping> { new CdfMapping { ExternalId = "m", Name = "M", Type = "t" } },
                Bindings = new List<CdfMappingBinding> { new CdfMappingBinding { MappingId = "m", Destinations = new List<string> { "/d" } } }
            };

            await Assert.ThrowsAsync<ArgumentException>(() =>
                destination.SetupConfig(invalidConfig1, CancellationToken.None));

            await Assert.ThrowsAsync<ArgumentNullException>(() =>
                destination.SetupConfig(null!, CancellationToken.None));

            Assert.Equal(0, server.Calls);
        }
    }
}
