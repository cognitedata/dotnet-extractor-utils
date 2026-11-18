#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
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
    public class CdfWriterTests
    {
        private readonly ITestOutputHelper _output;

        public CdfWriterTests(ITestOutputHelper output)
        {
            _output = output;
        }

        private class RootConfig
        {
            public CdfWriterConfig? CdfWriter { get; set; }
        }

        private const string Yaml = @"
cdf-writer:
  merge: preserve
  mappings:
    - external-id: fba8cb22-e44a-4ee1-9282-57ce84f23d7c
      name: Norway time series
      kind: spaceRouting
      description: Route Norwegian sites to their own spaces
      resources:
        CogniteTimeseries:
          parameters:
            property: [externalId]
            rules:
              - pattern: ""nor-oslo-.*""
                space: sp-oslo
              - pattern: ""nor-.*""
                space: sp-norway
            default: sp-other
          properties: |
            { ""properties"": input.properties }
          auto-create-direct-relations: false
        CogniteSourceSystem:
          parameters:
            default: sp-norway
    - external-id: 7e3b9a52-1c4d-4f8a-9b6e-0d2c5f8a1e93
      name: Norway time series (state series)
      kind: spaceRouting
      api-version: beta
      resources:
        CogniteTimeseries:
          parameters:
            property: [extractedData, site]
            rules:
              - pattern: oslo
                space: sp-oslo
            default: sp-other
        CogniteStateSet:
          parameters:
            default: sp-norway
  bindings:
    - task-ids: [""Browse OPC-UA Node Hierarchy"", ""Stream data to CDF""]
      mapping-id: fba8cb22-e44a-4ee1-9282-57ce84f23d7c
";

        private static readonly ExtractorId extractorId = new ExtractorId
        {
            ExternalId = "cognite-opcua",
            Version = "1.0.0",
        };

        private static CdfWriterDeclaration Declaration()
        {
            var declaration = new CdfWriterDeclaration();
            declaration.Tasks.Add(new CdfWriterTaskDeclaration
            {
                Name = "Browse OPC-UA Node Hierarchy",
                Resources = new[] { "CogniteTimeseries", "CogniteSourceSystem" },
            });
            declaration.Tasks.Add(new CdfWriterTaskDeclaration
            {
                Name = "Stream data to CDF",
                Resources = new[] { "CogniteTimeseries", "CogniteSourceSystem" },
            });
            declaration.EmittedViews["CogniteTimeseries"] = new CdfWriterViewReference
            {
                Space = "cdf_extraction_extensions",
                ExternalId = "CogniteExtractorTimeSeries",
                Version = "v1",
            };
            declaration.EmittedViews["CogniteSourceSystem"] = new CdfWriterViewReference
            {
                Space = "cdf_cdm",
                ExternalId = "CogniteSourceSystem",
                Version = "v1",
            };
            return declaration;
        }

        private static CdfWriterConfig Parse(string yaml = Yaml)
            => ConfigurationUtils.ReadString<RootConfig>(yaml).CdfWriter!;

        [Fact]
        public void TestParseConfig()
        {
            var config = Parse();
            Assert.Null(CdfWriterConfig.Validate(config));
            Assert.Equal("preserve", config.Merge);
            Assert.Equal(2, config.Mappings!.Count);
            Assert.Single(config.Bindings!);

            var mapping = config.Mappings[0];
            Assert.Equal("spaceRouting", mapping.Kind);
            Assert.Null(mapping.ApiVersion);
            // Resource keys are kept verbatim, not converted by the naming convention.
            Assert.Equal(new[] { "CogniteTimeseries", "CogniteSourceSystem" }, mapping.Resources!.Keys);
            var ts = mapping.Resources["CogniteTimeseries"];
            Assert.Equal(new[] { "externalId" }, ts.Parameters!.Property);
            Assert.Equal(2, ts.Parameters.Rules!.Count);
            Assert.Equal("nor-oslo-.*", ts.Parameters.Rules[0].Pattern);
            Assert.Equal("sp-oslo", ts.Parameters.Rules[0].Space);
            Assert.Equal("sp-other", ts.Parameters.Default);
            Assert.Contains("input.properties", ts.Properties, StringComparison.Ordinal);
            Assert.False(ts.AutoCreateDirectRelations);
            var source = mapping.Resources["CogniteSourceSystem"];
            Assert.Null(source.Parameters!.Property);
            Assert.Null(source.Parameters.Rules);
            Assert.Null(source.AutoCreateDirectRelations);

            Assert.Equal("beta", config.Mappings[1].ApiVersion);
            Assert.Equal(new[] { "extractedData", "site" }, config.Mappings[1].Resources!["CogniteTimeseries"].Parameters!.Property);

            Assert.Equal(new[] { "Browse OPC-UA Node Hierarchy", "Stream data to CDF" }, config.Bindings![0].TaskIds);

            Assert.Null(ConfigurationUtils.ReadString<RootConfig>("{}").CdfWriter);
        }

        private static CdfWriterConfig Valid() => new CdfWriterConfig
        {
            Mappings = new List<CdfWriterMappingConfig>
            {
                new CdfWriterMappingConfig
                {
                    ExternalId = "m1",
                    Name = "M1",
                    Kind = "spaceRouting",
                    Resources = new Dictionary<string, CdfWriterResourceConfig>
                    {
                        ["CogniteTimeseries"] = new CdfWriterResourceConfig
                        {
                            Parameters = new SpaceRoutingParametersConfig
                            {
                                Property = new List<string> { "externalId" },
                                Rules = new List<SpaceRoutingRuleConfig> { new SpaceRoutingRuleConfig { Pattern = "a.*", Space = "sp-a" } },
                                Default = "sp",
                            },
                        },
                    },
                },
            },
            Bindings = new List<CdfWriterBindingConfig>
            {
                new CdfWriterBindingConfig { MappingId = "m1", TaskIds = new List<string> { "task" } },
            },
        };

        public static IEnumerable<object[]> InvalidConfigs()
        {
            (Action<CdfWriterConfig> change, string error)[] cases =
            {
                (c => c.Merge = "overwrite", "cdf-writer.merge must be"),
                (c => c.Mappings = null, "Missing required field cdf-writer.mappings"),
                (c => c.Bindings = new List<CdfWriterBindingConfig>(), "Missing required field cdf-writer.bindings"),
                (c => c.Mappings![0].ExternalId = null, "cdf-writer.mappings[0].external-id"),
                (c => c.Mappings![0].Name = "", "cdf-writer.mappings[0].name"),
                (c => c.Mappings![0].Kind = null, "cdf-writer.mappings[0].kind"),
                (c => c.Mappings![0].ApiVersion = "v2", "cdf-writer.mappings[0].api-version must be"),
                (c => c.Mappings![0].Resources = null, "cdf-writer.mappings[0].resources"),
                (c => c.Mappings![0].Resources!["CogniteStateSet"] = new CdfWriterResourceConfig { Parameters = new SpaceRoutingParametersConfig { Default = "sp" } },
                    "must be 'beta' when CogniteStateSet"),
                (c => c.Mappings!.Add(c.Mappings[0]), "external-id 'm1' is not unique"),
                (c => c.Mappings![0].Resources!["CogniteTimeseries"].Parameters = null, "resources.CogniteTimeseries.parameters"),
                (c => c.Mappings![0].Resources!["CogniteTimeseries"].Properties = " ", "resources.CogniteTimeseries.properties must not be empty"),
                (c => c.Mappings![0].Resources!["CogniteTimeseries"].Parameters!.Default = null, "parameters.default"),
                (c => c.Mappings![0].Resources!["CogniteTimeseries"].Parameters!.Property = null, "property is required when"),
                (c => c.Mappings![0].Resources!["CogniteTimeseries"].Parameters!.Rules = null, "rules is required when"),
                (c => c.Mappings![0].Resources!["CogniteTimeseries"].Parameters!.Property = new List<string>(), "property must not be empty"),
                (c => c.Mappings![0].Resources!["CogniteTimeseries"].Parameters!.Property = new List<string> { "extractedData.site" }, "must not contain dots"),
                (c => c.Mappings![0].Resources!["CogniteTimeseries"].Parameters!.Rules = new List<SpaceRoutingRuleConfig>(), "rules must not be empty"),
                (c => c.Mappings![0].Resources!["CogniteTimeseries"].Parameters!.Rules![0].Space = null, "rules[0].space"),
                (c => c.Mappings![0].Resources!["CogniteTimeseries"].Parameters!.Rules!.Add(new SpaceRoutingRuleConfig { Pattern = "a.*", Space = "x" }),
                    "rules[1].pattern 'a.*' is not unique"),
                (c => c.Bindings![0].MappingId = "m2", "does not reference an existing mapping"),
                (c => c.Bindings![0].TaskIds = null, "cdf-writer.bindings[0].task-ids"),
                (c => c.Bindings![0].TaskIds = new List<string> { "" }, "task-ids contains an empty value"),
            };
            return cases.Select(c => new object[] { c.change, c.error });
        }

        [Theory]
        [MemberData(nameof(InvalidConfigs))]
        public void TestValidateInvalid(Action<CdfWriterConfig> change, string expected)
        {
            var config = Valid();
            Assert.Null(CdfWriterConfig.Validate(config));
            change(config);
            var error = CdfWriterConfig.Validate(config);
            Assert.NotNull(error);
            Assert.Contains(expected, error, StringComparison.Ordinal);
        }

        [Fact]
        public void TestValidateNull()
        {
            Assert.Null(CdfWriterConfig.Validate(null));
        }

        [Fact]
        public void TestBuildSetupRequest()
        {
            var request = CdfWriterExtensions.BuildSetupRequest(Parse(), Declaration(), "my-integration", extractorId);

            Assert.Equal("my-integration", request.IntegrationId);
            Assert.Equal("cognite-opcua", request.ExtractorType);
            Assert.Equal("1.0.0", request.ExtractorVersion);
            Assert.Equal("preserve", request.Merge);
            Assert.Equal(2, request.Tasks!.Count());
            Assert.Equal("CogniteExtractorTimeSeries", request.EmittedViews!["CogniteTimeseries"].ExternalId);

            var mappings = request.Mappings!.ToList();
            Assert.Equal(2, mappings.Count);
            var ts = mappings[0].Resources!["CogniteTimeseries"];
            Assert.Equal(new[] { "externalId" }, ts.Parameters!.Property);
            Assert.Equal("sp-norway", ts.Parameters.Rules!.ElementAt(1).Space);
            Assert.False(ts.AutoCreateDirectRelations);
            Assert.Equal("beta", mappings[1].ApiVersion);

            var binding = Assert.Single(request.Bindings!);
            Assert.Equal("fba8cb22-e44a-4ee1-9282-57ce84f23d7c", binding.MappingId);
            Assert.Equal(2, binding.TaskIds!.Count());
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
                        ? "{\"integrationId\":\"my-integration\",\"version\":3}"
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
        public async Task TestSetupCdfWriter()
        {
            var (provider, destination, server) = GetDestination();
            using var p = provider;

            var result = await destination.SetupCdfWriter("my-integration", extractorId, Parse(), Declaration(), CancellationToken.None);

            Assert.Equal(3, result.Version);
            Assert.Equal(1, server.Calls);
            Assert.Equal("https://greenfield.cognitedata.com/api/v1/projects/project/cdfwriter/setup", server.LastUrl);
            var body = JsonNode.Parse(server.LastBody!)!;
            var expected = JsonNode.Parse(@"{
                ""integrationId"": ""my-integration"",
                ""extractorType"": ""cognite-opcua"",
                ""extractorVersion"": ""1.0.0"",
                ""merge"": ""preserve"",
                ""tasks"": [
                    { ""name"": ""Browse OPC-UA Node Hierarchy"", ""resources"": [""CogniteTimeseries"", ""CogniteSourceSystem""] },
                    { ""name"": ""Stream data to CDF"", ""resources"": [""CogniteTimeseries"", ""CogniteSourceSystem""] }
                ],
                ""emittedViews"": {
                    ""CogniteTimeseries"": { ""space"": ""cdf_extraction_extensions"", ""externalId"": ""CogniteExtractorTimeSeries"", ""version"": ""v1"" },
                    ""CogniteSourceSystem"": { ""space"": ""cdf_cdm"", ""externalId"": ""CogniteSourceSystem"", ""version"": ""v1"" }
                },
                ""mappings"": [
                    {
                        ""externalId"": ""fba8cb22-e44a-4ee1-9282-57ce84f23d7c"",
                        ""name"": ""Norway time series"",
                        ""description"": ""Route Norwegian sites to their own spaces"",
                        ""kind"": ""spaceRouting"",
                        ""resources"": {
                            ""CogniteTimeseries"": {
                                ""parameters"": {
                                    ""property"": [""externalId""],
                                    ""rules"": [
                                        { ""pattern"": ""nor-oslo-.*"", ""space"": ""sp-oslo"" },
                                        { ""pattern"": ""nor-.*"", ""space"": ""sp-norway"" }
                                    ],
                                    ""default"": ""sp-other""
                                },
                                ""properties"": ""{ \""properties\"": input.properties }\n"",
                                ""autoCreateDirectRelations"": false
                            },
                            ""CogniteSourceSystem"": { ""parameters"": { ""default"": ""sp-norway"" } }
                        }
                    },
                    {
                        ""externalId"": ""7e3b9a52-1c4d-4f8a-9b6e-0d2c5f8a1e93"",
                        ""name"": ""Norway time series (state series)"",
                        ""kind"": ""spaceRouting"",
                        ""apiVersion"": ""beta"",
                        ""resources"": {
                            ""CogniteTimeseries"": {
                                ""parameters"": {
                                    ""property"": [""extractedData"", ""site""],
                                    ""rules"": [{ ""pattern"": ""oslo"", ""space"": ""sp-oslo"" }],
                                    ""default"": ""sp-other""
                                }
                            },
                            ""CogniteStateSet"": { ""parameters"": { ""default"": ""sp-norway"" } }
                        }
                    }
                ],
                ""bindings"": [
                    { ""taskIds"": [""Browse OPC-UA Node Hierarchy"", ""Stream data to CDF""], ""mappingId"": ""fba8cb22-e44a-4ee1-9282-57ce84f23d7c"" }
                ]
            }");
            Assert.True(JsonNode.DeepEquals(expected, body), body.ToJsonString());
        }

        [Theory]
        [InlineData(HttpStatusCode.BadRequest, typeof(ConfigurationException))]
        [InlineData(HttpStatusCode.Forbidden, typeof(ConfigurationException))]
        [InlineData(HttpStatusCode.UnprocessableEntity, typeof(ConfigurationException))]
        [InlineData(HttpStatusCode.RequestTimeout, typeof(CogniteUtilsException))]
        [InlineData(HttpStatusCode.TooManyRequests, typeof(CogniteUtilsException))]
        [InlineData(HttpStatusCode.InternalServerError, typeof(CogniteUtilsException))]
        public async Task TestSetupCdfWriterFailures(HttpStatusCode status, Type expected)
        {
            var (provider, destination, server) = GetDestination();
            using var p = provider;
            server.Status = status;

            var exc = await Assert.ThrowsAsync(expected, () =>
                destination.SetupCdfWriter("my-integration", extractorId, Valid(), Declaration(), CancellationToken.None));
            Assert.Contains("cdf-writer", exc.Message, StringComparison.Ordinal);
            Assert.Contains("test failure", exc.Message, StringComparison.Ordinal);
        }

        [Fact]
        public async Task TestSetupCdfWriterArguments()
        {
            var (provider, destination, server) = GetDestination();
            using var p = provider;

            await Assert.ThrowsAsync<ArgumentException>(() =>
                destination.SetupCdfWriter("", extractorId, Valid(), Declaration(), CancellationToken.None));
            await Assert.ThrowsAsync<ArgumentException>(() =>
                destination.SetupCdfWriter("i", new ExtractorId { ExternalId = "x" }, Valid(), Declaration(), CancellationToken.None));
            await Assert.ThrowsAsync<ArgumentNullException>(() =>
                destination.SetupCdfWriter("i", extractorId, null!, Declaration(), CancellationToken.None));
            await Assert.ThrowsAsync<ArgumentNullException>(() =>
                destination.SetupCdfWriter("i", extractorId, Valid(), null!, CancellationToken.None));
            Assert.Equal(0, server.Calls);
        }
    }
}
