using System.Collections.Generic;
using System.Text.Json;
using Cognite.Extractor.Utils.Unstable.Charon;
using Xunit;

namespace ExtractorUtils.Test.unit.Unstable
{
    /// <summary>
    /// Pins down the Charon wire contract: camelCase field names, insertion-ordered mapping
    /// serialization (first-match-wins routing depends on it), polymorphic setup items, and
    /// tolerant error-body parsing. These guard against a serialization change silently breaking
    /// the Charon integration.
    /// </summary>
    public class CharonWireContractTests
    {
        private static readonly JsonSerializerOptions CamelCase = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull,
        };

        [Fact]
        public void SetupRequestUsesCamelCaseFieldNames()
        {
            var request = new CharonSetupRequest
            {
                IntegrationId = "int-1",
                ExtractorType = "my-extractor",
                ExtractorVersion = "1.2.3",
                Items = new List<CharonSetupItem>
                {
                    CharonTask.Custom("task-1", CharonDestinations.TimeSeriesCreate,
                        new[] { new KeyValuePair<string, string>("externalId", "input.id") }),
                },
            };

            var json = JsonSerializer.Serialize(request, CamelCase);

            Assert.Contains("\"integrationId\":", json);
            Assert.Contains("\"extractorType\":", json);
            Assert.Contains("\"extractorVersion\":", json);
            Assert.Contains("\"taskName\":", json);
            Assert.Contains("\"destination\":", json);
            Assert.Contains("\"type\":\"custom\"", json);
            // Custom task has no default space -> omitted.
            Assert.DoesNotContain("\"default\":", json);
        }

        [Fact]
        public void SpaceRoutingMappingPreservesInsertionOrder()
        {
            // Order is contractually significant (first regex match wins), so the serializer must
            // emit keys in the order they were supplied, not sorted or hashed.
            var mapping = new[]
            {
                new KeyValuePair<string, string>("zzz.*", "space-z"),
                new KeyValuePair<string, string>("abc.*", "space-a"),
                new KeyValuePair<string, string>("mmm.*", "space-m"),
            };
            var item = CharonTask.SpaceRouting("task-1", CharonDestinations.TimeSeriesCreate, mapping, "space-default");

            var json = JsonSerializer.Serialize(item, CamelCase);

            var iZ = json.IndexOf("zzz.*", System.StringComparison.Ordinal);
            var iA = json.IndexOf("abc.*", System.StringComparison.Ordinal);
            var iM = json.IndexOf("mmm.*", System.StringComparison.Ordinal);
            Assert.True(iZ >= 0 && iA >= 0 && iM >= 0);
            Assert.True(iZ < iA, "zzz.* should serialize before abc.*");
            Assert.True(iA < iM, "abc.* should serialize before mmm.*");
            Assert.Contains("\"type\":\"space_routing\"", json);
            Assert.Contains("\"default\":\"space-default\"", json);
        }

        [Fact]
        public void OrderedStringMapRoundTripsPreservingOrder()
        {
            var map = new OrderedStringMap
            {
                { "b", "1" },
                { "a", "2" },
                { "c", "3" },
            };
            var json = JsonSerializer.Serialize(map, CamelCase);
            Assert.Equal("{\"b\":\"1\",\"a\":\"2\",\"c\":\"3\"}", json);

            var parsed = JsonSerializer.Deserialize<OrderedStringMap>(json, CamelCase)!;
            var keys = new List<string>();
            foreach (var kv in parsed) keys.Add(kv.Key);
            Assert.Equal(new[] { "b", "a", "c" }, keys);
        }

        [Fact]
        public void ErrorBodyParsesPerItemErrors()
        {
            var body = "{\"error\":{\"code\":422,\"message\":\"payload validation failed\"," +
                "\"errors\":[{\"itemIndex\":4,\"taskName\":\"task-1\",\"reason\":\"externalId evaluated to null\"}]}}";

            var parsed = JsonSerializer.Deserialize<CharonErrorBody>(body, CamelCase)!;

            Assert.Equal(422, parsed.Error!.Code);
            Assert.Single(parsed.Error.Errors!);
            Assert.Equal(4, parsed.Error.Errors![0].ItemIndex);
            Assert.Equal("task-1", parsed.Error.Errors[0].TaskName);
            Assert.Equal("externalId evaluated to null", parsed.Error.Errors[0].Reason);
        }

        [Fact]
        public void PayloadResponseParsesMixedStatusesAndIgnoresUnknownFields()
        {
            var body = "{\"tasks\":{" +
                "\"task-1\":{\"status\":200,\"body\":{}}," +
                "\"task-2\":{\"status\":403,\"body\":{\"error\":{}},\"extra\":\"ignored\"}}}";

            var parsed = JsonSerializer.Deserialize<CharonPayloadResponse>(body, CamelCase)!;

            Assert.Equal(2, parsed.Tasks.Count);
            Assert.Equal(200, parsed.Tasks["task-1"].Status);
            Assert.Equal(403, parsed.Tasks["task-2"].Status);
        }
    }
}
