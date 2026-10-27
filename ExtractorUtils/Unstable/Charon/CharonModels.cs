using System;
using System.Collections;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Cognite.Extractor.Utils.Unstable.Charon
{
    /// <summary>
    /// Shared JSON options for Charon wire (de)serialization: camelCase, omit nulls when writing.
    /// </summary>
    internal static class CharonJson
    {
        /// <summary>Serializer options used for all Charon request/response bodies.</summary>
        public static readonly JsonSerializerOptions Options = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            PropertyNameCaseInsensitive = true,
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        };
    }

    /// <summary>
    /// Ordered string-to-string map that serializes to a JSON object preserving insertion order.
    /// Used for <c>mapping</c> in Charon setup items, where match order is significant
    /// (first regex match wins in space routing).
    /// </summary>
    [JsonConverter(typeof(OrderedStringMapConverter))]
    public class OrderedStringMap : IEnumerable<KeyValuePair<string, string>>
    {
        private readonly List<KeyValuePair<string, string>> _entries = new List<KeyValuePair<string, string>>();

        /// <summary>Create an empty ordered map.</summary>
        public OrderedStringMap() { }

        /// <summary>Create an ordered map from an ordered sequence of pairs.</summary>
        /// <param name="entries">Pairs in the order they should be serialized.</param>
        public OrderedStringMap(IEnumerable<KeyValuePair<string, string>> entries)
        {
            if (entries == null) throw new ArgumentNullException(nameof(entries));
            foreach (var e in entries) _entries.Add(e);
        }

        /// <summary>Append a key/value pair, preserving order.</summary>
        /// <param name="key">Map key.</param>
        /// <param name="value">Map value.</param>
        public void Add(string key, string value) => _entries.Add(new KeyValuePair<string, string>(key, value));

        /// <summary>Number of entries in the map.</summary>
        public int Count => _entries.Count;

        /// <summary>Enumerate entries in insertion order.</summary>
        public IEnumerator<KeyValuePair<string, string>> GetEnumerator() => _entries.GetEnumerator();

        /// <summary>Enumerate entries in insertion order.</summary>
        IEnumerator IEnumerable.GetEnumerator() => _entries.GetEnumerator();
    }

    /// <summary>
    /// Converter that reads/writes <see cref="OrderedStringMap"/> as a JSON object in list order.
    /// </summary>
    internal class OrderedStringMapConverter : JsonConverter<OrderedStringMap>
    {
        /// <summary>Read a JSON object into an ordered map, preserving property order.</summary>
        public override OrderedStringMap Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            if (reader.TokenType != JsonTokenType.StartObject) throw new JsonException("Expected object for mapping");
            var map = new OrderedStringMap();
            while (reader.Read())
            {
                if (reader.TokenType == JsonTokenType.EndObject) return map;
                if (reader.TokenType != JsonTokenType.PropertyName) throw new JsonException("Expected property name");
                var key = reader.GetString()!;
                reader.Read();
                var value = reader.GetString() ?? string.Empty;
                map.Add(key, value);
            }
            throw new JsonException("Unexpected end of object");
        }

        /// <summary>Write the ordered map as a JSON object, keys in insertion order.</summary>
        public override void Write(Utf8JsonWriter writer, OrderedStringMap value, JsonSerializerOptions options)
        {
            writer.WriteStartObject();
            foreach (var entry in value)
            {
                writer.WriteString(entry.Key, entry.Value);
            }
            writer.WriteEndObject();
        }
    }

    /// <summary>
    /// Request body for <c>POST /cdfwriter/setup</c>. Full replace of the task set for an integration.
    /// </summary>
    public class CharonSetupRequest
    {
        /// <summary>Integration (extraction pipeline) external id.</summary>
        public string IntegrationId { get; set; } = string.Empty;
        /// <summary>Extractor type identifier.</summary>
        public string ExtractorType { get; set; } = string.Empty;
        /// <summary>Extractor version string.</summary>
        public string ExtractorVersion { get; set; } = string.Empty;
        /// <summary>The complete set of tasks for the integration.</summary>
        public List<CharonSetupItem> Items { get; set; } = new List<CharonSetupItem>();
    }

    /// <summary>
    /// A single task registration. <see cref="Type"/> is either <c>space_routing</c> or <c>custom</c>.
    /// Build these with <see cref="CharonTask"/>.
    /// </summary>
    public class CharonSetupItem
    {
        /// <summary>Task name, unique within a setup request.</summary>
        public string TaskName { get; set; } = string.Empty;
        /// <summary>Task type: <c>space_routing</c> or <c>custom</c>.</summary>
        public string Type { get; set; } = string.Empty;
        /// <summary>Destination path, e.g. <c>/timeseries/create</c>.</summary>
        public string Destination { get; set; } = string.Empty;
        /// <summary>Ordered mapping. For space routing: pattern-to-space (order significant).
        /// For custom: destination-key to Kuiper expression.</summary>
        public OrderedStringMap Mapping { get; set; } = new OrderedStringMap();
        /// <summary>Default space for space routing (required there, null for custom tasks).</summary>
        public string? Default { get; set; }
    }

    /// <summary>
    /// Request body for <c>POST /cdfwriter/payload</c>.
    /// </summary>
    public class CharonPayloadRequest
    {
        /// <summary>Integration (extraction pipeline) external id.</summary>
        public string IntegrationId { get; set; } = string.Empty;
        /// <summary>Items grouped by task name.</summary>
        public Dictionary<string, List<JsonElement>> Tasks { get; set; } = new Dictionary<string, List<JsonElement>>();
        /// <summary>Optional fallback destinations for tasks with no registered config.</summary>
        public Dictionary<string, string>? Destinations { get; set; }
    }

    /// <summary>
    /// A single per-item failure in a setup or payload validation error body.
    /// </summary>
    public class CharonItemError
    {
        /// <summary>Index of the failing item within its list.</summary>
        public int ItemIndex { get; set; }
        /// <summary>Name of the task the item belongs to, if known.</summary>
        public string? TaskName { get; set; }
        /// <summary>Human-readable failure reason.</summary>
        public string Reason { get; set; } = string.Empty;
    }

    /// <summary>
    /// Error envelope used by Charon for non-2xx responses.
    /// </summary>
    public class CharonErrorBody
    {
        /// <summary>The error detail.</summary>
        public CharonError? Error { get; set; }
    }

    /// <summary>
    /// Error detail inside a <see cref="CharonErrorBody"/>.
    /// </summary>
    public class CharonError
    {
        /// <summary>HTTP-style error code.</summary>
        public int Code { get; set; }
        /// <summary>Error message.</summary>
        public string Message { get; set; } = string.Empty;
        /// <summary>Per-item errors, when the failure is a validation error.</summary>
        public List<CharonItemError>? Errors { get; set; }
    }

    /// <summary>
    /// Response body for a <c>200</c>/<c>207</c> payload call: per-task downstream result.
    /// </summary>
    public class CharonPayloadResponse
    {
        /// <summary>Per-task downstream results, keyed by task name.</summary>
        public Dictionary<string, CharonTaskResult> Tasks { get; set; } = new Dictionary<string, CharonTaskResult>();
    }

    /// <summary>
    /// Downstream result for a single task in a payload response.
    /// </summary>
    public class CharonTaskResult
    {
        /// <summary>CDF (or transport) status for this task. 504 = downstream timeout, 502 = connect error.</summary>
        public int Status { get; set; }
        /// <summary>CDF response body verbatim.</summary>
        public JsonElement Body { get; set; }
    }
}
