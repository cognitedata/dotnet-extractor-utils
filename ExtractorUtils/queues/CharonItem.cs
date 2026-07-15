using System.Text.Json.Serialization;

namespace Cognite.Extractor.Utils
{
    /// <summary>
    /// A single item in a Charon <c>/insert_payload</c> request body.
    /// Field names are camelCase to match Pluto's <c>#[serde(rename_all = "camelCase")]</c>.
    /// </summary>
    public class CharonItem
    {
        /// <summary>
        /// Destination item type discriminator. Must be <c>"datapoint"</c>.
        /// </summary>
        [JsonPropertyName("type")]
        public string Type { get; set; } = "datapoint";

        /// <summary>
        /// External ID of the CDF time series this datapoint belongs to.
        /// Corresponds to <c>UADataPoint.Id</c> in the OPC-UA extractor.
        /// </summary>
        [JsonPropertyName("externalId")]
        public string ExternalId { get; set; } = "";

        /// <summary>
        /// Timestamp in milliseconds since Unix epoch.
        /// Corresponds to <c>UADataPoint.Timestamp</c> (the OPC-UA SourceTimestamp).
        /// </summary>
        [JsonPropertyName("timestamp")]
        public long Timestamp { get; set; }

        /// <summary>
        /// Numeric datapoint value. Corresponds to <c>UADataPoint.DoubleValue</c>.
        /// </summary>
        [JsonPropertyName("value")]
        public double Value { get; set; }
    }
}
