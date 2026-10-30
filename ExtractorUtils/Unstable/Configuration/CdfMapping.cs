using System.Collections.Generic;
using System.Text.Json.Nodes;

namespace Cognite.Extractor.Utils.Unstable.Configuration
{
    /// <summary>
    /// A single entry in the <c>cognite.cdf-mapping</c> config section. The extractor only validates that
    /// the required fields are present. Parameters are sent to the CDF writer with their YAML types.
    /// </summary>
    public class CdfMappingItem
    {
        /// <summary>
        /// Type name for items with a custom destination and field expressions.
        /// </summary>
        public const string CustomType = "custom";

        /// <summary>
        /// Name of the extractor task this mapping applies to.
        /// </summary>
        public string? TaskName { get; set; }
        /// <summary>
        /// Type of mapping. Any value is accepted, <see cref="CustomType"/> requires
        /// <see cref="Destination"/> and <see cref="Fields"/>.
        /// </summary>
        public string? Type { get; set; }
        /// <summary>
        /// Type specific parameters. Must be a mapping, which may contain any nested YAML structure.
        /// Not allowed for custom items.
        /// </summary>
        public CdfMappingParameters? Parameters { get; set; }
        /// <summary>
        /// Destination endpoint, for example <c>/timeseries/update</c>.
        /// Required for custom items, optional for other types.
        /// </summary>
        public string? Destination { get; set; }
        /// <summary>
        /// Map from target field to expression. Required for custom items, not allowed for other types.
        /// </summary>
        public Dictionary<string, string>? Fields { get; set; }

        /// <summary>
        /// Validate a list of mapping items.
        /// </summary>
        /// <param name="items">Items to validate, may be null.</param>
        /// <returns>An error message if the config is invalid, null otherwise.</returns>
        public static string? Validate(IEnumerable<CdfMappingItem?>? items)
        {
            if (items == null) return null;
            int idx = 0;
            foreach (var item in items)
            {
                var path = $"cognite.cdf-mapping[{idx++}]";
                if (item == null) return $"{path} must not be empty";
                if (string.IsNullOrWhiteSpace(item.TaskName)) return $"Missing required field {path}.task-name";
                if (string.IsNullOrWhiteSpace(item.Type)) return $"Missing required field {path}.type";
                if (item.Destination != null && string.IsNullOrWhiteSpace(item.Destination))
                {
                    return $"{path}.destination must not be empty";
                }

                if (item.Type == CustomType)
                {
                    if (item.Destination == null) return $"Missing required field {path}.destination";
                    if (item.Fields == null || item.Fields.Count == 0) return $"Missing required field {path}.fields";
                    if (item.Parameters?.Value != null) return $"{path}.parameters is not allowed when type is {CustomType}";
                }
                else
                {
                    if (item.Fields != null) return $"{path}.fields is only allowed when type is {CustomType}";
                    if (item.Parameters?.Value != null && !(item.Parameters.Value is JsonObject))
                    {
                        return $"{path}.parameters must be a mapping";
                    }
                }
            }
            return null;
        }
    }
}
