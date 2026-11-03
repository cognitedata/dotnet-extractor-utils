using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Nodes;

namespace Cognite.Extractor.Utils.Unstable.Configuration
{
    /// <summary>
    /// Root container for the <c>cognite.cdf-mapping</c> config section.
    /// Separates mapping definitions from their bindings to tasks/destinations.
    /// </summary>
    public class CdfMappingConfig
    {
        /// <summary>
        /// External ID of the integration. Required.
        /// </summary>
        public string? IntegrationId { get; set; }

        /// <summary>
        /// Type of extractor (e.g., "opcua"). Required.
        /// </summary>
        public string? ExtractorType { get; set; }

        /// <summary>
        /// Version of the extractor. Required.
        /// </summary>
        public string? ExtractorVersion { get; set; }

        /// <summary>
        /// List of mapping definitions, each with a unique external ID.
        /// </summary>
        public List<CdfMapping>? Mappings { get; set; }

        /// <summary>
        /// List of bindings connecting mappings to tasks and destinations.
        /// </summary>
        public List<CdfMappingBinding>? Bindings { get; set; }

        /// <summary>
        /// Validate the mapping configuration.
        /// </summary>
        /// <param name="config">Config to validate, may be null.</param>
        /// <returns>An error message if invalid, null otherwise.</returns>
        public static string? Validate(CdfMappingConfig? config)
        {
            if (config == null) return null;

            const string path = "cognite.cdf-mapping";

            if (string.IsNullOrWhiteSpace(config.IntegrationId))
                return $"Missing required field {path}.integration-id";
            if (string.IsNullOrWhiteSpace(config.ExtractorType))
                return $"Missing required field {path}.extractor-type";
            if (string.IsNullOrWhiteSpace(config.ExtractorVersion))
                return $"Missing required field {path}.extractor-version";

            if (config.Mappings == null || config.Mappings.Count == 0)
                return $"Missing required field {path}.mappings";
            if (config.Bindings == null || config.Bindings.Count == 0)
                return $"Missing required field {path}.bindings";

            // Validate mappings
            var mappingIds = new HashSet<string>();
            int mappingIdx = 0;
            foreach (var mapping in config.Mappings)
            {
                var mappingPath = $"{path}.mappings[{mappingIdx++}]";
                if (mapping == null) return $"{mappingPath} must not be empty";
                if (string.IsNullOrWhiteSpace(mapping.ExternalId))
                    return $"Missing required field {mappingPath}.external-id";
                if (string.IsNullOrWhiteSpace(mapping.Name))
                    return $"Missing required field {mappingPath}.name";
                if (string.IsNullOrWhiteSpace(mapping.Type))
                    return $"Missing required field {mappingPath}.type";

                if (!mappingIds.Add(mapping.ExternalId))
                    return $"{mappingPath}.external-id '{mapping.ExternalId}' is not unique";

                if (mapping.Parameters?.Value != null && !(mapping.Parameters.Value is JsonObject))
                    return $"{mappingPath}.parameters must be a mapping";
            }

            // Validate bindings
            int bindingIdx = 0;
            foreach (var binding in config.Bindings)
            {
                var bindingPath = $"{path}.bindings[{bindingIdx++}]";
                if (binding == null) return $"{bindingPath} must not be empty";
                if (string.IsNullOrWhiteSpace(binding.MappingId))
                    return $"Missing required field {bindingPath}.mapping-id";
                if (binding.Destinations == null || binding.Destinations.Count == 0)
                    return $"Missing required field {bindingPath}.destinations";

                if (!mappingIds.Contains(binding.MappingId))
                    return $"{bindingPath}.mapping-id '{binding.MappingId}' does not reference an existing mapping";

                foreach (var dest in binding.Destinations)
                {
                    if (string.IsNullOrWhiteSpace(dest))
                        return $"{bindingPath}.destinations contains an empty value";
                }
            }

            return null;
        }
    }

    /// <summary>
    /// A single mapping definition in the cdf-mapping config.
    /// </summary>
    public class CdfMapping
    {
        /// <summary>
        /// Unique identifier for this mapping.
        /// </summary>
        public string? ExternalId { get; set; }

        /// <summary>
        /// Human-readable name of this mapping.
        /// </summary>
        public string? Name { get; set; }

        /// <summary>
        /// Optional description of this mapping.
        /// </summary>
        public string? Description { get; set; }

        /// <summary>
        /// Type of mapping (e.g., "space_routing", "custom", "location_routing").
        /// </summary>
        public string? Type { get; set; }

        /// <summary>
        /// Type-specific parameters for this mapping. Must be a mapping (JsonObject) if set.
        /// </summary>
        public CdfMappingParameters? Parameters { get; set; }
    }

    /// <summary>
    /// A binding connecting a mapping to specific tasks and destinations.
    /// </summary>
    public class CdfMappingBinding
    {
        /// <summary>
        /// Task IDs this binding applies to. If null or empty, applies to all tasks.
        /// </summary>
        public List<string>? TaskIds { get; set; }

        /// <summary>
        /// Destination endpoints (e.g., "/timeseries/datapoints", "/timeseries/create").
        /// </summary>
        public List<string>? Destinations { get; set; }

        /// <summary>
        /// External ID of the mapping this binding references.
        /// </summary>
        public string? MappingId { get; set; }
    }
}
