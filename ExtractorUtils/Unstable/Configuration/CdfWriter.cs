using System;
using System.Collections.Generic;
using System.Linq;
using CogniteSdk.Alpha;

namespace Cognite.Extractor.Utils.Unstable.Configuration
{
    /// <summary>
    /// The root <c>cdf-writer</c> config section. When present, the extractor registers its tasks,
    /// emitted views, merge mode, mappings and bindings with the CDF writer at startup, and writes
    /// go through the CDF writer instead of directly to CDF. There is no automatic fallback.
    ///
    /// Extractors add a <c>CdfWriter</c> property of this type to their root config.
    /// </summary>
    public class CdfWriterConfig
    {
        /// <summary>Merge mode that writes properties as emitted, without reading.</summary>
        public const string MergeReplace = "replace";
        /// <summary>Merge mode that reads existing nodes and merges properties before writing.</summary>
        public const string MergePreserve = "preserve";
        /// <summary>The mapping kind supported by the CDF writer in iteration 1.</summary>
        public const string KindSpaceRouting = "spaceRouting";

        /// <summary>
        /// Integration-wide upsert merge behaviour, <c>replace</c> or <c>preserve</c>.
        /// If omitted, the CDF writer uses <c>replace</c>.
        /// </summary>
        public string? Merge { get; set; }

        /// <summary>
        /// Mapping definitions, each with a unique external ID.
        /// </summary>
        public List<CdfWriterMappingConfig>? Mappings { get; set; }

        /// <summary>
        /// Bindings attaching integration tasks to mappings.
        /// </summary>
        public List<CdfWriterBindingConfig>? Bindings { get; set; }

        /// <summary>
        /// Validate the structure of the config section. The CDF writer does the full validation
        /// when the config is registered, including binding coverage and view resolution.
        /// </summary>
        /// <param name="config">Config to validate, may be null.</param>
        /// <returns>An error message if invalid, null otherwise.</returns>
        public static string? Validate(CdfWriterConfig? config)
        {
            if (config == null) return null;

            const string path = "cdf-writer";

            if (config.Merge != null && config.Merge != MergeReplace && config.Merge != MergePreserve)
                return $"{path}.merge must be '{MergeReplace}' or '{MergePreserve}'";
            if (config.Mappings == null || config.Mappings.Count == 0)
                return $"Missing required field {path}.mappings";
            if (config.Bindings == null || config.Bindings.Count == 0)
                return $"Missing required field {path}.bindings";

            var mappingIds = new HashSet<string>();
            int mappingIdx = 0;
            foreach (var mapping in config.Mappings)
            {
                var mappingPath = $"{path}.mappings[{mappingIdx++}]";
                if (mapping == null) return $"{mappingPath} must not be empty";
                var error = mapping.Validate(mappingPath);
                if (error != null) return error;
                if (!mappingIds.Add(mapping.ExternalId!))
                    return $"{mappingPath}.external-id '{mapping.ExternalId}' is not unique";
            }

            int bindingIdx = 0;
            foreach (var binding in config.Bindings)
            {
                var bindingPath = $"{path}.bindings[{bindingIdx++}]";
                if (binding == null) return $"{bindingPath} must not be empty";
                if (string.IsNullOrWhiteSpace(binding.MappingId))
                    return $"Missing required field {bindingPath}.mapping-id";
                if (!mappingIds.Contains(binding.MappingId!))
                    return $"{bindingPath}.mapping-id '{binding.MappingId}' does not reference an existing mapping";
                if (binding.TaskIds == null || binding.TaskIds.Count == 0)
                    return $"Missing required field {bindingPath}.task-ids";
                if (binding.TaskIds.Any(string.IsNullOrWhiteSpace))
                    return $"{bindingPath}.task-ids contains an empty value";
            }

            return null;
        }
    }

    /// <summary>
    /// A mapping definition in the <c>cdf-writer</c> config.
    /// </summary>
    public class CdfWriterMappingConfig
    {
        /// <summary>
        /// Identifier of this mapping, unique within the integration.
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
        /// Mapping kind. Only <c>spaceRouting</c> is accepted by the CDF writer for now.
        /// </summary>
        public string? Kind { get; set; }

        /// <summary>
        /// CDF API version to write with, <c>stable</c> or <c>beta</c>. If omitted, the CDF writer uses <c>stable</c>.
        /// Mappings declaring <c>CogniteStateSet</c> require <c>beta</c>.
        /// </summary>
        public string? ApiVersion { get; set; }

        /// <summary>
        /// Map from emitted resource, for example <c>CogniteTimeseries</c>, to its config block.
        /// </summary>
        public Dictionary<string, CdfWriterResourceConfig>? Resources { get; set; }

        internal string? Validate(string path)
        {
            if (string.IsNullOrWhiteSpace(ExternalId))
                return $"Missing required field {path}.external-id";
            if (string.IsNullOrWhiteSpace(Name))
                return $"Missing required field {path}.name";
            if (string.IsNullOrWhiteSpace(Kind))
                return $"Missing required field {path}.kind";
            if (ApiVersion != null && ApiVersion != "stable" && ApiVersion != "beta")
                return $"{path}.api-version must be 'stable' or 'beta'";
            if (Resources == null || Resources.Count == 0)
                return $"Missing required field {path}.resources";
            if (Resources.ContainsKey("CogniteStateSet") && ApiVersion != "beta")
                return $"{path}.api-version must be 'beta' when CogniteStateSet is declared";

            foreach (var kvp in Resources)
            {
                var resourcePath = $"{path}.resources.{kvp.Key}";
                if (kvp.Value == null) return $"{resourcePath} must not be empty";
                var error = kvp.Value.Parameters?.Validate($"{resourcePath}.parameters")
                    ?? (kvp.Value.Parameters == null ? $"Missing required field {resourcePath}.parameters" : null);
                if (error != null) return error;
                if (kvp.Value.Properties != null && string.IsNullOrWhiteSpace(kvp.Value.Properties))
                    return $"{resourcePath}.properties must not be empty";
            }
            return null;
        }
    }

    /// <summary>
    /// Config for one emitted resource of a mapping.
    /// </summary>
    public class CdfWriterResourceConfig
    {
        /// <summary>
        /// Space routing parameters for this resource.
        /// </summary>
        public SpaceRoutingParametersConfig? Parameters { get; set; }

        /// <summary>
        /// Optional Kuiper expression applied to upsert items, returning <c>{ type?, properties }</c>,
        /// or <c>null</c> to skip the item.
        /// </summary>
        public string? Properties { get; set; }

        /// <summary>
        /// Create missing target nodes of direct relations. If omitted, the CDF writer uses <c>true</c>.
        /// </summary>
        public bool? AutoCreateDirectRelations { get; set; }
    }

    /// <summary>
    /// Space routing parameters for one resource.
    /// </summary>
    public class SpaceRoutingParametersConfig
    {
        /// <summary>
        /// Path segments to the value the rules match. <c>[externalId]</c> matches the item's external ID,
        /// any other path is read from the upsert item's properties. Required together with <see cref="Rules"/>.
        /// </summary>
        public List<string>? Property { get; set; }

        /// <summary>
        /// Rules tried in order, the first match wins.
        /// </summary>
        public List<SpaceRoutingRuleConfig>? Rules { get; set; }

        /// <summary>
        /// Space used when no rule matches. Required.
        /// </summary>
        public string? Default { get; set; }

        internal string? Validate(string path)
        {
            if (string.IsNullOrWhiteSpace(Default))
                return $"Missing required field {path}.default";
            if (Rules != null && Property == null)
                return $"{path}.property is required when {path}.rules is set";
            if (Property != null && Rules == null)
                return $"{path}.rules is required when {path}.property is set";
            if (Property != null)
            {
                if (Property.Count == 0) return $"{path}.property must not be empty";
                if (Property.Any(s => string.IsNullOrWhiteSpace(s) || s.Contains('.')))
                    return $"{path}.property segments must be non-empty and must not contain dots";
            }
            if (Rules != null)
            {
                if (Rules.Count == 0) return $"{path}.rules must not be empty";
                var patterns = new HashSet<string>();
                int idx = 0;
                foreach (var rule in Rules)
                {
                    var rulePath = $"{path}.rules[{idx++}]";
                    if (rule == null) return $"{rulePath} must not be empty";
                    if (string.IsNullOrEmpty(rule.Pattern)) return $"Missing required field {rulePath}.pattern";
                    if (string.IsNullOrWhiteSpace(rule.Space)) return $"Missing required field {rulePath}.space";
                    if (!patterns.Add(rule.Pattern!)) return $"{rulePath}.pattern '{rule.Pattern}' is not unique";
                }
            }
            return null;
        }
    }

    /// <summary>
    /// A space routing rule.
    /// </summary>
    public class SpaceRoutingRuleConfig
    {
        /// <summary>
        /// Regex matched against the whole value.
        /// </summary>
        public string? Pattern { get; set; }

        /// <summary>
        /// Space for matching items.
        /// </summary>
        public string? Space { get; set; }
    }

    /// <summary>
    /// A binding attaching integration tasks to a mapping. The binding covers every resource the mapping declares.
    /// </summary>
    public class CdfWriterBindingConfig
    {
        /// <summary>
        /// Integration task names, verbatim. Required, no wildcards.
        /// </summary>
        public List<string>? TaskIds { get; set; }

        /// <summary>
        /// External ID of the mapping this binding references.
        /// </summary>
        public string? MappingId { get; set; }
    }

    /// <summary>
    /// What the extractor emits, declared by the extractor's code rather than its config.
    /// Sent to the CDF writer together with <see cref="CdfWriterConfig"/>.
    /// </summary>
    public class CdfWriterDeclaration
    {
        /// <summary>
        /// Integration tasks that write through the CDF writer, with the resources each one emits
        /// under the active configuration. Task names must match the names reported to the integration.
        /// </summary>
        public IList<CdfWriterTaskDeclaration> Tasks { get; } = new List<CdfWriterTaskDeclaration>();

        /// <summary>
        /// Map from emitted resource to the view its upsert items are shaped for.
        /// Every resource that upserts must have an entry.
        /// </summary>
        public IDictionary<string, CdfWriterViewReference> EmittedViews { get; } = new Dictionary<string, CdfWriterViewReference>();
    }
}
