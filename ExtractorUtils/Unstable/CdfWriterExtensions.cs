using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extensions;
using Cognite.Extractor.Common;
using Cognite.Extractor.Utils.Unstable.Configuration;
using CogniteSdk;
using CogniteSdk.Alpha;

namespace Cognite.Extractor.Utils.Unstable
{
    /// <summary>
    /// Extensions for configuring the CDF writer from extractor config.
    /// </summary>
    public static class CdfWriterExtensions
    {
        /// <summary>
        /// Send the <c>cognite.cdf-mapping</c> config section to the CDF writer, replacing any
        /// existing mapping for the integration. Config should be validated with
        /// <see cref="CdfMappingConfig.Validate"/> before calling this.
        /// </summary>
        /// <param name="destination">Destination to send through</param>
        /// <param name="config">Mapping configuration</param>
        /// <param name="token">Cancellation token</param>
        /// <exception cref="ConfigurationException">If CDF rejects the mapping. Retrying with the same config will not help.</exception>
        /// <exception cref="CogniteUtilsException">If the request fails for other reasons, such as rate limiting or server errors.</exception>
        public static async Task SetupConfig(
            this CogniteDestination destination,
            CdfMappingConfig config,
            CancellationToken token)
        {
            if (destination == null) throw new ArgumentNullException(nameof(destination));
            if (config == null) throw new ArgumentNullException(nameof(config));
            if (string.IsNullOrWhiteSpace(config.IntegrationId)) throw new ArgumentException("Integration id is required", nameof(config));
            if (string.IsNullOrWhiteSpace(config.ExtractorType)) throw new ArgumentException("Extractor type is required", nameof(config));
            if (string.IsNullOrWhiteSpace(config.ExtractorVersion)) throw new ArgumentException("Extractor version is required", nameof(config));

            var request = BuildSetupRequest(config);
            try
            {
                await destination.CogniteClient.Alpha.CdfWriter.SetupAsync(request, token).ConfigureAwait(false);
            }
            catch (ResponseException ex) when (IsPermanentFailure(ex.Code))
            {
                throw new ConfigurationException($"CDF rejected the cdf-mapping configuration: {ex.Code} {ex.Message}", ex);
            }
            catch (Exception ex) when (!token.IsCancellationRequested)
            {
                throw new CogniteUtilsException($"Failed to send cdf-mapping to CDF: {ex.Message}", ex);
            }
        }

        /// <summary>
        /// Build the request sent to the CDF writer setup endpoint from config.
        /// Transforms mappings and bindings into a flattened list of items.
        /// </summary>
        /// <param name="config">Mapping configuration</param>
        /// <returns>Request</returns>
        public static CdfWriterSetupRequest BuildSetupRequest(CdfMappingConfig config)
        {
            if (config == null) throw new ArgumentNullException(nameof(config));
            if (config.Mappings == null) throw new ArgumentException("Mappings are required", nameof(config));
            if (config.Bindings == null) throw new ArgumentException("Bindings are required", nameof(config));

            var mappingDict = config.Mappings.ToDictionary(m => m.ExternalId!);
            var items = new List<CdfWriterSetupItem>();

            foreach (var binding in config.Bindings)
            {
                if (!mappingDict.TryGetValue(binding.MappingId!, out var mapping))
                    continue;

                foreach (var destination in binding.Destinations ?? new List<string>())
                {
                    items.Add(new CdfWriterSetupItem
                    {
                        Destination = destination,
                        Type = mapping.Type,
                        Parameters = ToJsonElement(mapping.Parameters?.Value),
                    });
                }
            }

            return new CdfWriterSetupRequest
            {
                IntegrationId = config.IntegrationId,
                ExtractorType = config.ExtractorType,
                ExtractorVersion = config.ExtractorVersion,
                Items = items,
            };
        }

        /// <summary>
        /// Whether a status code indicates that the request itself was rejected, as opposed
        /// to a transient server or rate limiting error.
        /// </summary>
        private static bool IsPermanentFailure(int status)
        {
            return status >= 400 && status < 500 && status != 408 && status != 429;
        }

        private static JsonElement? ToJsonElement(JsonNode? node)
        {
            if (node == null) return null;
            return JsonSerializer.Deserialize<JsonElement>(node.ToJsonString());
        }
    }
}
