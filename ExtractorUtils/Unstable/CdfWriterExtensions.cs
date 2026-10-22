using System;
using System.Collections;
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
        /// existing mapping for the integration. Items should be validated with
        /// <see cref="CdfMappingItem.Validate"/> before calling this.
        /// </summary>
        /// <param name="destination">Destination to send through</param>
        /// <param name="integrationId">External ID of the integration the mapping belongs to</param>
        /// <param name="extractorId">Extractor type and version</param>
        /// <param name="items">Mapping items</param>
        /// <param name="token">Cancellation token</param>
        /// <exception cref="ConfigurationException">If CDF rejects the mapping. Retrying with the same config will not help.</exception>
        /// <exception cref="CogniteUtilsException">If the request fails for other reasons, such as rate limiting or server errors.</exception>
        public static async Task SetupConfig(
            this CogniteDestination destination,
            string integrationId,
            ExtractorId extractorId,
            IEnumerable<CdfMappingItem> items,
            CancellationToken token)
        {
            if (destination == null) throw new ArgumentNullException(nameof(destination));
            if (extractorId == null) throw new ArgumentNullException(nameof(extractorId));
            if (items == null) throw new ArgumentNullException(nameof(items));
            if (string.IsNullOrWhiteSpace(integrationId)) throw new ArgumentException("Integration id is required", nameof(integrationId));
            if (string.IsNullOrWhiteSpace(extractorId.ExternalId)) throw new ArgumentException("Extractor type is required", nameof(extractorId));
            if (string.IsNullOrWhiteSpace(extractorId.Version)) throw new ArgumentException("Extractor version is required", nameof(extractorId));

            var request = BuildSetupRequest(items, integrationId, extractorId);
            try
            {
                await destination.CogniteClient.Alpha.CdfWriter.SetupAsync(request, token).ConfigureAwait(false);
            }
            catch (ResponseException ex) when (IsPermanentFailure(ex.Code))
            {
                throw new ConfigurationException($"CDF rejected the cdf-mapping configuration: {ex.Code} {ex.Message}", ex);
            }
            catch (Exception ex) when (!(ex is OperationCanceledException))
            {
                throw new CogniteUtilsException($"Failed to send cdf-mapping to CDF: {ex.Message}", ex);
            }
        }

        /// <summary>
        /// Build the request sent to the CDF writer setup endpoint from config items.
        /// </summary>
        /// <param name="items">Mapping items</param>
        /// <param name="integrationId">External ID of the integration</param>
        /// <param name="extractorId">Extractor type and version</param>
        /// <returns>Request</returns>
        public static CdfWriterSetupRequest BuildSetupRequest(
            IEnumerable<CdfMappingItem> items,
            string integrationId,
            ExtractorId extractorId)
        {
            if (items == null) throw new ArgumentNullException(nameof(items));
            if (extractorId == null) throw new ArgumentNullException(nameof(extractorId));
            return new CdfWriterSetupRequest
            {
                IntegrationId = integrationId,
                ExtractorType = extractorId.ExternalId,
                ExtractorVersion = extractorId.Version,
                Items = items.Select(item => new CdfWriterSetupItem
                {
                    TaskName = item.TaskName,
                    Type = item.Type,
                    Parameters = ToJsonElement(item.Parameters),
                    Destination = item.Destination,
                    Fields = item.Fields,
                }).ToList(),
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

        private static JsonElement? ToJsonElement(object? value)
        {
            var node = ToJson(value);
            if (node == null) return null;
            return JsonSerializer.Deserialize<JsonElement>(node.ToJsonString());
        }

        /// <summary>
        /// Convert an untyped deserialized YAML value to JSON. YAML scalars are deserialized as strings,
        /// mappings as dictionaries and sequences as lists.
        /// </summary>
        private static JsonNode? ToJson(object? value)
        {
            switch (value)
            {
                case null:
                    return null;
                case string s:
                    return JsonValue.Create(s);
                case IDictionary dict:
                    var obj = new JsonObject();
                    foreach (DictionaryEntry entry in dict)
                    {
                        obj[entry.Key.ToString() ?? ""] = ToJson(entry.Value);
                    }
                    return obj;
                case IEnumerable list:
                    var arr = new JsonArray();
                    foreach (var child in list)
                    {
                        arr.Add(ToJson(child));
                    }
                    return arr;
                default:
                    return JsonValue.Create(value.ToString());
            }
        }
    }
}
