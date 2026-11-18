using System;
using System.Collections.Generic;
using System.Linq;
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
        /// Register the extractor's tasks, emitted views and the <c>cdf-writer</c> config with the CDF writer,
        /// replacing the live CDF writer config for the integration. Config should be validated with
        /// <see cref="CdfWriterConfig.Validate"/> before calling this.
        /// </summary>
        /// <param name="destination">Destination to send through</param>
        /// <param name="integrationId">External ID of the integration</param>
        /// <param name="extractorId">Extractor type and version</param>
        /// <param name="config">The <c>cdf-writer</c> config</param>
        /// <param name="declaration">Tasks and emitted views declared by the extractor</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>The config revision stored by the CDF writer</returns>
        /// <exception cref="ConfigurationException">If CDF rejects the config. Retrying with the same config will not help.</exception>
        /// <exception cref="CogniteUtilsException">If the request fails for other reasons, such as rate limiting or server errors.</exception>
        public static async Task<CdfWriterSetupResponse> SetupCdfWriter(
            this CogniteDestination destination,
            string integrationId,
            ExtractorId extractorId,
            CdfWriterConfig config,
            CdfWriterDeclaration declaration,
            CancellationToken token)
        {
            if (destination == null) throw new ArgumentNullException(nameof(destination));
            if (extractorId == null) throw new ArgumentNullException(nameof(extractorId));
            if (config == null) throw new ArgumentNullException(nameof(config));
            if (declaration == null) throw new ArgumentNullException(nameof(declaration));
            if (string.IsNullOrWhiteSpace(integrationId)) throw new ArgumentException("Integration id is required", nameof(integrationId));
            if (string.IsNullOrWhiteSpace(extractorId.ExternalId)) throw new ArgumentException("Extractor type is required", nameof(extractorId));
            if (string.IsNullOrWhiteSpace(extractorId.Version)) throw new ArgumentException("Extractor version is required", nameof(extractorId));

            var request = BuildSetupRequest(config, declaration, integrationId, extractorId);
            try
            {
                return await destination.CogniteClient.Alpha.CdfWriter.SetupAsync(request, token).ConfigureAwait(false);
            }
            catch (ResponseException ex) when (IsPermanentFailure(ex.Code))
            {
                throw new ConfigurationException($"CDF rejected the cdf-writer configuration: {ex.Code} {ex.Message}", ex);
            }
            catch (Exception ex) when (!token.IsCancellationRequested)
            {
                throw new CogniteUtilsException($"Failed to send cdf-writer configuration to CDF: {ex.Message}", ex);
            }
        }

        /// <summary>
        /// Build the request sent to the CDF writer setup endpoint. Mappings and bindings are sent as configured.
        /// </summary>
        /// <param name="config">The <c>cdf-writer</c> config</param>
        /// <param name="declaration">Tasks and emitted views declared by the extractor</param>
        /// <param name="integrationId">External ID of the integration</param>
        /// <param name="extractorId">Extractor type and version</param>
        /// <returns>Request</returns>
        public static CdfWriterSetupRequest BuildSetupRequest(
            CdfWriterConfig config,
            CdfWriterDeclaration declaration,
            string integrationId,
            ExtractorId extractorId)
        {
            if (config == null) throw new ArgumentNullException(nameof(config));
            if (declaration == null) throw new ArgumentNullException(nameof(declaration));
            if (extractorId == null) throw new ArgumentNullException(nameof(extractorId));

            return new CdfWriterSetupRequest
            {
                IntegrationId = integrationId,
                ExtractorType = extractorId.ExternalId,
                ExtractorVersion = extractorId.Version,
                Merge = config.Merge,
                Tasks = declaration.Tasks.ToList(),
                EmittedViews = new Dictionary<string, CdfWriterViewReference>(declaration.EmittedViews),
                Mappings = (config.Mappings ?? Enumerable.Empty<CdfWriterMappingConfig>()).Select(ToMapping).ToList(),
                Bindings = (config.Bindings ?? Enumerable.Empty<CdfWriterBindingConfig>()).Select(b => new CdfWriterBinding
                {
                    TaskIds = b.TaskIds?.ToList(),
                    MappingId = b.MappingId,
                }).ToList(),
            };
        }

        private static CdfWriterMapping ToMapping(CdfWriterMappingConfig mapping)
        {
            return new CdfWriterMapping
            {
                ExternalId = mapping.ExternalId,
                Name = mapping.Name,
                Description = mapping.Description,
                Kind = mapping.Kind,
                ApiVersion = mapping.ApiVersion,
                Resources = mapping.Resources?.ToDictionary(kvp => kvp.Key, kvp => new CdfWriterMappingResource
                {
                    Parameters = kvp.Value.Parameters == null ? null : new CdfWriterSpaceRoutingParameters
                    {
                        Property = kvp.Value.Parameters.Property?.ToList(),
                        Rules = kvp.Value.Parameters.Rules?.Select(r => new CdfWriterSpaceRoutingRule
                        {
                            Pattern = r.Pattern,
                            Space = r.Space,
                        }).ToList(),
                        Default = kvp.Value.Parameters.Default,
                    },
                    Properties = kvp.Value.Properties,
                    AutoCreateDirectRelations = kvp.Value.AutoCreateDirectRelations,
                }),
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
    }
}
