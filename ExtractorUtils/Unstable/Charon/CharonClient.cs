using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extractor.Common;
using Cognite.Extractor.Utils.Unstable.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Cognite.Extractor.Utils.Unstable.Charon
{
    /// <summary>
    /// Typed HTTP client for the Charon CDF-writer service. The bearer token is attached by the
    /// HTTP client's authenticator handler (the same token used for CDF), so this class never
    /// touches credentials directly.
    /// </summary>
    public class CharonClient : ICharonClient
    {
        private readonly HttpClient _client;
        private readonly string _baseUrl;
        private readonly ILogger<CharonClient> _logger;

        /// <summary>
        /// Create a Charon client.
        /// </summary>
        /// <param name="client">HTTP client with the authenticator handler already attached.</param>
        /// <param name="config">CDF-writer config supplying the Charon base URL.</param>
        /// <param name="logger">Logger. Never logs tokens or item bodies.</param>
        /// <exception cref="ConfigurationException">Thrown if the base URL is not configured.</exception>
        public CharonClient(HttpClient client, CdfWriterConfig config, ILogger<CharonClient>? logger)
        {
            if (config == null) throw new ArgumentNullException(nameof(config));
            _client = client ?? throw new ArgumentNullException(nameof(client));
            _logger = logger ?? new NullLogger<CharonClient>();
            var baseUrl = config.BaseUrl?.Trim();
            if (string.IsNullOrEmpty(baseUrl))
            {
                throw new ConfigurationException("Cannot configure Charon client: base URL is not configured");
            }
            _baseUrl = baseUrl!.TrimEnd('/');
        }

        /// <inheritdoc />
        public async Task SetupAsync(CharonSetupRequest request, CancellationToken token)
        {
            if (request == null) throw new ArgumentNullException(nameof(request));
            _logger.LogDebug("Charon setup: {Count} tasks for integration {IntegrationId}",
                request.Items.Count, request.IntegrationId);
            using var response = await PostAsync("/cdfwriter/setup", request, token).ConfigureAwait(false);
            if (response.IsSuccessStatusCode) return;

            var body = await ReadBodyAsync(response, token).ConfigureAwait(false);
            if ((int)response.StatusCode == 422)
            {
                var errors = ParseItemErrors(body);
                throw new CharonSetupException(
                    $"Charon setup validation failed with {errors.Count} error(s)", errors);
            }
            throw BuildException(response.StatusCode, body, "setup");
        }

        /// <inheritdoc />
        public async Task<CharonPayloadResponse> SendPayloadAsync(CharonPayloadRequest request, CancellationToken token)
        {
            if (request == null) throw new ArgumentNullException(nameof(request));
            using var response = await PostAsync("/cdfwriter/payload", request, token).ConfigureAwait(false);
            var body = await ReadBodyAsync(response, token).ConfigureAwait(false);

            // 200 (all tasks ok) and 207 (at least one task failed) both carry a per-task result body.
            if (response.IsSuccessStatusCode || (int)response.StatusCode == 207)
            {
                try
                {
                    return JsonSerializer.Deserialize<CharonPayloadResponse>(body, CharonJson.Options)
                        ?? new CharonPayloadResponse();
                }
                catch (JsonException ex)
                {
                    throw new CharonException("Failed to parse Charon payload response", (int)response.StatusCode, ex);
                }
            }
            if ((int)response.StatusCode == 422)
            {
                var errors = ParseItemErrors(body);
                throw new CharonPayloadValidationException(
                    $"Charon payload validation failed with {errors.Count} error(s)", errors);
            }
            throw BuildException(response.StatusCode, body, "payload");
        }

        /// <summary>Serialize <paramref name="payload"/> and POST it to a Charon endpoint.</summary>
        /// <param name="path">Endpoint path, e.g. <c>/cdfwriter/setup</c>.</param>
        /// <param name="payload">Request object.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The HTTP response (caller disposes).</returns>
        private async Task<HttpResponseMessage> PostAsync(string path, object payload, CancellationToken token)
        {
            var json = JsonSerializer.Serialize(payload, payload.GetType(), CharonJson.Options);
            using var content = new StringContent(json, Encoding.UTF8, "application/json");
            try
            {
                return await _client.PostAsync(_baseUrl + path, content, token).ConfigureAwait(false);
            }
            catch (TaskCanceledException ex) when (!token.IsCancellationRequested)
            {
                throw new CharonException($"Charon request to {path} timed out", 0, ex);
            }
            catch (HttpRequestException ex)
            {
                throw new CharonException($"Charon request to {path} failed to connect", 0, ex);
            }
        }

        /// <summary>Read a response body as a string, tolerant of an empty body.</summary>
        /// <param name="response">HTTP response.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>Body text, or empty string.</returns>
        private static async Task<string> ReadBodyAsync(HttpResponseMessage response, CancellationToken token)
        {
#if NET5_0_OR_GREATER
            return await response.Content.ReadAsStringAsync(token).ConfigureAwait(false);
#else
            return await response.Content.ReadAsStringAsync().ConfigureAwait(false);
#endif
        }

        /// <summary>Parse the per-item error list from a Charon error body.</summary>
        /// <param name="body">Raw error body text.</param>
        /// <returns>Parsed per-item errors, empty if none could be parsed.</returns>
        private static IReadOnlyList<CharonItemError> ParseItemErrors(string body)
        {
            try
            {
                var parsed = JsonSerializer.Deserialize<CharonErrorBody>(body, CharonJson.Options);
                return parsed?.Error?.Errors ?? new List<CharonItemError>();
            }
            catch (JsonException)
            {
                return new List<CharonItemError>();
            }
        }

        /// <summary>Build a <see cref="CharonException"/> for a non-validation error, logging only the status.</summary>
        /// <param name="status">HTTP status code.</param>
        /// <param name="body">Raw error body (parsed for a message, never logged wholesale).</param>
        /// <param name="op">Operation name for the message.</param>
        /// <returns>The exception to throw.</returns>
        private CharonException BuildException(HttpStatusCode status, string body, string op)
        {
            string? message = null;
            try
            {
                message = JsonSerializer.Deserialize<CharonErrorBody>(body, CharonJson.Options)?.Error?.Message;
            }
            catch (JsonException) { }
            _logger.LogWarning("Charon {Op} failed with status {Status}", op, (int)status);
            return new CharonException(
                $"Charon {op} failed with status {(int)status}{(message != null ? $": {message}" : "")}", (int)status);
        }
    }
}
