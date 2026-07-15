using System;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Cognite.Extensions
{
    /// <summary>
    /// Response from the CDF sessions endpoint.
    /// </summary>
    public class SessionResponse
    {
        /// <summary>
        /// Internal CDF session ID.
        /// </summary>
        [JsonPropertyName("id")]
        public long Id { get; set; }

        /// <summary>
        /// Nonce to pass to a destination's credentials field (e.g. Charon /setup).
        /// </summary>
        [JsonPropertyName("nonce")]
        public string Nonce { get; set; } = "";

        /// <summary>
        /// Session status, e.g. "READY".
        /// </summary>
        [JsonPropertyName("status")]
        public string Status { get; set; } = "";
    }

    /// <summary>
    /// Wraps the CDF sessions API.
    /// CogniteSdk v5 does not expose a Sessions resource on <see cref="CogniteSdk.Client"/>,
    /// so we call the endpoint directly via a raw <see cref="HttpClient"/>.
    /// </summary>
    public class SessionsResource
    {
        private readonly IAuthenticator _authenticator;
        private readonly IHttpClientFactory _httpClientFactory;
        private readonly string _baseUrl;
        private readonly string _project;
        private readonly string? _clientId;
        private readonly string? _clientSecret;
        private readonly ILogger<SessionsResource> _logger;

        /// <summary>
        /// Name of the named <see cref="HttpClient"/> used for session creation requests.
        /// </summary>
        public const string HTTP_CLIENT_NAME = "CharonSessionClient";

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="authenticator">Authenticator that provides bearer tokens.</param>
        /// <param name="httpClientFactory">Factory for <see cref="HttpClient"/> instances.</param>
        /// <param name="baseUrl">CDF base URL, e.g. <c>https://az-arn-dev-002.cognitedata.com</c>.</param>
        /// <param name="project">CDF project name.</param>
        /// <param name="clientId">
        /// OAuth2 client ID. When provided together with <paramref name="clientSecret"/>, the session
        /// is created with <c>clientId</c>/<c>clientSecret</c> in the request body (client-credentials flow),
        /// matching what the Python SDK sends. When null, falls back to <c>tokenExchange</c>.
        /// </param>
        /// <param name="clientSecret">OAuth2 client secret.</param>
        /// <param name="logger">Optional logger.</param>
        public SessionsResource(
            IAuthenticator authenticator,
            IHttpClientFactory httpClientFactory,
            string baseUrl,
            string project,
            string? clientId = null,
            string? clientSecret = null,
            ILogger<SessionsResource>? logger = null)
        {
            _authenticator = authenticator ?? throw new ArgumentNullException(nameof(authenticator));
            _httpClientFactory = httpClientFactory ?? throw new ArgumentNullException(nameof(httpClientFactory));
            _baseUrl = (baseUrl ?? throw new ArgumentNullException(nameof(baseUrl))).TrimEnd('/');
            _project = project ?? throw new ArgumentNullException(nameof(project));
            _clientId = clientId;
            _clientSecret = clientSecret;
            _logger = logger ?? new NullLogger<SessionsResource>();
        }

        /// <summary>
        /// Creates a new CDF session and returns its nonce.
        ///
        /// When <c>clientId</c> and <c>clientSecret</c> were supplied at construction time, the request body
        /// uses the client-credentials format:
        /// <c>{"items": [{"clientId": "...", "clientSecret": "..."}]}</c>.
        /// This is what the Python SDK sends when the client was created with <see cref="OAuthClientCredentials"/>.
        /// Otherwise falls back to <c>{"items": [{"tokenExchange": true}]}</c>.
        /// </summary>
        /// <param name="token">Cancellation token.</param>
        /// <returns>The session response containing the nonce.</returns>
        /// <exception cref="InvalidOperationException">Thrown when the sessions API returns a non-2xx status or an empty nonce.</exception>
        public async Task<SessionResponse> CreateAsync(CancellationToken token)
        {
            var bearerToken = await _authenticator.GetToken(token).ConfigureAwait(false)
                ?? throw new InvalidOperationException("Authenticator returned null token when creating CDF session");

            var url = $"{_baseUrl}/api/v1/projects/{_project}/sessions";
            _logger.LogDebug("Creating CDF session at {Url}", url);

            // Build the item body matching the Python SDK's logic:
            //   client_credentials → {"clientId": "...", "clientSecret": "..."}
            //   token              → {"tokenExchange": true}
            string itemJson;
            if (!string.IsNullOrEmpty(_clientId) && !string.IsNullOrEmpty(_clientSecret))
            {
                itemJson = JsonSerializer.Serialize(new { clientId = _clientId, clientSecret = _clientSecret });
            }
            else
            {
                itemJson = "{\"tokenExchange\":true}";
            }

            using var request = new HttpRequestMessage(HttpMethod.Post, url);
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", bearerToken);
            request.Content = new StringContent(
                $"{{\"items\":[{itemJson}]}}",
                Encoding.UTF8,
                "application/json");

            var client = _httpClientFactory.CreateClient(HTTP_CLIENT_NAME);
            using var response = await client.SendAsync(request, token).ConfigureAwait(false);

#if NET5_0_OR_GREATER
            var body = await response.Content.ReadAsStringAsync(token).ConfigureAwait(false);
#else
            var body = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
#endif

            if (!response.IsSuccessStatusCode)
            {
                throw new InvalidOperationException(
                    $"CDF sessions API returned {(int)response.StatusCode}: {body}");
            }

            // The response is {"items": [{"id": ..., "nonce": "...", "status": "..."}]}
            var doc = JsonDocument.Parse(body);
            var item = doc.RootElement.GetProperty("items")[0];
            var session = JsonSerializer.Deserialize<SessionResponse>(item.GetRawText());

            if (session == null || string.IsNullOrEmpty(session.Nonce))
            {
                throw new InvalidOperationException(
                    $"CDF sessions API returned empty or missing nonce. Body: {body}");
            }

            _logger.LogDebug("Created CDF session id={Id} status={Status}", session.Id, session.Status);
            return session;
        }
    }
}
