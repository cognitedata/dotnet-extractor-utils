using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Cognite.Extractor.Utils
{
    /// <summary>
    /// Exception thrown when a Charon API call fails.
    /// Distinct from <see cref="CogniteSdk.ResponseException"/> so callers can
    /// differentiate Charon failures from direct CDF API failures.
    /// </summary>
    public class CharonException : Exception
    {
        /// <summary>
        /// HTTP status code returned by Charon, if available.
        /// </summary>
        public int? StatusCode { get; }

        /// <inheritdoc />
        public CharonException(string message) : base(message) { }

        /// <inheritdoc />
        public CharonException(string message, int statusCode) : base(message)
        {
            StatusCode = statusCode;
        }

        /// <inheritdoc />
        public CharonException(string message, Exception inner) : base(message, inner) { }
    }

    /// <summary>
    /// Interface for the Charon pipeline client.
    /// </summary>
    public interface ICharonClient
    {
        /// <summary>
        /// Calls <c>POST /setup</c> to provision the Charon/Pluto resources for this project.
        /// Creates a CDF session and passes its nonce in the request body.
        /// Idempotent — safe to call multiple times.
        /// </summary>
        /// <exception cref="CharonException">Thrown on non-2xx response.</exception>
        Task SetupAsync(CancellationToken token);

        /// <summary>
        /// Calls <c>POST /insert_payload</c> to send a batch of datapoint items to Charon.
        /// </summary>
        /// <param name="items">Datapoint items to send.</param>
        /// <param name="token">Cancellation token.</param>
        /// <exception cref="CharonException">Thrown when the response is non-2xx or <c>accepted</c> is not true.</exception>
        Task InsertPayloadAsync(IEnumerable<CharonItem> items, CancellationToken token);

        /// <summary>
        /// The Pluto job external ID returned by <c>/setup</c>.
        /// Null until <see cref="SetupAsync"/> has been called successfully.
        /// </summary>
        string? JobExternalId { get; }
    }

#pragma warning disable CA1812
    internal class CharonSetupResponse
    {
        [JsonPropertyName("job_external_id")]
        public string? JobExternalId { get; set; }

        [JsonPropertyName("created")]
        public bool Created { get; set; }
    }

    internal class CharonInsertPayloadResponse
    {
        [JsonPropertyName("accepted")]
        public bool Accepted { get; set; }
    }
#pragma warning restore CA1812

    /// <summary>
    /// HTTP client for the Charon REST-poll pipeline.
    /// </summary>
    public class CharonClient : ICharonClient
    {
        /// <summary>
        /// Name of the named <see cref="HttpClient"/> used for Charon API calls.
        /// </summary>
        public const string HTTP_CLIENT_NAME = "CharonClient";

        private readonly string _charonBaseUrl;
        private readonly string _project;
        private readonly IAuthenticator _authenticator;
        private readonly SessionsResource _sessions;
        private readonly IHttpClientFactory _httpClientFactory;
        private readonly ILogger<CharonClient> _logger;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="charonBaseUrl">
        /// Base URL for Charon requests, e.g. <c>https://az-arn-dev-002.cognitedata.com</c>.
        /// The <c>/api/v1/projects/{project}/charon</c> path is appended automatically.
        /// </param>
        /// <param name="project">CDF project name.</param>
        /// <param name="authenticator">Authenticator that provides bearer tokens.</param>
        /// <param name="sessions">Sessions resource for obtaining CDF session nonces.</param>
        /// <param name="httpClientFactory">Factory for <see cref="HttpClient"/> instances.</param>
        /// <param name="logger">Optional logger.</param>
        public CharonClient(
            string charonBaseUrl,
            string project,
            IAuthenticator authenticator,
            SessionsResource sessions,
            IHttpClientFactory httpClientFactory,
            ILogger<CharonClient>? logger = null)
        {
            if (string.IsNullOrEmpty(charonBaseUrl)) throw new ArgumentNullException(nameof(charonBaseUrl));
            if (string.IsNullOrEmpty(project)) throw new ArgumentNullException(nameof(project));

            _charonBaseUrl = $"{charonBaseUrl.TrimEnd('/')}/api/v1/projects/{project}/charon";
            _project = project;
            _authenticator = authenticator ?? throw new ArgumentNullException(nameof(authenticator));
            _sessions = sessions ?? throw new ArgumentNullException(nameof(sessions));
            _httpClientFactory = httpClientFactory ?? throw new ArgumentNullException(nameof(httpClientFactory));
            _logger = logger ?? new NullLogger<CharonClient>();
        }

        private string? _jobExternalId;

        /// <inheritdoc />
        public string? JobExternalId => _jobExternalId;

        /// <inheritdoc />
        public async Task SetupAsync(CancellationToken token)
        {
            _logger.LogInformation("Calling Charon /setup for project {Project}", _project);

            var bearerToken = await _authenticator.GetToken(token).ConfigureAwait(false)
                ?? throw new CharonException("Authenticator returned null token for Charon /setup");

            var session = await _sessions.CreateAsync(token).ConfigureAwait(false);

            var body = JsonSerializer.Serialize(new
            {
                credentials = new { nonce = session.Nonce }
            });

            var resp = await PostAsync("setup", bearerToken, body, token).ConfigureAwait(false);

            var setup = JsonSerializer.Deserialize<CharonSetupResponse>(resp);
            if (setup == null)
                throw new CharonException("Charon /setup returned an empty or unparseable body");

            _logger.LogInformation(
                "Charon /setup complete — job_external_id={JobId}",
                setup.JobExternalId);

            _jobExternalId = setup.JobExternalId;
        }

        /// <inheritdoc />
        public async Task InsertPayloadAsync(IEnumerable<CharonItem> items, CancellationToken token)
        {
            var bearerToken = await _authenticator.GetToken(token).ConfigureAwait(false)
                ?? throw new CharonException("Authenticator returned null token for Charon /insert_payload");

            var body = JsonSerializer.Serialize(items);
            _logger.LogDebug("Calling Charon /insert_payload");

            var resp = await PostAsync("insert_payload", bearerToken, body, token).ConfigureAwait(false);

            var insert = JsonSerializer.Deserialize<CharonInsertPayloadResponse>(resp);
            if (insert == null || !insert.Accepted)
            {
                throw new CharonException(
                    $"Charon /insert_payload did not return accepted=true. Body: {resp}");
            }

            _logger.LogDebug("Charon /insert_payload accepted");
        }

        private async Task<string> PostAsync(
            string path,
            string bearerToken,
            string jsonBody,
            CancellationToken token)
        {
            var url = $"{_charonBaseUrl}/{path}";
            using var request = new HttpRequestMessage(HttpMethod.Post, url);
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", bearerToken);
            request.Content = new StringContent(jsonBody, Encoding.UTF8, "application/json");

            var client = _httpClientFactory.CreateClient(HTTP_CLIENT_NAME);
            using var response = await client.SendAsync(request, token).ConfigureAwait(false);

#if NET5_0_OR_GREATER
            var body = await response.Content.ReadAsStringAsync(token).ConfigureAwait(false);
#else
            var body = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
#endif

            if (!response.IsSuccessStatusCode)
            {
                throw new CharonException(
                    $"Charon /{path} returned HTTP {(int)response.StatusCode}: {body}",
                    (int)response.StatusCode);
            }

            return body;
        }
    }
}
