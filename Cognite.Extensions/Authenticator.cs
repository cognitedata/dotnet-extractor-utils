using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Cognite.Extractor.Common;
using Cognite.Common;
using Cognite.Extensions.Unstable;

namespace Cognite.Extensions
{
    /// <summary>
    /// Configuration for certificate authentication.
    /// </summary>
    public class CertificateConfig
    {
        /// <summary>
        /// Path to base 64 encoded x509 certificate.
        /// </summary>
        public string? Path { get; set; }
        /// <summary>
        /// Authority URL. Either this or [authority]/[tenant] is used.
        /// </summary>
        public string? AuthorityUrl { get; set; }
        /// <summary>
        /// Certificate password.
        /// </summary>
        public string? Password { get; set; }
    }


    /// <summary>
    /// Authenticator configuration. For more information, read the 
    /// <see href="https://docs.microsoft.com/en-us/azure/active-directory/develop/v2-oauth2-client-creds-grant-flow">OAth 2.0 client credentials flow</see>
    /// </summary>
    public class AuthenticatorConfig
    {
        /// <summary>
        /// DEPRECATED: Available authenticator implementations 
        /// </summary>
        public enum AuthenticatorImplementation
        {
            /// <summary>
            /// Use Microsoft Authentication Library (MSAL). Recommended
            /// </summary>
            MSAL,
            /// <summary>
            /// Use a basic implementation. Post requests to the authority endpoint and parse the JSON response in case of success
            /// </summary>
            Basic
        }

        /// <summary>
        /// DEPRECATED: Which implementation to use in the authenticator (optional)
        /// </summary>
        public AuthenticatorImplementation Implementation { get; set; } = AuthenticatorImplementation.MSAL;

        /// <summary>
        /// Identity provider authority endpoint (optional)
        /// </summary>
        /// <value>URI</value>
        public string? Authority { get; set; } = "https://login.microsoftonline.com/";

        /// <summary>
        /// The application (client) Id
        /// </summary>
        /// <value>Client Id</value>
        public string? ClientId { get; set; }

        /// <summary>
        /// The directory tenant. Either this or TokenUrl must be set.
        /// </summary>
        /// <value>Tenant</value>
        public string? Tenant { get; set; }

        /// <summary>
        /// URL to fetch tokens from. Either this or Auhtority / Tenant must be set.
        /// </summary>
        /// <value>Tenant</value>
        public string? TokenUrl { get; set; }

        /// <summary>
        /// The client secret
        /// </summary>
        /// <value>Secret</value>
        public string? Secret { get; set; }

        /// <summary>
        /// Resource (optional, only valid for Basic implementation)
        /// </summary>
        /// <value>Secret</value>
        public string? Resource { get; set; }

        /// <summary>
        /// Resource scopes
        /// </summary>
        /// <value>Scope</value>
        public ListOrSpaceSeparated? Scopes { get; set; }

        /// <summary>
        /// Audience
        /// </summary>
        /// <value>Audience</value>
        public string? Audience { get; set; }

        /// <summary>
        /// Minimum time-to-live for the token in seconds (optional)
        /// </summary>
        /// <value>Minimum TTL</value>
        public int MinTtl { get; set; } = 30;

        /// <summary>
        /// Configuration for using certificate authentication.
        /// </summary>
        public CertificateConfig? Certificate { get; set; }
    }

    /// <summary>
    /// Interface for implementing authenticators based on bearer access tokens issued by an identity provider
    /// </summary>
    public interface IAuthenticator
    {
        /// <summary>
        /// Return a valid(not expired) token that can be used to authorize API calls
        /// </summary>
        /// <param name="token">Cancellation token</param>
        /// <returns>A valid token</returns>
        Task<string?> GetToken(CancellationToken token = default);
    }

    /// <summary>
    /// Authenticator that issues a POST request to an authority endpoint defined in the <see cref="AuthenticatorConfig.Authority"/> configuration
    /// in order to obtain bearer access tokens.
    /// The token is cached and renewed if it expired
    /// </summary>
    public class Authenticator : IAuthenticator
    {

#pragma warning disable CA1812
        private class ResponseDTO
#pragma warning restore CA1812
        {
            [JsonPropertyName("access_token")]
            public string? AccessToken { get; set; }

            [JsonPropertyName("expires_in")]
            public int ExpiresIn { get; set; }
        }

#pragma warning disable CA1812
        private class ErrorDTO
#pragma warning restore CA1812
        {
            [JsonPropertyName("error")]
            public string? Error { get; set; }

            [JsonPropertyName("error_description")]
            public string? ErrorDescription { get; set; }

            [JsonPropertyName("error_uri")]
            public string? ErrorDUri { get; set; }
        }

        // Injected properties
        private readonly Options _config;
        private readonly HttpClient _client;
        private readonly ILogger<IAuthenticator> _logger;

        private readonly SemaphoreSlim _mutex = new SemaphoreSlim(1, 1);

        private ResponseDTO? _response;
        private DateTime _requestTime;

        /// <summary>
        /// Authenticator options.
        /// </summary>
        public class Options
        {
            /// <summary>
            /// The application (client) Id. Required.
            /// </summary>
            /// <value>Client Id</value>
            public string ClientId { get; set; }
            /// <summary>
            /// The client secret. Required.
            /// </summary>
            /// <value>Secret</value>
            public string ClientSecret { get; set; }
            /// <summary>
            /// Resource scopes, space separated.
            /// </summary>
            /// <value>Scope</value>
            public string? Scopes { get; set; }
            /// <summary>
            /// Audience (optional)
            /// </summary>
            /// <value>Audience</value>
            public string? Audience { get; set; }
            /// <summary>
            /// Resource (optional).
            /// </summary>
            /// <value>Secret</value>
            public string? Resource { get; set; }

            /// <summary>
            /// URL to fetch tokens from. Required.
            /// </summary>
            /// <value>Tenant</value>
            public Uri TokenUrl { get; set; }

            /// <summary>
            /// Minimum time-to-live for the token.
            /// </summary>
            public TimeSpan MinTtl { get; set; } = TimeSpan.FromSeconds(30);

            /// <summary>
            /// Create a new set of authenticator options with the required parameters.
            /// </summary>
            /// <param name="clientId">Client ID</param>
            /// <param name="clientSecret">Client secret</param>
            /// <param name="tokenUrl">Token URL</param>
            public Options(string clientId, string clientSecret, Uri tokenUrl)
            {
                ClientId = clientId;
                ClientSecret = clientSecret;
                TokenUrl = tokenUrl;
            }

            /// <summary>
            /// Create a new set of authenticator options from authenticator config.
            /// </summary>
            /// <param name="config"></param>
            /// <exception cref="ArgumentNullException"></exception>
            /// <exception cref="ConfigurationException"></exception>
            public Options(AuthenticatorConfig config)
            {
                if (config is null) throw new ArgumentNullException(nameof(config));

                ClientId = config.ClientId ?? throw new ConfigurationException("Missing client ID");
                ClientSecret = config.Secret ?? throw new ConfigurationException("Missing client secret");
                TokenUrl = new Uri(config.TokenUrl ?? throw new ConfigurationException("Missing token URL"));
                Scopes = config.Scopes != null ? string.Join(" ", config.Scopes) : null;
                Audience = config.Audience;
                Resource = config.Resource;
                MinTtl = TimeSpan.FromSeconds(config.MinTtl);
            }

            /// <summary>
            /// Create a new set of authenticator options from client credentials config.
            /// </summary>
            /// <param name="config"></param>
            /// <exception cref="ArgumentNullException"></exception>
            /// <exception cref="ConfigurationException"></exception>
            public Options(ClientCredentialsConfig config)
            {
                if (config is null) throw new ArgumentNullException(nameof(config));

                ClientId = config.ClientId ?? throw new ConfigurationException("Missing client ID");
                ClientSecret = config.ClientSecret ?? throw new ConfigurationException("Missing client secret");
                TokenUrl = new Uri(config.TokenUrl ?? throw new ConfigurationException("Missing token URL"));
                Scopes = config.Scopes != null ? string.Join(" ", config.Scopes) : null;
                Audience = config.Audience;
                Resource = config.Resource;
                MinTtl = config.MinTtlValue.Value;
            }

            internal Dictionary<string, string> GetFormdata()
            {
                var form = new Dictionary<string, string>
                {
                    { "client_id", ClientId! },
                    { "client_secret", ClientSecret! },
                    { "grant_type", "client_credentials" }
                };

                if (!string.IsNullOrWhiteSpace(Scopes))
                {
                    form["scope"] = Scopes!;
                }

                if (!string.IsNullOrWhiteSpace(Audience))
                {
                    form["audience"] = Audience!;
                }

                if (!string.IsNullOrWhiteSpace(Resource))
                {
                    form["resource"] = Resource!;
                }

                return form;
            }
        }

        /// <summary>
        /// Creates a new authenticator
        /// </summary>
        /// <param name="config">Configuration object</param>
        /// <param name="client">Http client</param>
        /// <param name="logger">Logger</param>
        public Authenticator(AuthenticatorConfig config, HttpClient client, ILogger<IAuthenticator>? logger)
        {
            if (config == null)
            {
                throw new ConfigurationException("Configuration missing");
            }
            if (config.Certificate != null)
            {
                throw new ConfigurationException("Certificate configuration cannot be used with basic authenticator");
            }
            _config = new Options(config);
            _client = client;
            _logger = logger ?? new Microsoft.Extensions.Logging.Abstractions.NullLogger<Authenticator>();
        }

        /// <summary>
        /// Creates a new authenticator
        /// </summary>
        /// <param name="config">Configuration object</param>
        /// <param name="client">Http client</param>
        /// <param name="logger">Logger</param>
        public Authenticator(ClientCredentialsConfig config, HttpClient client, ILogger<IAuthenticator>? logger)
        {
            if (config == null)
            {
                throw new ConfigurationException("Configuration missing");
            }
            _config = new Options(config);
            _client = client;
            _logger = logger ?? new Microsoft.Extensions.Logging.Abstractions.NullLogger<Authenticator>();
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Reliability", "CA2007: Do not directly await a Task", Justification = "Awaiter configured by the caller")]
        private async Task<ResponseDTO> RequestToken(CancellationToken token = default)
        {
            var form = _config.GetFormdata();

            using (var httpContent = new FormUrlEncodedContent(form))
            {
                var response = await _client.PostAsync(_config.TokenUrl, httpContent, token);
#if NET5_0_OR_GREATER
                var body = await response.Content.ReadAsStringAsync(token);
#else
                var body = await response.Content.ReadAsStringAsync();
#endif
                if (response.IsSuccessStatusCode)
                {
                    var tokenResponse = JsonSerializer.Deserialize<ResponseDTO>(body);
                    if (tokenResponse == null)
                    {
                        throw new CogniteUtilsException("Could not obtain OIDC token: Empty response");
                    }

                    if (tokenResponse.AccessToken == null)
                    {
                        throw new CogniteUtilsException("Successfully requested OIDC token, but the access-token was null");
                    }

                    _logger.LogDebug(
                        "New OIDC token. Expires on {ttl}",
                        (DateTime.UtcNow + TimeSpan.FromSeconds(tokenResponse.ExpiresIn)).ToISOString());
                    return tokenResponse;
                }
                else
                {
                    try
                    {
                        var error = JsonSerializer.Deserialize<ErrorDTO>(body);
                        if (error == null)
                        {
                            throw new CogniteUtilsException("Could not obtain OIDC token: Empty error");
                        }
                        _logger.LogError("Unable to obtain OIDC token: {Message}", error.ErrorDescription);
                        throw new CogniteUtilsException($"Could not obtain OIDC token: {error.Error} {error.ErrorDescription}");
                    }
                    catch (JsonException ex)
                    {
                        _logger.LogError("Unable to obtain OIDC token: R{Code} - {Message}", (int)response.StatusCode, response.ReasonPhrase);
                        throw new CogniteUtilsException(
                            $"Could not obtain OIDC token: {(int)response.StatusCode} - {response.ReasonPhrase}",
                            ex);
                    }
                }
            }
        }

        private bool TokenValid()
        {
            if (_response == null || _response.AccessToken == null)
            {
                return false;
            }
            return _requestTime + TimeSpan.FromSeconds(_response.ExpiresIn) > DateTime.UtcNow + _config.MinTtl;
        }

        /// <summary>
        /// Request a token and cache it until it expires.
        /// </summary>
        /// <param name="token">Cancellation token</param>
        /// <returns>A valid bearer access token</returns>
        /// <exception cref="CogniteUtilsException">Thrown when it was not possible to obtain an authentication token.</exception>
        public async Task<string?> GetToken(CancellationToken token = default)
        {
            if (_config == null)
            {
                _logger.LogInformation("OIDC authentication disabled.");
                return null;
            }
            await _mutex.WaitAsync(token).ConfigureAwait(false);
            try
            {
                if (TokenValid())
                {
                    return _response?.AccessToken;
                }

                var time = DateTime.UtcNow;
                _response = await RequestToken(token).ConfigureAwait(false);
                _requestTime = time;

                return _response.AccessToken;
            }
            finally
            {
                _mutex.Release();
            }
        }
    }

    /// <summary>
    /// Delegating handler for adding authentication tokens to requests.
    /// </summary>
    public class AuthenticatorDelegatingHandler : DelegatingHandler
    {
        private IAuthenticator? _authenticator;
        /// <summary>
        /// Delegating handler for adding authentication tokens to requests.
        /// Used instead of auth in the SDK to add tokens _inside_ retries.
        /// </summary>
        /// <param name="authenticator">The inner authenticator.
        /// Can be null, in which case this handler is a no-op.</param>
        public AuthenticatorDelegatingHandler(IAuthenticator? authenticator)
        {
            _authenticator = authenticator;
        }

        /// <inheritdoc />
        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            if (request == null) throw new ArgumentNullException(nameof(request));
            if (_authenticator == null)
            {
                return await base.SendAsync(request, cancellationToken).ConfigureAwait(false);
            }
            var token = await _authenticator.GetToken(cancellationToken).ConfigureAwait(false);
            if (token != null)
            {
                request.Headers.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", token);
            }
            else
            {
                request.Headers.Remove("Authorization");
            }

            return await base.SendAsync(request, cancellationToken).ConfigureAwait(false);
        }
    }
}