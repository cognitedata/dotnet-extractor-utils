using System.Linq;
using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Cognite.Extractor.Configuration;

namespace Cognite.Extractor.Utils
{
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
        Task<string> GetToken(CancellationToken token = default);
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
            public string AccessToken { get; set; }

            [JsonPropertyName("expires_in")]
            public int ExpiresIn { get; set; }
        }

        // Injected properties
        private readonly AuthenticatorConfig _config;
        private readonly HttpClient _client;
        private readonly ILogger<IAuthenticator> _logger;

        private readonly Uri _tokenUri;

        private ResponseDTO _response;
        private DateTime _requestTime;

        /// <summary>
        /// Creates a new authenticator
        /// </summary>
        /// <param name="config">Configuration object</param>
        /// <param name="client">Http client</param>
        /// <param name="logger">Logger</param>
        public Authenticator(AuthenticatorConfig config, HttpClient client, ILogger<IAuthenticator> logger)
        {
            _config = config;
            _client = client;
            _logger = logger;

            if (!string.IsNullOrWhiteSpace(config.TokenUrl))
            {
                _tokenUri = new Uri(config.TokenUrl);
            }
            else if (!string.IsNullOrWhiteSpace(config.Authority))
            {
                var uriBuilder = new UriBuilder(_config.Authority);
                uriBuilder.Path = $"{_config.Tenant}/oauth2/v2.0/token";
                _tokenUri = uriBuilder.Uri;
            }
            else
            {
                throw new ConfigurationException("No AAD tenant or token url defined");
            }
        }

        private async Task<ResponseDTO> RequestToken(CancellationToken token = default)
        {
            var form = new Dictionary<string, string>
            {
                { "client_id", _config.ClientId },
                { "client_secret", _config.Secret },
                { "scope", string.Join(" ", _config.Scopes) },
                { "grant_type", "client_credentials" }
            };

            if (!string.IsNullOrWhiteSpace(_config.Resource))
            {
                form["resource"] = _config.Resource;
            }

            using (var httpContent = new FormUrlEncodedContent(form))
            {
                _requestTime = DateTime.UtcNow;
                var response = await _client.PostAsync(_tokenUri, httpContent, token);
                _logger.LogInformation("Request AAD token {status} {message}", (int)response.StatusCode, response.ReasonPhrase);
                var body = await response.Content.ReadAsStringAsync();
                if (response.IsSuccessStatusCode)
                {
                    return JsonSerializer.Deserialize<ResponseDTO>(body);
                }
                // TODO: parse error json response. 
            }
            return null;
        }

        private bool TokenValid()
        {
            if (_response == null || _response.AccessToken == null)
            {
                return false;
            }
            var difference = (_requestTime + TimeSpan.FromSeconds(_response.ExpiresIn)) - (DateTime.UtcNow + TimeSpan.FromSeconds(_config.MinTtl));
            return _requestTime + TimeSpan.FromSeconds(_response.ExpiresIn) > DateTime.UtcNow + TimeSpan.FromSeconds(_config.MinTtl);
        }

        /// <summary>
        /// Request a token and cache it until it expires.
        /// </summary>
        /// <param name="token">Cancellation token</param>
        /// <returns>A valid bearer access token</returns>
        public async Task<string> GetToken(CancellationToken token = default)
        {
            // TODO: could start a background task to update the token so that this call does not block on the HTTP request.
            if (_config == null)
            {
                _logger.LogInformation("ADD authentication disabled.");
                return null;
            }
            if (TokenValid())
            {
                return _response.AccessToken;
            }

            _requestTime = DateTime.UtcNow;
            _response = await RequestToken(token);

            if (_response != null)
            {
                _logger.LogDebug("New AAD token TTL {ttl}", _response.ExpiresIn);
            }

            return _response?.AccessToken;
        }
    }
}