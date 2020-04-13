using System;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;

namespace ExtractorUtils
{
    public class Authenticator
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
        private readonly ILogger<Authenticator> _logger;

        private ResponseDTO _response;
        private DateTime _requestTime;

        public Authenticator(BaseConfig baseConfig, HttpClient client, ILogger<Authenticator> logger)
        {
            _config = baseConfig.Cognite.IdpAuthentication;
            _client = client;
            _logger = logger;
        }

        private async Task<ResponseDTO> RequestToken(CancellationToken token = default)
        {
            var form = new Dictionary<string, string>
            {
                { "client_id", _config.ClientId },
                { "tenant", _config.Tenant },
                { "client_secret", _config.Secret },
                { "scope", _config.Scope },
                { "grant_type", "client_credentials" }
            };
            using (var httpContent = new FormUrlEncodedContent(form))
            {
                _requestTime = DateTime.UtcNow;
                var url = new Uri($"https://login.microsoftonline.com/{_config.Tenant}/oauth2/v2.0/token");
                var response = await _client.PostAsync(url, httpContent, token);
                _logger.LogInformation("Request AAD token {status} {message}", (int) response.StatusCode, response.ReasonPhrase);
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

        /**
         * Request a token and cache it until it expires.
         * TODO: could start a background task to update the token so that this call does not block on the HTTP request.
         **/
        public async Task<string> GetToken(CancellationToken token = default)
        {
            if (_config == null) {
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