using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Identity.Client;
using Cognite.Extensions;
using Cognite.Extractor.Common;
using System.Net.Http;

namespace Cognite.Extractor.Utils
{
    /// <summary>
    /// Uses Microsoft Authentication Library (MSAL) to acquire tokens from the identity provider endpoint
    /// defined in the <see cref="AuthenticatorConfig.Authority"/> configuration
    /// </summary>
    public class MsalAuthenticator : IAuthenticator
    {
        // Injected properties
        private readonly AuthenticatorConfig _config;
        private readonly ILogger<IAuthenticator> _logger;

        private readonly IConfidentialClientApplication _app;
        private DateTimeOffset _lastTokenTime;

        /// <summary>
        /// Creates a new MSAL authenticator
        /// </summary>
        /// <param name="config">Configuration object</param>
        /// <param name="logger">Logger</param>
        /// <param name="httpClientFactory">Http client factory used by this authenticator</param>
        /// <param name="authClientName">Name of http client. Used by the factory to instantiate a pre-configured client</param>
        public MsalAuthenticator(AuthenticatorConfig config, ILogger<IAuthenticator> logger, IHttpClientFactory httpClientFactory, string authClientName)
        {
            _config = config;
            _logger = logger;
            if (_config != null) {
                var uriBuilder = new UriBuilder(_config.Authority);
                uriBuilder.Path = $"{_config.Tenant}";
                var url = uriBuilder.Uri;
                _app = ConfidentialClientApplicationBuilder.Create(_config.ClientId)
                    .WithHttpClientFactory(new MsalClientFactory(httpClientFactory, authClientName))
                    .WithClientSecret(_config.Secret)
                    .WithAuthority(url)
                    .Build();
            }
        }

        /// <summary>
        /// Request a token and cache it until it expires.
        /// </summary>
        /// <param name="token">Cancellation token</param>
        /// <returns>A valid bearer access token</returns>
        /// <exception cref="CogniteUtilsException">Thrown when it was not possible to obtain an authentication token.</exception>
        public async Task<string> GetToken(CancellationToken token = default)
        {
            if (_config == null) {
                _logger.LogInformation("OIDC authentication disabled.");
                return null;
            }

            AuthenticationResult result = null;
            try
            {
                result = await _app.AcquireTokenForClient(_config.Scopes)
                    .ExecuteAsync(token).ConfigureAwait(false);
                
                // The client application will take care of caching the token and 
                // renewal before expiration
                if (result.ExpiresOn != _lastTokenTime) {
                    _logger.LogDebug(
                        "New OIDC token. Expires on {ttl}", 
                        result.ExpiresOn.UtcDateTime.ToISOString());
                    _lastTokenTime = result.ExpiresOn;
                }
            }
            catch (MsalServiceException ex)
            {
                _logger.LogError("Unable to obtain OIDC token: {Message}", ex.Message);
                throw new CogniteUtilsException($"Could not obtain OIDC token: {ex.ErrorCode} {ex.Message}");
            }

            return result?.AccessToken;
        }
    }

    internal class MsalClientFactory : IMsalHttpClientFactory
    {
        private readonly IHttpClientFactory httpClientFactory;
        private readonly string clientName;

        public MsalClientFactory(IHttpClientFactory httpClientFactory, string clientName)
        {
            this.httpClientFactory = httpClientFactory;
            this.clientName = clientName;
        }

        public HttpClient GetHttpClient()
        {
            var client = httpClientFactory.CreateClient(clientName);
            return client;
        }
    }
}
