using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Identity.Client;
using Cognite.Extractor.Common;
using System.Net.Http;
using Microsoft.Extensions.Logging.Abstractions;
using System.IO;
using System.Security.Cryptography.X509Certificates;

namespace Cognite.Extensions
{
    /// <summary>
    /// Uses Microsoft Authentication Library (MSAL) to acquire tokens from the identity provider endpoint
    /// defined in the <see cref="AuthenticatorConfig.Authority"/> configuration
    /// </summary>
    public class MsalAuthenticator : IAuthenticator
    {
        // Injected properties
        private readonly AuthenticatorConfig? _config;
        private readonly ILogger<IAuthenticator> _logger;

        private readonly IConfidentialClientApplication? _app;
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
            _logger = logger ?? new NullLogger<MsalAuthenticator>();
            if (_config != null) {
                Uri authorityUrl;
                if (_config.Certificate?.AuthorityUrl != null)
                {
                    authorityUrl = new Uri(_config.Certificate.AuthorityUrl);
                }
                else if (_config.Authority != null && _config.Tenant != null)
                {
                    var uriBuilder = new UriBuilder(_config.Authority);
                    uriBuilder.Path = $"{_config.Tenant}";
                    authorityUrl = uriBuilder.Uri;
                }
                else
                {
                    throw new ConfigurationException("MSAL authenticator requires either Certificate.AuthorityUrl or Authority and Tenant");
                }

                var builder = ConfidentialClientApplicationBuilder.Create(_config.ClientId)
                    .WithHttpClientFactory(new MsalClientFactory(httpClientFactory, authClientName))
                    .WithAuthority(authorityUrl);

                if (_config.Certificate != null)
                {
                    if (_config.Certificate.Path == null) throw new ConfigurationException("Certificate path is required for certificate authentication");
                    var ext = Path.GetExtension(_config.Certificate.Path);

                    X509Certificate2 cert;
#pragma warning disable CA2000
                    if (ext == ".pfx")
                    {
                        if (_config.Certificate.Password != null)
                        {
                            cert = new X509Certificate2(_config.Certificate.Path, _config.Certificate.Password);
                        }
                        else
                        {
                            cert = new X509Certificate2(_config.Certificate.Path);
                        }
                    }
#if NET5_0_OR_GREATER
                    else if (ext == ".pem")
                    {
                        if (_config.Certificate.Password != null)
                        {
                            cert = X509Certificate2.CreateFromEncryptedPemFile(_config.Certificate.Path, _config.Certificate.Password);
                        }
                        else
                        {
                            cert = X509Certificate2.CreateFromPemFile(_config.Certificate.Path, _config.Certificate.Password);
                        }
                    }
                    else 
                    {
                        throw new ConfigurationException($"Unrecognized certificate extension {ext}. Only .pem and .pfx files are supported");
                    }
#else
                    else
                    {
                        throw new ConfigurationException($"Unrecognized certificate extension {ext}. Only .pfx files are supported");
                    }
#endif

#pragma warning restore CA2000
                    builder = builder.WithCertificate(cert);
                }
                else if (_config.Secret != null)
                {
                    builder = builder.WithClientSecret(_config.Secret);
                }
                else
                {
                    throw new ConfigurationException("Either certificate or client-secret must be configured");
                }

                _app = builder.Build();
            }
        }

        /// <summary>
        /// Request a token and cache it until it expires.
        /// </summary>
        /// <param name="token">Cancellation token</param>
        /// <returns>A valid bearer access token</returns>
        /// <exception cref="CogniteUtilsException">Thrown when it was not possible to obtain an authentication token.</exception>
        public async Task<string?> GetToken(CancellationToken token = default)
        {
            if (_config == null || _app == null) {
                _logger.LogInformation("OIDC authentication disabled.");
                return null;
            }

            AuthenticationResult result;
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

            return result.AccessToken;
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
