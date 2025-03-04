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
using Cognite.Extensions.Unstable;
using System.Collections.Generic;

namespace Cognite.Extensions
{
    /// <summary>
    /// Uses Microsoft Authentication Library (MSAL) to acquire tokens from the identity provider endpoint
    /// defined in the <see cref="AuthenticatorConfig.Authority"/> configuration
    /// </summary>
    public class MsalAuthenticator : IAuthenticator
    {
        // Injected properties
        private readonly ILogger<IAuthenticator> _logger;

        private readonly IConfidentialClientApplication _app;
        private DateTimeOffset _lastTokenTime;
        private readonly SemaphoreSlim _mutex = new SemaphoreSlim(1, 1);

        private IEnumerable<string>? _scopes;

        /// <summary>
        /// Creates a new MSAL authenticator
        /// </summary>
        /// <param name="config">Configuration object</param>
        /// <param name="logger">Logger</param>
        /// <param name="httpClientFactory">Http client factory used by this authenticator</param>
        /// <param name="authClientName">Name of http client. Used by the factory to instantiate a pre-configured client</param>
        public MsalAuthenticator(AuthenticatorConfig config, ILogger<IAuthenticator> logger, IHttpClientFactory httpClientFactory, string authClientName)
        {
            if (config is null) throw new ArgumentNullException(nameof(config));
            _logger = logger ?? new NullLogger<MsalAuthenticator>();
            _scopes = config.Scopes?.Values;

            Uri authorityUrl;
            if (!string.IsNullOrWhiteSpace(config.Certificate?.AuthorityUrl))
            {
                authorityUrl = new Uri(config.Certificate!.AuthorityUrl);
            }
            else if (!string.IsNullOrWhiteSpace(config.Authority) && !string.IsNullOrWhiteSpace(config.Tenant))
            {
                var uriBuilder = new UriBuilder(config.Authority);
                uriBuilder.Path = $"{config.Tenant}";
                authorityUrl = uriBuilder.Uri;
            }
            else
            {
                throw new ConfigurationException("MSAL authenticator requires either Certificate.AuthorityUrl or Authority and Tenant");
            }

            var builder = ConfidentialClientApplicationBuilder.Create(config.ClientId)
                .WithHttpClientFactory(new MsalClientFactory(httpClientFactory, authClientName))
                .WithAuthority(authorityUrl);

            if (config.Certificate?.Path != null)
            {
                var ext = Path.GetExtension(config.Certificate.Path);

                X509Certificate2 cert;
#pragma warning disable CA2000
                if (ext == ".pfx")
                {
                    if (config.Certificate.Password != null)
                    {
                        cert = new X509Certificate2(config.Certificate.Path, config.Certificate.Password);
                    }
                    else
                    {
                        cert = new X509Certificate2(config.Certificate.Path);
                    }
                }
#if NET5_0_OR_GREATER
                else if (ext == ".pem")
                {
                    if (config.Certificate.Password != null)
                    {
                        cert = X509Certificate2.CreateFromEncryptedPemFile(config.Certificate.Path, config.Certificate.Password);
                    }
                    else
                    {
                        cert = X509Certificate2.CreateFromPemFile(config.Certificate.Path, config.Certificate.Password);
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
            else if (config.Secret != null)
            {
                builder = builder.WithClientSecret(config.Secret);
            }
            else
            {
                throw new ConfigurationException("Either certificate or client-secret must be configured");
            }

            _app = builder.Build();
        }

        /// <summary>
        /// Build a client with certificate authentication.
        /// </summary>
        /// <param name="config">Configuration object</param>
        /// <param name="logger">Logger</param>
        /// <param name="httpClientFactory">Http client factory used by this authenticator</param>
        /// <param name="authClientName">Name of http client. Used by the factory to instantiate a pre-configured client</param>
        public MsalAuthenticator(ClientCertificateConfig config, ILogger<IAuthenticator> logger, IHttpClientFactory httpClientFactory, string authClientName)
        {
            if (config is null) throw new ArgumentNullException(nameof(config));
            _scopes = config.Scopes?.Values;
            _logger = logger;

            var authorityUrl = new Uri(config.AuthorityUrl ?? throw new ConfigurationException("Missing authority URL for certificate authentication"));
            var builder = ConfidentialClientApplicationBuilder.Create(config.ClientId)
                .WithHttpClientFactory(new MsalClientFactory(httpClientFactory, authClientName))
                .WithAuthority(authorityUrl);

            if (config.Path is null) throw new ConfigurationException("Missing Path for certificate authentication");

            var ext = Path.GetExtension(config.Path);

            X509Certificate2 cert;
#pragma warning disable CA2000
            if (ext == ".pfx")
            {
                if (config.Password != null)
                {
                    cert = new X509Certificate2(config.Path, config.Password);
                }
                else
                {
                    cert = new X509Certificate2(config.Path);
                }
            }
#if NET5_0_OR_GREATER
            else if (ext == ".pem")
            {
                if (config.Password != null)
                {
                    cert = X509Certificate2.CreateFromEncryptedPemFile(config.Path, config.Password);
                }
                else
                {
                    cert = X509Certificate2.CreateFromPemFile(config.Path, config.Password);
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
            _app = builder.WithCertificate(cert).Build();
        }

        /// <summary>
        /// Request a token and cache it until it expires.
        /// </summary>
        /// <param name="token">Cancellation token</param>
        /// <returns>A valid bearer access token</returns>
        /// <exception cref="CogniteUtilsException">Thrown when it was not possible to obtain an authentication token.</exception>
        public async Task<string?> GetToken(CancellationToken token = default)
        {
            await _mutex.WaitAsync(token).ConfigureAwait(false);
            try
            {
                var result = await _app.AcquireTokenForClient(_scopes)
                    .ExecuteAsync(token).ConfigureAwait(false);

                // The client application will take care of caching the token and 
                // renewal before expiration
                if (result.ExpiresOn != _lastTokenTime)
                {
                    _logger.LogDebug(
                        "New OIDC token. Expires on {ttl}",
                        result.ExpiresOn.UtcDateTime.ToISOString());
                    _lastTokenTime = result.ExpiresOn;
                }

                return result.AccessToken;
            }
            catch (MsalServiceException ex)
            {
                _logger.LogError("Unable to obtain OIDC token: {Message}", ex.Message);
                throw new CogniteUtilsException($"Could not obtain OIDC token: {ex.ErrorCode} {ex.Message}");
            }
            finally
            {
                _mutex.Release();
            }
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
