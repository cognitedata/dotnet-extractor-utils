using System;
using System.Collections.Generic;
using System.Net.Http;
using Cognite.Common;
using Cognite.Extractor.Common;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Cognite.Extensions.Unstable
{
    /// <summary>
    /// Base class for authenticator alternatives.
    /// </summary>
    public abstract class BaseAuthenticationConfig
    {
        /// <summary>
        /// Authentication type
        /// </summary>
        public abstract string Type { get; set; }

        /// <summary>
        /// Create an authenticator using this configuration object.
        /// </summary>
        /// <param name="provider">Service provider, must contain an HTTP client.</param>
        /// <returns>Authenticator.</returns>
        public abstract IAuthenticator GetAuthenticator(IServiceProvider provider);

        /// <summary>
        /// Get a map from discriminator value to type for this union.
        /// </summary>
        public static IDictionary<string, Type> Variants()
        {
            return new Dictionary<string, Type>
            {
                { "client-credentials", typeof(ClientCredentialsConfig) },
                { "client-certificate", typeof(ClientCertificateConfig) },
            };
        }

    }


    /// <summary>
    /// Configuration for basic client credentials.
    /// </summary>
    public class ClientCredentialsConfig : BaseAuthenticationConfig
    {
        /// <inheritdoc />
        public override string Type { get; set; } = "client-credentials";

        /// <summary>
        /// The application (client) Id. Required.
        /// </summary>
        /// <value>Client Id</value>
        public string? ClientId { get; set; }
        /// <summary>
        /// The client secret. Required.
        /// </summary>
        /// <value>Secret</value>
        public string? ClientSecret { get; set; }
        /// <summary>
        /// URL to fetch tokens from. Required.
        /// </summary>
        /// <value>Tenant</value>
        public string? TokenUrl { get; set; }
        /// <summary>
        /// Resource scopes
        /// </summary>
        /// <value>Scope</value>
        public ListOrSpaceSeparated? Scopes { get; set; }

        /// <summary>
        /// Resource (optional).
        /// </summary>
        /// <value>Secret</value>
        public string? Resource { get; set; }
        /// <summary>
        /// Audience (optional)
        /// </summary>
        /// <value>Audience</value>
        public string? Audience { get; set; }

        /// <summary>
        /// Minimum time-to-live for the token.
        /// </summary>
        public TimeSpanWrapper MinTtlValue { get; } = new TimeSpanWrapper(true, "s", "30s");

        /// <summary>
        /// Minimum time-to-live for the token in seconds (optional)
        /// </summary>
        /// <value>Minimum TTL</value>
        public string MinTtl
        {
            get => MinTtlValue.RawValue;
            set => MinTtlValue.RawValue = value;
        }

        /// <inheritdoc />
        public override IAuthenticator GetAuthenticator(IServiceProvider provider)
        {
            return new Authenticator(this, provider.GetRequiredService<HttpClient>(), provider.GetService<ILogger<IAuthenticator>>());
        }
    }

    /// <summary>
    /// Configuration for authenticating using a client certificate.
    /// </summary>
    public class ClientCertificateConfig : BaseAuthenticationConfig
    {
        /// <inheritdoc />
        public override string Type { get; set; } = "client-certificate";

        /// <summary>
        /// The application (client) Id. Required.
        /// </summary>
        /// <value>Client Id</value>
        public string? ClientId { get; set; }

        /// <summary>
        /// Path to base 64 encoded x509 certificate, required.
        /// </summary>
        public string? Path { get; set; }
        /// <summary>
        /// Authority URL, required.
        /// </summary>
        public string? AuthorityUrl { get; set; }
        /// <summary>
        /// Certificate password.
        /// </summary>
        public string? Password { get; set; }
        /// <summary>
        /// Resource scopes
        /// </summary>
        /// <value>Scope</value>
        public ListOrSpaceSeparated? Scopes { get; set; }

        /// <inheritdoc />
        public override IAuthenticator GetAuthenticator(IServiceProvider provider)
        {
            return new MsalAuthenticator(
                this,
                provider.GetService<ILogger<IAuthenticator>>() ?? new NullLogger<IAuthenticator>(),
                provider.GetRequiredService<IHttpClientFactory>(),
                "AuthenticatorClient"
            );
        }
    }
}