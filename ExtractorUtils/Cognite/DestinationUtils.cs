﻿using System.Net;
using System.Net.Http.Headers;
using System;
using System.Net.Http;
using System.Threading;
using Cognite.Extensions;
using Cognite.Extractor.Common;
using Cognite.Extractor.StateStorage;
using CogniteSdk;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using System.Linq;
using System.Net.Security;

namespace Cognite.Extractor.Utils
{
    /// <summary>
    /// Utilities for the setting up a cognite destination.
    /// </summary>
    public static class DestinationUtils
    {
        private static CogniteDestination GetCogniteDestination(IServiceProvider provider)
        {
            var client = provider.GetService<Client>();
            var logger = provider.GetService<ILogger<CogniteDestination>>();
            var config = provider.GetService<CogniteConfig>();
            if (client == null || config == null) return null!;
            return new CogniteDestination(client, logger ?? new NullLogger<CogniteDestination>(), config);
        }

        private static CogniteDestinationWithIDM GetCogniteDestinationWithIDM(IServiceProvider provider)
        {
            var client = provider.GetService<Client>();
            var logger = provider.GetService<ILogger<CogniteDestinationWithIDM>>();
            var config = provider.GetService<CogniteConfig>();
            if (client == null || config == null) return null!;
            return new CogniteDestinationWithIDM(client, logger ?? new NullLogger<CogniteDestinationWithIDM>(), config);
        }

#if NETSTANDARD2_1_OR_GREATER
        /// <summary>
        /// Return a http handler configured to ignore certificate errors based on passed CertificateConfig.
        /// </summary>
        /// <param name="config">Certificate config to use</param>
        public static HttpClientHandler GetClientHandler(CertificateConfig? config)
        {
            var handler = new HttpClientHandler();
            if (config == null) return handler;

            handler.ServerCertificateCustomValidationCallback = (message, cert, chain, sslPolicyErrors) => {
                if (sslPolicyErrors == SslPolicyErrors.None) return true;

                if (config.AcceptAll) return true;

                if (config.AllowList?.Any(acc => acc.ToLower() == cert.GetCertHashString().ToLower()) ?? false) return true;

                return false;
            };

            return handler;
        }
#endif

        private static bool _sslPolicyConfigured;
        /// <summary>
        /// Configure global handling of SSL certificates.
        /// This must be called to ignore certificates if you require the .NET standard 2.0 version of the library,
        /// since .NET framework lacks local ignoring of SSL errors.
        /// </summary>
        /// <param name="config"></param>
        public static void ConfigureSslPolicy(CertificateConfig config)
        {
            if (_sslPolicyConfigured) return;
            _sslPolicyConfigured = true;
            ServicePointManager.ServerCertificateValidationCallback += (sender, cert, chain, sslPolicyErrors) =>
            {
                if (sslPolicyErrors == SslPolicyErrors.None) return true;

                if (config.AcceptAll) return true;

                if (cert != null && (config.AllowList?.Any(acc => acc.ToLower() == cert.GetCertHashString().ToLower()) ?? false)) return true;

                return false;
            };
        }

        /// <summary>
        /// Add a few HTTP client handlers for retries, timeouts, and certificate policy to an HTTP client builder.
        /// </summary>
        /// <param name="builder">The client builder to modify.</param>
        /// <returns>The given client builder.</returns>
        public static IHttpClientBuilder ConfigureCogniteHttpClientHandlers(this IHttpClientBuilder builder)
        {
            return builder.AddPolicyHandler((provider, message) =>
            {
                try
                {
                    var retryConfig = provider.GetService<CogniteConfig>()?.CdfRetries;
                    return CogniteExtensions.GetTimeoutPolicy(retryConfig?.Timeout);
                }
                catch (ObjectDisposedException)
                {
                    return CogniteExtensions.GetTimeoutPolicy(null);
                }
            })
            .AddPolicyHandler((provider, message) =>
            {
                try
                {
                    var retryConfig = provider.GetService<CogniteConfig>()?.CdfRetries;
                    return CogniteExtensions.GetRetryPolicy(provider.GetService<ILogger<Client>>(),
                        retryConfig?.MaxRetries, retryConfig?.MaxDelay);
                }
                catch (ObjectDisposedException)
                {
                    return CogniteExtensions.GetRetryPolicy(new NullLogger<Client>(), null, null);
                }
            })
#if NETSTANDARD2_1_OR_GREATER
            .ConfigurePrimaryHttpMessageHandler(provider =>
            {
                try
                {
                    var certConfig = provider.GetService<CogniteConfig>()?.Certificates;
                    return GetClientHandler(certConfig);
                }
                catch (ObjectDisposedException) 
                {
                    return GetClientHandler(null);
                }
            });
#else
            ;
#endif
        }

        /// <summary>
        /// The name of the authenticator client, use this if you
        /// wish to add your own HTTP client for authentication.
        /// </summary>
        public const string AUTH_CLIENT_NAME = "AuthenticatorClient";

        /// <summary>
        /// Adds a configured Cognite client to the <paramref name="services"/> collection as a transient service
        /// </summary>
        /// <param name="services">The service collection</param>
        /// <param name="appId">Identifier of the application using the Cognite API</param>
        /// <param name="userAgent">User-Agent header. If added, should conform to RFC 7231: 'Product/Version (Optional comment)'</param>
        /// <param name="setLogger">If true, a <see cref="ILogger"/> logger is created and used by the client to log calls to the 
        /// Cognite API (enabled in debug mode)</param>
        /// <param name="setMetrics">If true, a <see cref="IMetrics"/> metrics collector is created and used by the client
        /// to report metrics on the number and duration of API requests</param>
        /// <param name="setHttpClient">Default true. If false CogniteSdk Client.Builder is not added to the
        /// <see cref="ServiceCollection"/>. If this is false it must be added before this method is called.</param>
        /// <param name="required">True to fail if cognite configuration is missing</param>
        public static void AddCogniteClient(this IServiceCollection services,
                                            string? appId,
                                            string? userAgent = null,
                                            bool setLogger = false,
                                            bool setMetrics = false,
                                            bool setHttpClient = true,
                                            bool required = true)
        {
            if (setHttpClient)
            {
                services.AddHttpClient<Client.Builder>(c => c.Timeout = Timeout.InfiniteTimeSpan)
                    .AddHttpMessageHandler(provider =>
                    {
                        try
                        {
                            return new AuthenticatorDelegatingHandler(provider.GetService<IAuthenticator>());
                        }
                        catch (ObjectDisposedException)
                        {
                            return new AuthenticatorDelegatingHandler(null);
                        }
                    })
                    .ConfigureCogniteHttpClientHandlers();

                // Configure token based authentication
                services.AddHttpClient(
                    AUTH_CLIENT_NAME,
                    c =>
                    {
                        if (userAgent != null)
                        {
                            c.DefaultRequestHeaders.UserAgent.ParseAdd(userAgent);
                        }
                    })
                    .ConfigureCogniteHttpClientHandlers();
            }

            services.AddTransient<IAuthenticator>(provider =>
            {
                var conf = provider.GetService<CogniteConfig>();
                if (conf?.IdpAuthentication == null)
                    return null!;
                var logger = provider.GetRequiredService<ILogger<IAuthenticator>>();
                var clientFactory = provider.GetRequiredService<IHttpClientFactory>();

                if (!string.IsNullOrWhiteSpace(conf.IdpAuthentication.Tenant)
                    || !string.IsNullOrWhiteSpace(conf.IdpAuthentication.Certificate?.Path))
                {
                    return new MsalAuthenticator(conf.IdpAuthentication, logger, clientFactory, AUTH_CLIENT_NAME);
                }

                var client = clientFactory.CreateClient(AUTH_CLIENT_NAME);
                return new Authenticator(conf.IdpAuthentication, client, logger);
            });

            services.AddSingleton<IMetrics, CdfMetricCollector>();
            services.AddTransient(provider =>
            {
                var conf = provider.GetService<CogniteConfig>();
                if ((conf == null || conf.Project?.TrimToNull() == null) && !required) return null!;
                var cdfBuilder = provider.GetRequiredService<Client.Builder>();
                var logger = setLogger ?
                    provider.GetRequiredService<ILogger<Client>>() : null;
                CogniteExtensions.AddExtensionLoggers(provider);
                var metrics = setMetrics ?
                    provider.GetRequiredService<IMetrics>() : null;
                var client = cdfBuilder.Configure(conf!, appId, userAgent, null, logger, metrics).Build();
                return client;
            });
            services.AddTransient(GetCogniteDestination);
            services.AddTransient<IRawDestination, CogniteDestination>(GetCogniteDestination);
            services.AddTransient(GetCogniteDestinationWithIDM);
            services.AddTransient<IRawDestination, CogniteDestinationWithIDM>(GetCogniteDestinationWithIDM);
        }


        /// <summary>
        /// Configure a CogniteSdk Client.Builder according to the <paramref name="config"/> object
        /// </summary>
        /// <param name="clientBuilder">This builder</param>
        /// <param name="config">A <see cref="CogniteConfig"/> configuration object</param>
        /// <param name="appId">Identifier of the application using the Cognite API</param>
        /// <param name="userAgent">User-agent header</param>
        /// <param name="auth">A <see cref="IAuthenticator"/> authenticator used to obtain bearer access token</param>
        /// <param name="logger">A <see cref="ILogger"/> logger that the client can use to log calls to the 
        /// Cognite API (enabled in debug mode)</param>
        /// <param name="metrics">A <see cref="IMetrics"/> metrics collector, that the client can use
        /// to report metrics on the number and duration of API requests</param>
        /// <returns>A configured builder</returns>
        /// <exception cref="CogniteUtilsException">Thrown when <paramref name="config"/> is null or 
        /// the configured project is empty</exception>
        public static Client.Builder Configure(
            this Client.Builder clientBuilder,
            CogniteConfig config,
            string? appId,
            string? userAgent = null,
            IAuthenticator? auth = null,
            ILogger<Client>? logger = null,
            IMetrics? metrics = null)
        {
            if (config == null)
            {
                throw new CogniteUtilsException("Cannot configure Builder: Configuration is missing");
            }
            if (config.Project?.TrimToNull() == null)
            {
                throw new CogniteUtilsException("Cannot configure Builder: Project is not configured");
            }

            var builder = clientBuilder
                .SetAppId(appId)
                .SetProject(config.Project);

            if (userAgent != null)
            {
                builder = builder.SetUserAgent(userAgent);
            }

            if (config.Host?.TrimToNull() != null)
                builder = builder.SetBaseUrl(new Uri(config.Host));

            if (auth != null)
            {
                builder = builder.SetTokenProvider(token => auth.GetToken(token));
            }

            if (config.SdkLogging != null && !config.SdkLogging.Disable && logger != null)
            {
                builder = builder
                    .SetLogLevel(config.SdkLogging.Level)
                    .SetLogFormat(config.SdkLogging.Format)
                    .SetLogger(logger);
            }

            if (metrics != null)
            {
                builder = builder
                    .SetMetrics(metrics);
            }

            return builder;
        }
    }
}
