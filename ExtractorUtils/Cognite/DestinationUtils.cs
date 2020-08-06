using System;
using System.Threading;
using Cognite.Extensions;
using Cognite.Extractor.Common;
using Cognite.Extractor.StateStorage;
using CogniteSdk;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Cognite.Extractor.Utils
{
    /// <summary>
    /// Utilities for the setting up a cognite destination.
    /// </summary>
    public static class DestinationUtils
    {
        /// <summary>
        /// Adds a configured Cognite client to the <paramref name="services"/> collection as a transient service
        /// </summary>
        /// <param name="services">The service collection</param>
        /// <param name="appId">Identifier of the application using the Cognite API</param>
        /// <param name="setLogger">If true, a <see cref="ILogger"/> logger is created and used by the client log calls to the 
        /// Cognite API (enabled in debug mode)</param>
        /// <param name="setMetrics">If true, a <see cref="IMetrics"/> metrics collector is created and used by the client
        /// to report metrics on the number and duration of API requests</param>
        /// <param name="setHttpClient">Default true. If false CogniteSdk Client.Builder is not added to the
        /// <see cref="ServiceCollection"/>. If this is false it must be added before this method is called.</param>
        public static void AddCogniteClient(this IServiceCollection services,
                                            string appId,
                                            bool setLogger = false,
                                            bool setMetrics = false,
                                            bool setHttpClient = true)
        {
            if (setHttpClient)
            {
                services.AddHttpClient<Client.Builder>(c => c.Timeout = Timeout.InfiniteTimeSpan)
                    .AddPolicyHandler((provider, message) =>
                    {
                        var retryConfig = provider.GetService<CogniteConfig>()?.CdfRetries;
                        return CogniteExtensions.GetRetryPolicy(provider.GetRequiredService<ILogger<Client>>(),
                            retryConfig?.MaxRetries, retryConfig?.MaxDelay);

                    })
                    .AddPolicyHandler((provider, message) =>
                    {
                        var retryConfig = provider.GetService<CogniteConfig>()?.CdfRetries;
                        return CogniteExtensions.GetTimeoutPolicy(retryConfig?.Timeout);
                    });
            }

            services.AddHttpClient<Authenticator>();
            services.AddSingleton<IMetrics, CdfMetricCollector>();
            services.AddTransient(provider => {
                var cdfBuilder = provider.GetRequiredService<Client.Builder>();
                var conf = provider.GetService<CogniteConfig>();
                var auth = conf?.IdpAuthentication != null ?
                    provider.GetRequiredService<Authenticator>() : null;
                var logger = setLogger ?
                    provider.GetRequiredService<ILogger<Client>>() : null;
                var metrics = setMetrics ?
                    provider.GetRequiredService<IMetrics>() : null;
                var client = cdfBuilder.Configure(conf, appId, auth, logger, metrics).Build();
                return client;
            });
            services.AddTransient<CogniteDestination>();
            services.AddTransient<IRawDestination, CogniteDestination>();
        }


        /// <summary>
        /// Configure a CogntieSdk Client.Builder according to the <paramref name="config"/> object
        /// </summary>
        /// <param name="clientBuilder">This builder</param>
        /// <param name="config">A <see cref="CogniteConfig"/> configuration object</param>
        /// <param name="appId">Identifier of the application using the Cognite API</param>
        /// <param name="auth">A <see cref="Authenticator"/> authenticator used to obtain bearer access token. 
        /// If null, API keys are used for authentication</param>
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
            string appId,
            Authenticator auth = null,
            ILogger<Client> logger = null,
            IMetrics metrics = null)
        {
            var builder = clientBuilder
                .SetAppId(appId);

            if (config?.Project?.TrimToNull() != null)
            {
                builder = builder.SetProject(config?.Project);
            }

            if (config?.Host?.TrimToNull() != null)
                builder = builder.SetBaseUrl(new Uri(config.Host));

            if (config?.ApiKey?.TrimToNull() != null)
            {
                builder = builder
                    .SetApiKey(config.ApiKey);
            }
            else if (auth != null)
            {
                builder = builder.SetTokenProvider(token => auth.GetToken(token));
            }

            if (config?.SdkLogging != null && !config.SdkLogging.Disable && logger != null)
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
