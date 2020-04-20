using System;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using CogniteSdk;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Polly;
using Polly.Extensions.Http;
using Polly.Timeout;

namespace ExtractorUtils
{
    /// <summary>
    /// Utility class for configuring a <see href="https://github.com/cognitedata/cognite-sdk-dotnet">Cognite SDK</see> client
    /// </summary>
    public static class CogniteUtils
    {
        /// <summary>
        /// Configure a <see cref="Client.Builder"/> according to the <paramref name="config"/> object
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
                .SetAppId(appId)
                .SetProject(config?.Project);

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

            if (logger != null) {
                // TODO: Make format and level properties in the configuration object
                builder = builder
                    .SetLogLevel(LogLevel.Debug) // Log as debug
                    .SetLogFormat("CDF ({Message}): {HttpMethod} {Url} - {Elapsed} ms")
                    .SetLogger(logger);
            }

            if (metrics != null) {
                builder = builder
                    .SetMetrics(metrics);
            }

            return builder;
        }
    }

    /// <summary>
    /// Exceptions produced by the Cognite utility classes
    /// </summary>
    public class CogniteUtilsException : Exception
    {
        /// <summary>
        /// Create a new Cognite utils exception with the given <paramref name="message"/>
        /// </summary>
        /// <param name="message">Message</param>
        /// <returns>A new <see cref="CogniteUtilsException"/> exception</returns>
        public CogniteUtilsException(string message) : base(message)
        {
        }

    }

    /// <summary>
    /// Extension utilities for the Cognite client
    /// </summary>
    public static class CogniteExtensions
    {
        static IAsyncPolicy<HttpResponseMessage> GetRetryPolicy(ILogger logger)
        {
            int numRetries = 5;
            return Policy
                .HandleResult<HttpResponseMessage>(msg => 
                    msg.StatusCode == HttpStatusCode.Unauthorized
                    || (int)msg.StatusCode == 429) //  HttpStatusCode.TooManyRequests not in .Net Framework, is in .Net Core 3.0
                .OrTransientHttpError()
                .Or<TimeoutRejectedException>()
                .WaitAndRetryAsync(
                    // retry interval 0.125, 0.25, 0.5, 1, 2, ..., i.e. max 0.125 * 2^numRetries
                    numRetries,
                    retry => TimeSpan.FromMilliseconds(125 * Math.Pow(2, Math.Min(retry - 1, numRetries))),
                    (ex, ts) =>
                    {
                        if (ex.Result != null)
                        {
                            logger.LogDebug("Failed request with status code: {Code}. Retrying in {Time} ms. {Message}",
                                (int) ex.Result.StatusCode, ts.TotalMilliseconds, ex.Result.ReasonPhrase);
                            logger.LogDebug("Failed request: {Method} {Uri}",
                                ex.Result.RequestMessage.Method,
                                ex.Result.RequestMessage.RequestUri);
                        }
                        else if (ex.Exception != null)
                        {
                            logger.LogDebug("Request timed out or failed with message: {Message} Retrying in {Time} ms.",
                                ex.Exception.Message, ts.TotalMilliseconds);
                            var inner = ex.Exception.InnerException;
                            while (inner != null)
                            {
                                logger.LogDebug("Inner exception: {Message}", inner.Message);
                                inner = inner.InnerException;
                            }
                        }
                    });
        }

        static IAsyncPolicy<HttpResponseMessage> GetTimeoutPolicy()
        {
            return Policy.TimeoutAsync<HttpResponseMessage>(TimeSpan.FromSeconds(80)); // timeout for each individual try
        }

        /// <summary>
        /// Adds a configured Cognite client to the <paramref name="services"/> collection as a transient service
        /// </summary>
        /// <param name="services">The service collection</param>
        /// <param name="appId">Identifier of the application using the Cognite API</param>
        /// <param name="setLogger">If true, a <see cref="ILogger"/> logger is created and used by the client log calls to the 
        /// Cognite API (enabled in debug mode)</param>
        /// <param name="setMetrics">If true, a <see cref="IMetrics"/> metrics collector is created and used by the client
        /// to report metrics on the number and duration of API requests</param>
        public static void AddCogniteClient(this IServiceCollection services, string appId, bool setLogger = false, bool setMetrics = false)
        {
            services.AddHttpClient<Client.Builder>(c => c.Timeout = Timeout.InfiniteTimeSpan)
                .AddPolicyHandler((provider, message) => { return GetRetryPolicy(provider.GetRequiredService<ILogger<Client>>()); })
                .AddPolicyHandler(GetTimeoutPolicy());
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

        }
    }
}