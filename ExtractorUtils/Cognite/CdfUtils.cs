using System;
using System.Net;
using System.Net.Http;
using System.Threading;
using CogniteSdk;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Polly;
using Polly.Extensions.Http;
using Polly.Timeout;

namespace ExtractorUtils
{
    public static class CdfUtils
    {
        public static Client.Builder Configure(this Client.Builder @this, CogniteConfig config, Authenticator auth, string appId)
        {
            var builder = @this
                .SetAppId(appId)
                .SetProject(config.Project);
                //.SetMetrics(_cdfMetrics);

            if (config.ApiKey.TrimToNull() != null)
            {
                builder = builder
                    .SetApiKey(config.ApiKey);
            }
            else if (auth != null)
            {
                builder = builder.SetTokenProvider(token => auth.GetToken(token));
            }

            if (config.Host.TrimToNull() != null)
                builder = builder.SetBaseUrl(new Uri(config.Host));

            return builder;
        }

    }
    public static class CdfExtensions
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

        public static void AddCogniteClient(this IServiceCollection services, string appId)
        {
            services.AddHttpClient<Client.Builder>(c => c.Timeout = Timeout.InfiniteTimeSpan)
                .AddPolicyHandler((provider, message) => { return GetRetryPolicy(provider.GetRequiredService<ILogger>()); })
                .AddPolicyHandler(GetTimeoutPolicy());
            services.AddHttpClient<Authenticator>();
            services.AddTransient(provider => {
                var cdfBuilder = provider.GetRequiredService<Client.Builder>();
                var conf = provider.GetRequiredService<BaseConfig>();
                var auth = conf.Cognite?.IdpAuthentication != null ? 
                    provider.GetRequiredService<Authenticator>() : null;
                return cdfBuilder.Configure(conf.Cognite, auth, appId).Build();
            });

        }
    }
}