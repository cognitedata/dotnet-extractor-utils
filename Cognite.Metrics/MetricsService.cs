using System.Net;
using System.Threading;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Polly;
using Polly.Extensions.Http;
using Polly.Timeout;
using Prometheus;
using Cognite.Extractor.Common;

namespace Cognite.Extractor.Metrics 
{
    /// <summary>
    /// Utility class for configuring <see href="https://prometheus.io/">Prometheus</see> for monitoring and metrics.
    /// A metrics server and multiple push gateway destinations can be configured according to <see cref="MetricsConfig"/>.
    /// </summary>
    public class MetricsService 
    {
        internal const string HttpClientName = "prometheus-httpclient";
        private readonly IHttpClientFactory _clientFactory;
        private readonly MetricsConfig? _config;
        private readonly ILogger<MetricsService> _logger;
        private readonly IList<MetricPusher> _pushers;
        private MetricServer? _server;

        /// <summary>
        /// Initialized the metrics service with the given <paramref name="config"/> object.
        /// </summary>
        /// <param name="clientFactory">A pre-configured http client factory</param>
        /// <param name="config">Configuration object</param>
        /// <param name="logger">Logger</param>
        public MetricsService(IHttpClientFactory clientFactory, ILogger<MetricsService> logger, MetricsConfig? config = null) {
            _clientFactory = clientFactory;
            _config = config;
            _logger = logger;
            _pushers = new List<MetricPusher>();
        }

        /// <summary>
        /// Starts a Prometheus server for scrape and multiple push gateway destinations, based on the configuration. 
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Reliability", "CA2000: Dispose objects before losing scope", Justification = "StopAsync() will dispose of the pusher")]        
        public void Start() {
            if (_config == null || (_config.PushGateways == null && _config.Server == null))
            {
                _logger.LogWarning("Metrics disabled: metrics configuration missing");
                return;
            }

            var pushGateways = _config.PushGateways;
            if (pushGateways != null && pushGateways.Any())
            {
                foreach (var gateway in pushGateways)
                {
                    var pusher = StartPusher(gateway);
                    if (pusher != null)
                    {
                        _pushers.Add(pusher);
                    }
                }
            }

            if (_config.Server != null) {
                _server = StartServer(_config.Server);
            }
        }

        /// <summary>
        /// Stops the metrics service.
        /// </summary>
        /// <returns></returns>
        public async Task Stop() {
            if (_pushers.Any())
            {
                await Task.WhenAll(_pushers.Select(p => p.StopAsync())).ConfigureAwait(false);
                _pushers.Clear();
            }
            if (_server != null)
            {
                await _server.StopAsync().ConfigureAwait(false);
            }
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Reliability", "CA2000: Dispose objects before losing scope", Justification = "StopAsync() will dispose of the pusher")]
        private MetricPusher? StartPusher(PushGatewayConfig config) {
            if (config.Host.TrimToNull() == null || config.Job.TrimToNull() == null)
            {
                _logger.LogWarning("Invalid metrics push destination (missing Host or Job)");
                return null;
            }

            _logger.LogInformation("Pushing metrics to {PushgatewayHost} with job name {PushgatewayJob}", config.Host, config.Job);

            var uri = new Uri(config.Host);
            if (uri.Segments.Last() != "metrics" && uri.Segments.Last() != "metrics/") {
                uri = new Uri(uri, "metrics/");
            }

            var pusher = new MetricPusher(new MetricPusherOptions
            {
                Endpoint =  uri.ToString(),
                Job = config.Job,
                IntervalMilliseconds = config.PushInterval * 1_000L,
                HttpClientProvider = () => CreateClient(config),
                OnError = (e) => {
                    if (e is TimeoutRejectedException)
                    {
                        _logger.LogError("Metrics push attempt timed out after retrying");
                    }
                    else
                    {
                        _logger.LogError("Metrics push error: " + e.Message);
                    }
                },
            });
            try 
            {
                pusher.Start();
            }
            catch (InvalidOperationException e)
            {
                _logger.LogWarning("Could not start metrics pusher to {PushgatewayHost}: {Message}", config.Host, e.Message);
                return null;
            }
            return pusher;
        }

        private HttpClient CreateClient(PushGatewayConfig config)
        {
            var client = _clientFactory.CreateClient(HttpClientName);
            if (config.Username.TrimToNull() != null && config.Password.TrimToNull() != null)
            {
                var headerValue = Convert.ToBase64String(
                        System.Text.Encoding
                            .GetEncoding("ISO-8859-1")
                            .GetBytes(config.Username + ":" + config.Password));
                client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", headerValue);
            }

            return client;
        }

        private MetricServer? StartServer(MetricsServerConfig config)
        {
            if (config.Host.TrimToNull() == null || config.Port <= 0)
            {
                _logger.LogWarning("Invalid metrics server (missing Host or Port)");
                return null;
            }
            var server = new MetricServer(hostname: config.Host!, port: config.Port);
            server.Start();
            _logger.LogInformation("Metrics server started at {MetricsServerHost}:{MetricsServerPort}", config.Host, config.Port);
            return server;
        }
    }

    /// <summary>
    /// Extension utilities for metrics.
    /// </summary>
    public static class MetricsExtensions {
        
        /// <summary>
        /// Adds a configured metrics service to the <paramref name="services"/> collection
        /// Also adds a named <see cref="IHttpClientFactory"/> to be used by the push gateways, with
        /// simple retry policy to handle transient http errors configured (5 retries with exponential backoff).
        /// </summary>
        /// <param name="services"></param>
        /// <param name="pushTimeout">Timeout in milliseconds for each push attempt</param>
        public static void AddMetrics(this IServiceCollection services, int pushTimeout = 80_000)
        {
            services.AddHttpClient(MetricsService.HttpClientName, c => c.Timeout = Timeout.InfiniteTimeSpan)
                .AddPolicyHandler((p, m) => GetRetryPolicy(p.GetRequiredService<ILogger<MetricServer>>()))
                // The Prometheus client may silently terminate on OperationCanceledException.
                // This timeout policy will produce a TimeoutRejectException instead. 
                .AddPolicyHandler(Policy.TimeoutAsync<HttpResponseMessage>(TimeSpan.FromMilliseconds(pushTimeout)));
            services.AddSingleton<MetricsService>();
        }

        static IAsyncPolicy<HttpResponseMessage> GetRetryPolicy(ILogger logger)
        {
            return HttpPolicyExtensions
                .HandleTransientHttpError()
                .Or<TimeoutRejectedException>()
                .WaitAndRetryAsync(
                    3, // Don't need to retry too many times, as data will be pushed in the next push interval.
                    retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)),
                    (ex, ts) => {
                        if (ex.Result != null)
                        {
                            logger.LogWarning("{Method} {Uri} failed with status code: {Code}. Retrying in {Time} s. {Message}",
                                ex.Result.RequestMessage?.Method, ex.Result.RequestMessage?.RequestUri,
                                (int)ex.Result.StatusCode, ts.TotalSeconds, ex.Result.ReasonPhrase);
                        }
                        else if (ex.Exception != null)
                        {
                            logger.LogWarning("Metrics push attempt timed out or failed: {Message} Retrying in {Time} s.",
                                ex.Exception.Message, ts.TotalSeconds);
                            var inner = ex.Exception.InnerException;
                            while (inner != null)
                            {
                                logger.LogDebug("Inner exception: {Message}", inner.Message);
                                inner = inner.InnerException;
                            }
                        }
                    });
        }
    }
}
