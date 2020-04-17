using System.Linq;
using System.Net.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System;
using System.Net.Http.Headers;
using Prometheus;
using System.Collections.Generic;
using Polly;
using Polly.Extensions.Http;
using System.Threading.Tasks;

namespace ExtractorUtils {
    /// <summary>
    /// Utility class for configuring <see href="https://prometheus.io/">Prometheus</see> for monitoring and metrics.
    /// A metrics server and multiple push gateway destinations can be configured according to <see cref="MetricsConfig"/>.
    /// </summary>
    public class MetricsService {
        private IHttpClientFactory _clientFactory;
        private MetricsConfig _config;
        private ILogger<MetricsService> _logger;
        private IList<MetricPusher> _pushers;
        private MetricServer _server;

        internal const string HttpClientName = "prometheus-httpclient";

        /// <summary>
        /// Initialized the metrics service with the given <paramref name="config"/> object.
        /// </summary>
        /// <param name="clientFactory">A pre-configured http client factory</param>
        /// <param name="config">Configuration object</param>
        /// <param name="logger">Logger</param>
        public MetricsService(IHttpClientFactory clientFactory, ILogger<MetricsService> logger, MetricsConfig config = null) {
            _clientFactory = clientFactory;
            _config = config;
            _logger = logger;
            _pushers = new List<MetricPusher>();
        }

        /// <summary>
        /// Starts a Prometheus server for scrape and multiple push gateway destinations, based on the configuration. 
        /// </summary>
        public void Start() {
            if (_config == null)
            {
                _logger.LogWarning("Metrics disabled: metrics configuration missing");
                return;
            }

            var pushGateways = _config.PushGateways;
            if (pushGateways != null && pushGateways.Any())
            {
                foreach (var gateway in pushGateways)
                {

                    HttpClient client = _clientFactory.CreateClient(HttpClientName);
                    var pusher = StartPusher(gateway, client);
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
                await Task.WhenAll(_pushers.Select(p => p.StopAsync()));
            }
            if (_server != null)
            {
                await _server.StopAsync();
            }
        }

        private MetricPusher StartPusher(PushGatewayConfig config, HttpClient client) {
            if (config.Host.TrimToNull() == null || config.Job.TrimToNull() == null)
            {
                _logger.LogWarning("Invalid metrics push destination (missing Host or Job)");
                return null;
            }

            _logger.LogInformation("Pushing metrics to {PushgatewayHost} with job name {PushgatewayJob}", config.Host, config.Job);

            if (config.Username.TrimToNull() != null && config.Password.TrimToNull() != null)
            {
                var headerValue = Convert.ToBase64String(
                        System.Text.Encoding
                            .GetEncoding("ISO-8859-1")
                            .GetBytes(config.Username + ":" + config.Password));
                client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", headerValue);
            }

            var uri = new Uri(config.Host);
            if (uri.Segments.Last() != "metrics" && uri.Segments.Last() != "metrics/") {
                uri = new Uri(uri, "metrics/");
            }

            var pusher = new MetricPusher(new MetricPusherOptions
            {
                Endpoint =  uri.ToString(),
                Job = config.Job,
                IntervalMilliseconds = config.PushInterval * 1_000L,
                HttpClientProvider = () => client
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

        private MetricServer StartServer(MetricsServerConfig config)
        {
            if (config.Host.TrimToNull() == null || config.Port <= 0)
            {
                _logger.LogWarning("Invalid metrics server (missing Host or Port)");
                return null;
            }
            var server = new MetricServer(hostname: config.Host, port: config.Port);
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
        public static void AddMetrics(this IServiceCollection services)
        {
            services.AddHttpClient(MetricsService.HttpClientName)
                .AddPolicyHandler(GetRetryPolicy());
            services.AddSingleton<MetricsService>();
        }

        static IAsyncPolicy<HttpResponseMessage> GetRetryPolicy()
        {
            return HttpPolicyExtensions
                .HandleTransientHttpError()
                .WaitAndRetryAsync(5, retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)));
        }
    }
}