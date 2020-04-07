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
    public class MetricsPusher {
        private IHttpClientFactory _clientFactory;
        private MetricsConfig _config;
        private ILogger<MetricsPusher> _logger;
        private IList<MetricPusher> _pushers;

        internal const string HttpClientName = "prometheus-httpclient";

        public MetricsPusher(IHttpClientFactory clientFactory, BaseConfig config, ILogger<MetricsPusher> logger) {
            _clientFactory = clientFactory;
            _config = config.Metrics;
            _logger = logger;
            _pushers = new List<MetricPusher>();
        }

        public void Start() {
            if (_config == null) {
                _logger.LogWarning("Metrics disabled: metrics configuration missing");
                return;
            }

            var pushGateways = _config.PushGateways;
            if (pushGateways != null && pushGateways.Any()) {
                foreach (var gateway in pushGateways) {

                    HttpClient client = _clientFactory.CreateClient(HttpClientName);
                    var pusher = StartPusher(gateway, client);
                    _pushers.Add(pusher);
                }
            }
        }

        public async Task Stop() {
            if (_pushers.Any()) {
                await Task.WhenAll(_pushers.Select(p => p.StopAsync()));
            }
        }

        private MetricPusher StartPusher(PushGatewayConfig config, HttpClient client) {
            if (config.Host == null || config.Job == null)
            {
                _logger.LogWarning("Invalid metrics push destination (missing Host or Job)");
                return null;
            }

            _logger.LogInformation("Pushing metrics to {PushgatewayHost} with job name {PushgatewayJob}", config.Host, config.Job);

            if (config.Username.TrimToNull() != null && config.Password.TrimToNull() != null) {
                var headerValue = Convert.ToBase64String(
                        System.Text.Encoding
                            .GetEncoding("ISO-8859-1")
                            .GetBytes(config.Username + ":" + config.Password));
                client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", headerValue);
            }

            var pusher = new MetricPusher(new MetricPusherOptions
            {
                Endpoint =  config.Host,
                Job = config.Job,
                HttpClientProvider = () => client
            });
            try {
                pusher.Start();
            }
            catch (InvalidOperationException e) {
                _logger.LogWarning("Could not start metrics pusher to {PushgatewayHost}: {Message}", config.Host, e.Message);
                return null;
            }
            return pusher;
        } 
    }

    public static class MetricsExtensions {
        
        public static void AddMetrics(this IServiceCollection services)
        {
            services.AddHttpClient(MetricsPusher.HttpClientName)
                .AddPolicyHandler(GetRetryPolicy());
            services.AddSingleton<MetricsPusher>();
        }

        static IAsyncPolicy<HttpResponseMessage> GetRetryPolicy()
        {
            return HttpPolicyExtensions
                .HandleTransientHttpError()
                .WaitAndRetryAsync(5, retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)));
        }
    }
}