using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Moq;
using Moq.Protected;
using Prometheus;
using Xunit;

namespace ExtractorUtils.Test {
    public class MetricsTest {
        private static readonly Counter testCount = Metrics.CreateCounter("extractor_utils_test_count", "Counter used for unit testing.");
        private const string endpoint = @"http://localhost101:9091";
        private const string job = "unit-test-job";

        private static async Task<HttpResponseMessage> mockSendAsync(HttpRequestMessage message , CancellationToken token) {
            var content = await message.Content.ReadAsStringAsync();
            var auth = message.Headers.Authorization;

            // Verify endpoint, content and authentication
            Assert.Equal($@"{endpoint}/metrics/job/{job}", message.RequestUri.ToString());
            Assert.Equal(HttpMethod.Post, message.Method);
            Assert.Contains("TYPE extractor_utils_test_count counter", content);
            Assert.NotNull(auth);
            Assert.Equal("Basic", auth.Scheme);
            
            // Return 200
            var response = new HttpResponseMessage
            {
                StatusCode = HttpStatusCode.OK
            };
            return response;
        }

        [Fact]
        public static async Task TestMetricsAsync() {
            Metrics.SuppressDefaultMetrics();

            string path = "test-metrics-config.yml";
            string[] lines = {  "version: 2",
                                "logger:",
                                "  console:",
                                "    level: verbose",
                                "metrics:",
                                "  push-gateways:",
                               $"    - host: {endpoint}",
                               $"      job: {job}",
                                "      username: user",
                                "      password: pass",
                                "      push-interval: 1" };
            File.WriteAllLines(path, lines);

            // Mock http client factory
            var mockFactory = new Mock<IHttpClientFactory>();
            var mockHttpMessageHandler = new Mock<HttpMessageHandler>();
            mockHttpMessageHandler.Protected()
                .Setup<Task<HttpResponseMessage>>("SendAsync", 
                                                  ItExpr.IsAny<HttpRequestMessage>(), 
                                                  ItExpr.IsAny<CancellationToken>())
                .Returns<HttpRequestMessage, CancellationToken>(mockSendAsync);
            var client = new HttpClient(mockHttpMessageHandler.Object);
            mockFactory.Setup(_ => _.CreateClient(It.IsAny<string>())).Returns(client);
            
            // Setup services
            var services = new ServiceCollection();
            services.AddSingleton<IHttpClientFactory>(mockFactory.Object); // inject the mock factory
            services.AddConfig<BaseConfig>(path, 2);
            services.AddLogger();
            services.AddMetrics();

            using (var provider = services.BuildServiceProvider()) {
                var metrics = provider.GetRequiredService<MetricsService>();
                metrics.Start();
                testCount.Inc();
                await Task.Delay(1000);
                await metrics.Stop();
            }

            // Verify that the metrics are sent the prometheus endpoint at least once 
            mockHttpMessageHandler.Protected()
                .Verify<Task<HttpResponseMessage>>(
                    "SendAsync", 
                    Times.AtLeastOnce(), // push every second
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>());
            
            File.Delete(path);
        }


        private static Task<HttpResponseMessage> mockNoAssertSendAsync(HttpRequestMessage message , CancellationToken token) {
            return Task.FromResult(new HttpResponseMessage
            {
                StatusCode = HttpStatusCode.OK
            });
        }

        [Fact]
        public async Task TestMultipeGatewaysAsync() {
            Metrics.SuppressDefaultMetrics();
            string endpoint2 = @"http://localhost202:9091";
            string job2 = "unit-test-job2";

            string path = "test-multiple-gateways-config.yml";
            string[] lines = {  "version: 2",
                                "logger:",
                                "  console:",
                                "    level: verbose",
                                "metrics:",
                                "  push-gateways:",
                               $"    - host: {endpoint}",
                               $"      job: {job}",
                               $"    - host: {endpoint2}",
                               $"      job: {job2}" };
            File.WriteAllLines(path, lines);

            // Mock http client factory
            var mockFactory = new Mock<IHttpClientFactory>();
            var mockHttpMessageHandler = new Mock<HttpMessageHandler>();
            mockHttpMessageHandler.Protected()
                .Setup<Task<HttpResponseMessage>>("SendAsync", 
                                                  ItExpr.IsAny<HttpRequestMessage>(), 
                                                  ItExpr.IsAny<CancellationToken>())
                .Returns<HttpRequestMessage, CancellationToken>(mockNoAssertSendAsync);
            var client = new HttpClient(mockHttpMessageHandler.Object);
            mockFactory.Setup(_ => _.CreateClient(It.IsAny<string>())).Returns(client);
            
            // Setup services
            var services = new ServiceCollection();
            services.AddSingleton<IHttpClientFactory>(mockFactory.Object); // inject the mock factory
            services.AddConfig<BaseConfig>(path, 2);
            services.AddLogger();
            services.AddMetrics();

            using (var provider = services.BuildServiceProvider()) {
                var metrics = provider.GetRequiredService<MetricsService>();
                metrics.Start();
                testCount.Inc();
                await Task.Delay(1000);
                await metrics.Stop();
            }

            // Verify that the metrics are sent to both endpoints at least once 
            mockHttpMessageHandler.Protected()
                .Verify<Task<HttpResponseMessage>>(
                    "SendAsync", 
                    Times.AtLeastOnce(), // push every second
                    ItExpr.Is<HttpRequestMessage>(m => m.RequestUri.ToString() == $@"{endpoint}/metrics/job/{job}"),
                    ItExpr.IsAny<CancellationToken>());

            mockHttpMessageHandler.Protected()
                .Verify<Task<HttpResponseMessage>>(
                    "SendAsync", 
                    Times.AtLeastOnce(), // push every second
                    ItExpr.Is<HttpRequestMessage>(m => m.RequestUri.ToString() == $@"{endpoint2}/metrics/job/{job2}"),
                    ItExpr.IsAny<CancellationToken>());

            File.Delete(path);
        }

    }
}