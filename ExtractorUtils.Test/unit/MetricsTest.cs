using System;
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
using Cognite.Extractor.Logging;
using Cognite.Extractor.Metrics;
using Cognite.Extractor.Utils;
using System.Reflection;
using Xunit.Abstractions;
using Cognite.Extractor.Testing;

namespace ExtractorUtils.Test.Unit
{
    public class MetricsTest 
    {
        private readonly ITestOutputHelper _output;
        public MetricsTest(ITestOutputHelper output)
        {
            _output = output;
        }


        private static readonly Counter testCount = Metrics.CreateCounter("extractor_utils_test_count", "Counter used for unit testing.");
        private const string endpoint = @"http://localhost101:9091";
        private const string job = "unit-test-job";

        private static async Task<HttpResponseMessage> MockSendAsync(HttpRequestMessage message , CancellationToken token) {
            var content = await message.Content.ReadAsStringAsync(token);
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
        public async Task TestMetricsAsync() {
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
            var (factory, handler) = TestUtilities.GetMockedHttpClientFactory(MockSendAsync);
            
            // Setup services
            var services = new ServiceCollection();
            services.AddSingleton<IHttpClientFactory>(factory.Object); // inject the mock factory
            services.AddConfig<BaseConfig>(path, 2);
            services.AddTestLogging(_output);
            services.AddMetrics();

            using (var provider = services.BuildServiceProvider()) {
                var metrics = provider.GetRequiredService<MetricsService>();
                metrics.Start();
                testCount.Inc();
                await Task.Delay(1000);
                await metrics.Stop();
            }

            // Verify that the metrics are sent the prometheus endpoint at least once 
            handler.Protected()
                .Verify<Task<HttpResponseMessage>>(
                    "SendAsync", 
                    Times.AtLeastOnce(), // push every second
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>());
            
            File.Delete(path);
        }

        [Fact]
        public async Task TestDisableMetricsAsync() {
            Metrics.SuppressDefaultMetrics();

            string path = "test-disable-metrics-config.yml";
            string[] lines = {  "version: 2",
                                "logger:",
                                "  file:",
                                "    level: warning",
                               @"    path: metrics-logs/log.txt"};
            File.WriteAllLines(path, lines);
            
            // Setup services
            var services = new ServiceCollection();
            services.AddConfig<BaseConfig>(path, 2);
            services.AddTestLogging(_output);
            services.AddMetrics();

            using (var provider = services.BuildServiceProvider()) {
                var metrics = provider.GetRequiredService<MetricsService>();
                metrics.Start();
                await metrics.Stop();
            }

            var logfile = $@"metrics-logs/log{DateTime.Now:yyyyMMdd}.txt";
            Assert.True(File.Exists(logfile));
            using (StreamReader r = new StreamReader(logfile))
            {
                string line1 = r.ReadLine();
                Assert.Contains("Metrics disabled: metrics configuration missing", line1);
                Assert.Null(r.ReadLine());
            }
            Directory.Delete("metrics-logs", true);
            File.Delete(path);
        }

        [Fact]
        public async Task TestInvalidPushGatewayAsync() {
            Metrics.SuppressDefaultMetrics();

            string path = "test-invalid-pg-config.yml";
            string[] lines = {  "version: 2",
                                "metrics:",
                                "  push-gateways:",
                                "    - host: ",
                                "      job: ",
                                "logger:",
                                "  file:",
                                "    level: warning",
                               @"    path: pg-logs/log.txt"};
            File.WriteAllLines(path, lines);
            
            // Setup services
            var services = new ServiceCollection();
            services.AddConfig<BaseConfig>(path, 2);
            services.AddTestLogging(_output);
            services.AddMetrics();

            using (var provider = services.BuildServiceProvider()) {
                var metrics = provider.GetRequiredService<MetricsService>();
                metrics.Start();
                await metrics.Stop();
            }

            var logfile = $@"pg-logs/log{DateTime.Now:yyyyMMdd}.txt";
            Assert.True(File.Exists(logfile));
            using (StreamReader r = new StreamReader(logfile))
            {
                string line1 = r.ReadLine();
                Assert.Contains("Invalid metrics push destination", line1);
                Assert.Null(r.ReadLine());
            }
            Directory.Delete("pg-logs", true);
            File.Delete(path);
        }

        private static Task<HttpResponseMessage> MockNoAssertSendAsync(HttpRequestMessage message , CancellationToken token) {
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
            var (factory, handler) = TestUtilities.GetMockedHttpClientFactory(MockNoAssertSendAsync);
            
            // Setup services
            var services = new ServiceCollection();
            services.AddSingleton<IHttpClientFactory>(factory.Object); // inject the mock factory
            services.AddConfig<BaseConfig>(path, 2);
            services.AddTestLogging(_output);
            services.AddMetrics();

            using (var provider = services.BuildServiceProvider()) {
                var metrics = provider.GetRequiredService<MetricsService>();
                metrics.Start();
                testCount.Inc();
                await Task.Delay(1000);
                await metrics.Stop();
            }

            // Verify that the metrics are sent to both endpoints at least once 
            handler.Protected()
                .Verify<Task<HttpResponseMessage>>(
                    "SendAsync", 
                    Times.AtLeastOnce(), // push every second
                    ItExpr.Is<HttpRequestMessage>(m => m.RequestUri.ToString() == $@"{endpoint}/metrics/job/{job}"),
                    ItExpr.IsAny<CancellationToken>());

            handler.Protected()
                .Verify<Task<HttpResponseMessage>>(
                    "SendAsync", 
                    Times.AtLeastOnce(), // push every second
                    ItExpr.Is<HttpRequestMessage>(m => m.RequestUri.ToString() == $@"{endpoint2}/metrics/job/{job2}"),
                    ItExpr.IsAny<CancellationToken>());

            File.Delete(path);
        }

        [Fact]
        public void TestVersion()
        {
            var assembly = Assembly.GetExecutingAssembly();

            string version = Cognite.Extractor.Metrics.Version.GetVersion(assembly);
            string desc = Cognite.Extractor.Metrics.Version.GetDescription(assembly);
            // This test does not pass in CI, because github actions does not provide proper access
            // Assert.False(string.IsNullOrWhiteSpace(version));
            // Assert.False(string.IsNullOrWhiteSpace(desc));
            // Assert.NotEqual(version.Trim(), desc.Trim());
        }
    }
}