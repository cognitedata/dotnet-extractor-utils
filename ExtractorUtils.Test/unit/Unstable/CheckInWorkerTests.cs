using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extensions.Unstable;
using Cognite.Extractor.Utils.Unstable;
using Cognite.Extractor.Utils.Unstable.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Newtonsoft.Json;
using Xunit;
using Xunit.Abstractions;
using Cognite.Extensions;
using Cognite.Extractor.Testing;
using Cognite.Extractor.Configuration;
using Cognite.Extractor.Utils;
using Cognite.Extractor.Utils.Unstable.Tasks;
using Microsoft.Extensions.Logging;
using CogniteSdk.Alpha;
using CogniteSdk;


namespace ExtractorUtils.Test.Unit.Unstable
{
    public class CheckInWorkerTests
    {
        private List<dynamic> taskEvents = new();
        private List<dynamic> errors = new();

        private int? _lastConfigRevision;

        private readonly ITestOutputHelper _output;
        private int _checkInCount;
        public CheckInWorkerTests(ITestOutputHelper output)
        {
            _output = output;
        }


        private ConnectionConfig GetConfig()
        {
            return new ConnectionConfig
            {
                Project = "project",
                BaseUrl = "https://greenfield.cognitedata.com",
                Integration = "test-integration",
                Authentication = new ClientCredentialsConfig
                {
                    ClientId = "someId",
                    ClientSecret = "thisIsASecret",
                    Scopes = new Cognite.Common.ListOrSpaceSeparated("https://greenfield.cognitedata.com/.default"),
                    MinTtl = "60s",
                    Resource = "resource",
                    Audience = "audience",
                    TokenUrl = "http://example.url/token",
                }
            };
        }

        private (ServiceProvider, CheckInWorker) GetCheckInWorker()
        {
            var config = GetConfig();

            var services = new ServiceCollection();
            services.AddConfig(config, typeof(ConnectionConfig));
            var mocks = TestUtilities.GetMockedHttpClientFactory(mockCheckInAsync);
            var mockHttpMessageHandler = mocks.handler;
            var mockFactory = mocks.factory;
            services.AddSingleton(mockFactory.Object);
            services.AddTestLogging(_output);
            DestinationUtilsUnstable.AddCogniteClient(services, "myApp", null, setLogger: true, setMetrics: true, setHttpClient: true);
            var provider = services.BuildServiceProvider();

            var client = provider.GetRequiredService<Client>();

            return (provider, new CheckInWorker(config.Integration, provider.GetRequiredService<ILogger<CheckInWorker>>(), client));
        }

        [Fact]
        public async Task TestCheckInWorker()
        {
            var (provider, checkIn) = GetCheckInWorker();
            using var p = provider;
            using var source = new CancellationTokenSource();
            // Check that this doesn't crash, and properly cancels out at the end.
            var runTask = checkIn.RunPeriodicCheckin(source.Token, Timeout.InfiniteTimeSpan);
            // First, we should very quickly report a checkin on the start of the run task...
            await TestUtils.WaitForCondition(() => _checkInCount == 1, 5);

            // Report an empty checkin.
            await checkIn.Flush(source.Token);
            Assert.Equal(2, _checkInCount);

            var start = DateTime.UtcNow;
            // Report a checkin with some task starts
            checkIn.ReportTaskStart("task1", null, start);
            checkIn.ReportTaskStart("task2", null, start);
            await checkIn.Flush(source.Token);
            Assert.Equal(3, _checkInCount);
            Assert.Equal(2, taskEvents.Count);
            Assert.Equal("task1", (string)taskEvents[0].name);
            Assert.Equal("started", (string)taskEvents[0].type);
            Assert.NotNull(taskEvents[0].timestamp);

            _lastConfigRevision = 1;

            // Report some errors
            checkIn.ReportError(new ExtractorError(ErrorLevel.warning, "test", checkIn, now: start.AddSeconds(1)));
            checkIn.ReportError(new ExtractorError(ErrorLevel.error, "test", checkIn, now: start.AddSeconds(1)));

            await checkIn.Flush(source.Token);
            Assert.Equal(4, _checkInCount);
            Assert.Equal(2, errors.Count);

            // Report some task ends
            checkIn.ReportTaskEnd("task1", null, start.AddSeconds(2));
            checkIn.ReportTaskEnd("task2", null, start.AddSeconds(2));
            await checkIn.Flush(source.Token);
            Assert.Equal(5, _checkInCount);
            Assert.Equal(4, taskEvents.Count);

            source.Cancel();
            await TestUtils.RunWithTimeout(runTask, 5);
        }

        [Fact]
        public async Task TestCheckInWorkerBatch()
        {
            var (provider, checkIn) = GetCheckInWorker();
            using var p = provider;
            using var source = new CancellationTokenSource();

            // Check that this doesn't crash, and properly cancels out at the end.
            var runTask = checkIn.RunPeriodicCheckin(source.Token, Timeout.InfiniteTimeSpan);
            // First, we should very quickly report a checkin on the start of the run task...
            await TestUtils.WaitForCondition(() => _checkInCount == 1, 5);

            // Lots of task updates.
            var start = DateTime.UtcNow;
            for (int i = 0; i < 1000; i++)
            {
                checkIn.ReportTaskStart("task1", null, start.AddSeconds(i));
                checkIn.ReportTaskEnd("task1", null, start.AddSeconds(i + 1));
            }
            // Add errors in wrong order. The one offset by 1 should be written first.
            checkIn.ReportError(new ExtractorError(ErrorLevel.error, "test", checkIn, now: start.AddSeconds(900)));
            checkIn.ReportError(new ExtractorError(ErrorLevel.warning, "test", checkIn, now: start.AddSeconds(1)));

            await checkIn.Flush(source.Token);
            Assert.Equal(3, _checkInCount);
            Assert.Equal(2000, taskEvents.Count);
            Assert.Equal(2, errors.Count);
            Assert.Equal(ErrorLevel.warning, (ErrorLevel)errors[0].level);
            Assert.Equal(ErrorLevel.error, (ErrorLevel)errors[1].level);

            // Lots of errors
            taskEvents.Clear();
            errors.Clear();
            for (int i = 0; i < 2000; i++)
            {
                checkIn.ReportError(new ExtractorError(ErrorLevel.error, "test", checkIn, now: start.AddSeconds(i)));
            }
            checkIn.ReportTaskStart("task1", null, start.AddSeconds(1));
            checkIn.ReportTaskEnd("task1", null, start.AddSeconds(1900));
            await checkIn.Flush(source.Token);
            Assert.Equal(5, _checkInCount);
            Assert.Equal(2, taskEvents.Count);
            Assert.Equal(2000, errors.Count);

            source.Cancel();
            await TestUtils.RunWithTimeout(runTask, 5);
        }


        private async Task<HttpResponseMessage> mockCheckInAsync(
            HttpRequestMessage message,
            CancellationToken token
        )
        {
            var uri = message.RequestUri.ToString();
            if (uri == "http://example.url/token")
            {
                var reply = "{" + Environment.NewLine +
                       $"  \"token_type\": \"Bearer\",{Environment.NewLine}" +
                       $"  \"expires_in\": 2,{Environment.NewLine}" +
                       $"  \"access_token\": \"token\"{Environment.NewLine}" +
                        "}";
                // Return 200
                var response = new HttpResponseMessage
                {
                    StatusCode = HttpStatusCode.OK,
                    Content = new StringContent(reply)
                };

                return response;
            }

            Assert.Contains("/integrations/checkin", uri);
            var content = await message.Content.ReadAsStringAsync(token);
            _output.WriteLine(content);
            var data = JsonConvert.DeserializeObject<dynamic>(content);
            Assert.Equal("test-integration", (string)data.externalId);

            if (data.taskEvents != null)
            {
                taskEvents.AddRange(data.taskEvents);
            }
            if (data.errors != null)
            {
                errors.AddRange(data.errors);
            }
            _checkInCount++;

            dynamic resData = new ExpandoObject();
            resData.lastConfigRevision = _lastConfigRevision;
            resData.externalId = "test-integration";

            var resBody = JsonConvert.SerializeObject(resData);

            var fresponse = new HttpResponseMessage
            {
                StatusCode = HttpStatusCode.OK,
                Content = new StringContent(resBody)
            };
            fresponse.Content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
            fresponse.Headers.Add("x-request-id", "1");

            return fresponse;
        }
    }
}