using System;
using System.Collections.Generic;
using System.Dynamic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extensions;
using Cognite.Extractor.Common;
using Cognite.Extractor.StateStorage;
using Cognite.Extractor.Testing;
using Cognite.Extractor.Testing.Mock;
using Cognite.Extractor.Utils;
using CogniteSdk;
using Com.Cognite.V1.Timeseries.Proto;
using Google.Protobuf;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Moq;
using Moq.Protected;
using Newtonsoft.Json;
using Xunit;
using Xunit.Abstractions;
using TimeRange = Cognite.Extractor.Common.TimeRange;

namespace ExtractorUtils.Test.Unit
{
    public class DatapointsTest
    {
        private const string _project = "someProject";

        private readonly ITestOutputHelper _output;
        public DatapointsTest(ITestOutputHelper output)
        {
            _output = output;
        }

        [Fact]
        public async Task TestInsertDataPoints()
        {
            string path = "test-insert-data-points-config.yml";
            string[] lines = {  "version: 2",
                                "logger:",
                                "  console:",
                                "    level: verbose",
                                "cognite:",
                               $"  project: {_project}",
                               $"  host: https://test.cognitedata.com",
                                "  cdf-chunking:",
                                "    data-points: 4",
                                "    data-point-time-series: 2",
                                "  cdf-throttling:",
                                "    data-points: 2" };
            System.IO.File.WriteAllLines(path, lines);

            // Setup services
            var services = new ServiceCollection();
            services.AddConfig<BaseConfig>(path, 2);
            services.AddTestLogging(_output);
            CdfMock.RegisterHttpClient(services);
            services.AddCogniteClient("testApp");
            using (var provider = services.BuildServiceProvider())
            {
                var cogniteDestination = provider.GetRequiredService<CogniteDestination>();
                var mock = provider.GetRequiredService<CdfMock>();

                var timeseries = new TimeSeriesMock();
                mock.AddMatcher(timeseries.CreateDatapointsMatcher(Times.Exactly(4)));

                double[] doublePoints = { 0.0, 1.1, 2.2, double.NaN, 3.3, 4.4, double.NaN, 5.5, double.NegativeInfinity };
                string[] stringPoints = { "0", null, "1", new string('!', CogniteUtils.TimeSeriesStringBytesMax), new string('2', CogniteUtils.TimeSeriesStringBytesMax + 1), "3" };

                var datapoints = new Dictionary<Identity, IEnumerable<Datapoint>>() {
                    { new Identity("A"), new Datapoint[] { new Datapoint(DateTime.UtcNow, "1"), new Datapoint(DateTime.UtcNow, "2") }},
                    { new Identity(1), doublePoints.Select(d => new Datapoint(DateTime.UtcNow, d))},
                    { new Identity(2), stringPoints.Select(s => new Datapoint(DateTime.UtcNow, s))},
                    { new Identity(3), Array.Empty<Datapoint>() },
                    { new Identity(4), new Datapoint[] { new Datapoint(DateTime.MinValue, 1), new Datapoint(DateTime.MaxValue, 1)}}
                };
                timeseries.MockTimeSeries(new Identity("A"), true);
                timeseries.MockTimeSeries(new Identity(1), false);
                timeseries.MockTimeSeries(new Identity(2), true);

                var result = await cogniteDestination.InsertDataPointsAsync(
                    datapoints,
                    SanitationMode.Clean,
                    RetryMode.OnError,
                    CancellationToken.None);

                Assert.Equal(7, timeseries.GetTimeSeries(new Identity(1)).NumericDatapoints.Count);
                Assert.Equal(2, timeseries.GetTimeSeries(new Identity("A")).StringDatapoints.Count);
                Assert.DoesNotContain(timeseries.GetTimeSeries(new Identity(1)).NumericDatapoints,
                    dp => dp.NullValue || dp.Value == double.NaN || dp.Value == double.NegativeInfinity);
                Assert.Equal(6, timeseries.GetTimeSeries(new Identity(2)).StringDatapoints.Count);
                Assert.DoesNotContain(timeseries.GetTimeSeries(new Identity(2)).StringDatapoints,
                    dp => dp.NullValue || Encoding.UTF8.GetByteCount(dp.Value) > CogniteUtils.TimeSeriesStringBytesMax);

                mock.AssertAndClear();
                timeseries.Clear();
                mock.AddMatcher(timeseries.GetByIdsMatcher(Times.Exactly(2)));
                mock.AddMatcher(timeseries.CreateDatapointsMatcher(Times.Exactly(7)));

                timeseries.AutoMockTimeSeries("idNumeric1", "idNumeric2", "idString1", "idMismatchedString1", "idMismatched2");

                datapoints = new Dictionary<Identity, IEnumerable<Datapoint>>() {
                    { new Identity("idMissing1"), new Datapoint[] { new Datapoint(DateTime.UtcNow, "1")}},
                    { new Identity("idNumeric1"), new Datapoint[] { new Datapoint(DateTime.UtcNow, 1)}},
                    { new Identity("idNumeric2"), new Datapoint[] { new Datapoint(DateTime.UtcNow, 1)}},
                    { new Identity(-1), doublePoints.Select(d => new Datapoint(DateTime.UtcNow, d)).Take(2)},
                    { new Identity("idMismatchedString1"), new Datapoint[] { new Datapoint(DateTime.UtcNow, 1)}},
                    { new Identity("idString1"), new Datapoint[] { new Datapoint(DateTime.UtcNow, "1")}},
                    { new Identity("idMismatched2"), new Datapoint[] { new Datapoint(DateTime.UtcNow, "1")}}
                };
                result = await cogniteDestination.InsertDataPointsAsync(
                    datapoints,
                    SanitationMode.Clean,
                    RetryMode.OnError,
                    CancellationToken.None);

                var errs = result.Errors.ToArray();
                var notFoundErr = errs.Where(err => err.Resource == ResourceType.Id).SelectMany(err => err.Values);
                var mismatched = errs.Where(err => err.Type == ErrorType.MismatchedType).SelectMany(err => err.Skipped);

                var logger = provider.GetRequiredService<ILogger<DatapointsTest>>();
                foreach (var err in errs)
                {
                    logger.LogCogniteError(err, RequestType.CreateDatapoints, false);
                }
                Assert.Contains(new Identity("idMissing1"), notFoundErr);
                Assert.Contains(new Identity(-1), notFoundErr);
                Assert.Contains(mismatched.OfType<DataPointInsertError>(), err => err.Id.ExternalId == "idMismatchedString1");
                Assert.Contains(mismatched.OfType<DataPointInsertError>(), err => err.Id.ExternalId == "idMismatched2");

                Assert.Single(timeseries.GetTimeSeries("idNumeric1").NumericDatapoints);
                Assert.Single(timeseries.GetTimeSeries("idNumeric2").NumericDatapoints);
                Assert.Single(timeseries.GetTimeSeries("idString1").StringDatapoints);
            }

            System.IO.File.Delete(path);
        }

        [Fact]
        public async Task TestDeleteDataPoints()
        {
            string path = "test-delete-data-points-config.yml";
            string[] lines = {  "version: 2",
                                "logger:",
                                "  console:",
                                "    level: verbose",
                                "cognite:",
                               $"  project: {_project}",
                               $"  host: https://test.cognitedata.com",
                                "  cdf-chunking:",
                                "    data-point-delete: 4",
                                "    data-point-list: 2",
                                "  cdf-throttling:",
                                "    data-points: 2" };
            System.IO.File.WriteAllLines(path, lines);

            var mocks = TestUtilities.GetMockedHttpClientFactory(mockDeleteDataPointsAsync);
            var mockHttpMessageHandler = mocks.handler;
            var mockFactory = mocks.factory;

            // Setup services
            var services = new ServiceCollection();
            services.AddSingleton<IHttpClientFactory>(mockFactory.Object); // inject the mock factory
            services.AddConfig<BaseConfig>(path, 2);
            services.AddTestLogging(_output);
            services.AddCogniteClient("testApp");
            using (var provider = services.BuildServiceProvider())
            {
                var cogniteDestination = provider.GetRequiredService<CogniteDestination>();

                var ranges = new Dictionary<Identity, IEnumerable<TimeRange>>() {
                    { new Identity("A"), new TimeRange[] { new TimeRange(DateTime.UtcNow - TimeSpan.FromDays(2), DateTime.UtcNow),
                                                           new TimeRange(DateTime.UtcNow - TimeSpan.FromDays(4), DateTime.UtcNow - TimeSpan.FromDays(2))}},
                    { new Identity("B"), new TimeRange[] { new TimeRange(DateTime.UtcNow - TimeSpan.FromDays(2), DateTime.UtcNow)}},
                };

                var errors = await cogniteDestination.DeleteDataPointsIgnoreErrorsAsync(
                    ranges,
                    CancellationToken.None);

                mockHttpMessageHandler.Protected()
                .Verify<Task<HttpResponseMessage>>(
                    "SendAsync",
                    Times.Exactly(1), // 1 delete
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>());

                Assert.Empty(errors.IdsDeleteNotConfirmed);
                Assert.Empty(errors.IdsNotFound);

                ranges.Add(new Identity("missing-C"), new TimeRange[] { new TimeRange(DateTime.UtcNow - TimeSpan.FromDays(2), DateTime.UtcNow) });
                ranges.Add(new Identity("D"), new TimeRange[] { new TimeRange(DateTime.UtcNow - TimeSpan.FromDays(2), DateTime.UtcNow) });
                ranges.Add(new Identity("nc-E"), new TimeRange[] { new TimeRange(DateTime.UtcNow - TimeSpan.FromDays(2), DateTime.UtcNow) });
                ranges.Add(new Identity("missing-F"), new TimeRange[] { new TimeRange(DateTime.UtcNow - TimeSpan.FromDays(2), DateTime.UtcNow) });
                ranges.Add(new Identity("G"), new TimeRange[] { new TimeRange(DateTime.UtcNow - TimeSpan.FromDays(2), DateTime.UtcNow) });
                ranges.Add(new Identity("nc-H"), new TimeRange[] { new TimeRange(DateTime.UtcNow - TimeSpan.FromDays(2), DateTime.UtcNow) });

                errors = await cogniteDestination.DeleteDataPointsIgnoreErrorsAsync(
                    ranges,
                    CancellationToken.None);

                Assert.Contains(new Identity("missing-C"), errors.IdsNotFound);
                Assert.Contains(new Identity("missing-F"), errors.IdsNotFound);
            }

            System.IO.File.Delete(path);
        }
        [Fact]
        public async Task TestUploadQueue()
        {
            string path = "test-ts-queue-config.yml";
            string[] lines = {  "version: 2",
                                "logger:",
                                "  console:",
                                "    level: verbose",
                                "cognite:",
                               $"  project: {_project}",
                               $"  host: https://test.cognitedata.com",
                                "  cdf-chunking:",
                                "    data-points: 4",
                                "    data-point-time-series: 2",
                                "  cdf-throttling:",
                                "    data-points: 2"};
            System.IO.File.WriteAllLines(path, lines);

            var services = new ServiceCollection();
            services.AddConfig<BaseConfig>(path, 2);
            services.AddTestLogging(_output);
            CdfMock.RegisterHttpClient(services);
            services.AddCogniteClient("testApp", setLogger: true, setMetrics: false);
            var index = 0;

            Func<int, Dictionary<Identity, Datapoint>> uploadGenerator = (int i) => new Dictionary<Identity, Datapoint>() {
                    { new Identity("idMissing1"), new Datapoint(DateTime.UtcNow, i.ToString())},
                    { new Identity("idNumeric1"), new Datapoint(DateTime.UtcNow, i) },
                    { new Identity("idNumeric2"), new Datapoint(DateTime.UtcNow, i) },
                    { new Identity("idMismatchedString1"), new Datapoint(DateTime.UtcNow, i) },
                    { new Identity("idString1"), new Datapoint(DateTime.UtcNow, i.ToString()) }
                };

            var states = new[]
            {
                    new BaseExtractionState("idNumeric1"),
                    new BaseExtractionState("idNumeric2"),
                    new BaseExtractionState("idString1")
            };
            foreach (var state in states) state.InitExtractedRange(CogniteTime.DateTimeEpoch, CogniteTime.DateTimeEpoch);
            var stateMap = states.ToDictionary(state => Identity.Create(state.Id));

            var stateStore = new DummyExtractionStore();

            int dpCount = 0;
            int cbCount = 0;

            using (var source = new CancellationTokenSource())
            using (var provider = services.BuildServiceProvider())
            {
                var cogniteDestination = provider.GetRequiredService<CogniteDestination>();
                var logger = provider.GetRequiredService<ILogger<DatapointsTest>>();
                var mock = provider.GetRequiredService<CdfMock>();

                var timeseries = new TimeSeriesMock();
                timeseries.AutoMockTimeSeries("idNumeric1", "idNumeric2", "idString1", "idMismatchedString1");
                mock.AddMatcher(timeseries.CreateDatapointsMatcher(Times.AtLeast(10)));
                mock.AddMatcher(timeseries.GetByIdsMatcher(Times.AtLeast(1)));
                // queue with 1 sec upload interval
                await using (var queue = cogniteDestination.CreateTimeSeriesUploadQueue(TimeSpan.FromSeconds(1), 0, res =>
                {
                    dpCount += res.Uploaded?.Count() ?? 0;
                    cbCount++;
                    return Task.CompletedTask;
                }))
                {
                    queue.AddStateStorage(stateMap, stateStore, "test-states");
                    var enqueueTask = Task.Run(async () =>
                    {
                        while (index < 13)
                        {
                            var dps = uploadGenerator(index);
                            foreach (var kvp in dps) queue.Enqueue(kvp.Key, kvp.Value);
                            await Task.Delay(100, source.Token);
                            index++;
                        }
                    });
                    var uploadTask = queue.Start(source.Token);

                    var t = Task.WhenAny(uploadTask, enqueueTask);
                    await t;
                    logger.LogInformation("Enqueueing task completed. Disposing of the upload queue");
                }
                logger.LogInformation("Upload queue disposed");

                Assert.Equal(3 * 13, dpCount);
                Assert.True(cbCount >= 2, $"Got {cbCount}");
                cbCount = 0;

                // queue with maximum size
                await using (var queue = cogniteDestination.CreateTimeSeriesUploadQueue(TimeSpan.FromMinutes(10), 5, res =>
                {
                    dpCount += res.Uploaded?.Count() ?? 0;
                    cbCount++;
                    return Task.CompletedTask;
                }))
                {
                    queue.AddStateStorage(stateMap, stateStore, "test-states");
                    var enqueueTask = Task.Run(async () =>
                    {
                        while (index < 23)
                        {
                            var dps = uploadGenerator(index);
                            foreach (var kvp in dps) queue.Enqueue(kvp.Key, kvp.Value);
                            await Task.Delay(100, source.Token);
                            index++;
                        }
                    });
                    var uploadTask = queue.Start(source.Token);

                    await enqueueTask;

                    // test cancelling the token;
                    source.Cancel();
                    await uploadTask;
                    Assert.True(uploadTask.IsCompleted);
                    logger.LogInformation("Enqueueing task cancelled. Disposing of the upload queue");
                }
                logger.LogInformation("Upload queue disposed");

                Assert.Equal(3 * 23, dpCount);
                Assert.True(cbCount >= 10 && cbCount <= 14);
                foreach (var state in states)
                {
                    Assert.NotEqual(CogniteTime.DateTimeEpoch, state.DestinationExtractedRange.Last);
                }
            }

            System.IO.File.Delete(path);
        }
        [Fact]
        public async Task TestUploadQueueBuffer()
        {
            string path = "test-ts-queue-buffer-config.yml";
            string[] lines = {  "version: 2",
                                "logger:",
                                "  console:",
                                "    level: verbose",
                                "cognite:",
                               $"  project: {_project}",
                               $"  host: https://test.cognitedata.com",
                                "  cdf-chunking:",
                                "    data-points: 4",
                                "    data-point-time-series: 2",
                                "  cdf-throttling:",
                                "    data-points: 2",
                                "  cdf-retries:",
                                "    max-retries: 0"};
            System.IO.File.WriteAllLines(path, lines);

            var services = new ServiceCollection();
            services.AddConfig<BaseConfig>(path, 2);
            services.AddTestLogging(_output);
            CdfMock.RegisterHttpClient(services);
            services.AddCogniteClient("testApp", setLogger: true, setMetrics: false);

            Func<int, Dictionary<Identity, Datapoint>> uploadGenerator = (int i) => new Dictionary<Identity, Datapoint>() {
                    { new Identity("idMissing1"), new Datapoint(DateTime.UtcNow, i.ToString())},
                    { new Identity("idNumeric1"), new Datapoint(DateTime.UtcNow, i) },
                    { new Identity("idNumeric2"), new Datapoint(DateTime.UtcNow, i) },
                    { new Identity("idMismatchedString1"), new Datapoint(DateTime.UtcNow, i) },
                    { new Identity("idString1"), new Datapoint(DateTime.UtcNow, i.ToString()) }
                };

            System.IO.File.Create("dp-buffer.bin").Close();

            using (var source = new CancellationTokenSource())
            using (var provider = services.BuildServiceProvider())
            {
                var cogniteDestination = provider.GetRequiredService<CogniteDestination>();
                var logger = provider.GetRequiredService<ILogger<DatapointsTest>>();
                var mock = provider.GetRequiredService<CdfMock>();
                var timeseries = new TimeSeriesMock();
                timeseries.AutoMockTimeSeries(
                    "idNumeric1", "idNumeric2", "idString1", "idMismatchedString1"
                );
                // Since we have a chunk size of 4, with 2 timeseries per request, and we write
                // 50 datapoints, we expect 13 requests, but we get two repeats due to the mismatched timeseries.
                mock.AddMatcher(timeseries.CreateDatapointsMatcher(Times.Exactly(15)));
                mock.AddMatcher(timeseries.GetByIdsMatcher(Times.Exactly(3)));
                mock.AddTokenInspectEndpoint(Times.AtLeastOnce(), _project);

                // queue that will not upload automatically.
                await using (var queue = cogniteDestination.CreateTimeSeriesUploadQueue(TimeSpan.Zero, 0, null, "dp-buffer.bin"))
                {
                    var _ = queue.Start(CancellationToken.None);
                    for (int i = 0; i < 10; i++)
                    {
                        var dps = uploadGenerator(i);
                        foreach (var kvp in dps) queue.Enqueue(kvp.Key, kvp.Value);
                    }
                    mock.RejectAllMessages = true;
                    Assert.Equal(0, new FileInfo("dp-buffer.bin").Length);
                    await queue.Trigger(CancellationToken.None);
                    Assert.True(new FileInfo("dp-buffer.bin").Length > 0);
                    Assert.Equal(0, timeseries.All.Sum(ts => ts.NumericDatapoints.Count + ts.StringDatapoints.Count));
                    await queue.Trigger(CancellationToken.None);
                    Assert.True(new FileInfo("dp-buffer.bin").Length > 0);
                    Assert.Equal(0, timeseries.All.Sum(ts => ts.NumericDatapoints.Count + ts.StringDatapoints.Count));
                    mock.RejectAllMessages = false;
                    await queue.Trigger(CancellationToken.None);
                    Assert.Equal(0, new FileInfo("dp-buffer.bin").Length);
                    Assert.Equal(10, timeseries.GetTimeSeries("idNumeric1").NumericDatapoints.Count);
                    logger.LogInformation("Disposing of the upload queue");
                }
                logger.LogInformation("Upload queue disposed");

            }

            System.IO.File.Delete("dp-buffer.bin");
            System.IO.File.Delete(path);
        }

        [Fact]
        public void TestToInsertRequest()
        {
            var dps = new Dictionary<Identity, IEnumerable<Datapoint>> {
                { Identity.Create("dp-double"), new[] {
                    new Datapoint(DateTime.UtcNow, 123, StatusCode.Parse("Bad")),
                    new Datapoint(DateTime.UtcNow, 123, StatusCode.Parse("Good")),
                    new Datapoint(DateTime.UtcNow, 123),
                    new Datapoint(DateTime.UtcNow, false),
                    new Datapoint(DateTime.UtcNow, "foo"),
                }}, { Identity.Create("dp-string"), new[] {
                    new Datapoint(DateTime.UtcNow, "foo", StatusCode.Parse("Bad")),
                    new Datapoint(DateTime.UtcNow, "foo", StatusCode.Parse("Good")),
                    new Datapoint(DateTime.UtcNow, "foo"),
                    new Datapoint(DateTime.UtcNow, true),
                    new Datapoint(DateTime.UtcNow, 123),
                }}
            };

            var req = dps.ToInsertRequest();
            var byId = req.Items.ToDictionary(r => r.ExternalId);
            var c1 = byId["dp-double"];
            Assert.Equal(4, c1.NumericDatapoints.Datapoints.Count);
            var d1 = c1.NumericDatapoints.Datapoints;
            Assert.Equal(StatusCode.Parse("Bad").Code, (ulong)d1[0].Status.Code);
            Assert.Equal(123, d1[0].Value);
            Assert.False(d1[0].NullValue);
            Assert.Equal(StatusCode.Parse("Good").Code, (ulong)d1[1].Status.Code);
            Assert.Equal(123, d1[1].Value);
            Assert.False(d1[1].NullValue);
            Assert.Equal(StatusCode.Parse("Good").Code, (ulong)d1[2].Status.Code);
            Assert.Equal(123, d1[2].Value);
            Assert.False(d1[2].NullValue);
            Assert.Equal(StatusCode.Parse("Bad").Code, (ulong)d1[3].Status.Code);
            Assert.Equal(0, d1[3].Value);
            Assert.True(d1[3].NullValue);

            var c2 = byId["dp-string"];
            Assert.Equal(4, c2.StringDatapoints.Datapoints.Count);
            var d2 = c2.StringDatapoints.Datapoints;
            Assert.Equal(StatusCode.Parse("Bad").Code, (ulong)d2[0].Status.Code);
            Assert.Equal("foo", d2[0].Value);
            Assert.False(d2[0].NullValue);
            Assert.Equal(StatusCode.Parse("Good").Code, (ulong)d2[1].Status.Code);
            Assert.Equal("foo", d2[1].Value);
            Assert.False(d2[1].NullValue);
            Assert.Equal(StatusCode.Parse("Good").Code, (ulong)d2[2].Status.Code);
            Assert.Equal("foo", d2[2].Value);
            Assert.False(d2[2].NullValue);
            Assert.Equal(StatusCode.Parse("Bad").Code, (ulong)d2[3].Status.Code);
            Assert.Equal("", d2[3].Value);
            Assert.True(d2[3].NullValue);
        }

        #region mock
        private static async Task<HttpResponseMessage> mockDeleteDataPointsAsync(HttpRequestMessage message, CancellationToken token)
        {
            var uri = message.RequestUri.ToString();
            HttpContent responseBody = new StringContent("{ }");
            var statusCode = HttpStatusCode.OK;

            var content = await message.Content.ReadAsStringAsync(token);
            var ids = JsonConvert.DeserializeObject<dynamic>(content);
            IEnumerable<dynamic> items = ids.items;

            if (uri.Contains($"{_project}/timeseries/data/list"))
            {
                Assert.True(items.Count() <= 2);
                DataPointListResponse dpList = new DataPointListResponse();
                foreach (var item in items)
                {
                    string id = item.externalId;
                    var dp = new DataPointListItem();
                    dp.ExternalId = id;
                    dp.NumericDatapoints = new NumericDatapoints();
                    dpList.Items.Add(dp);
                }
                using (MemoryStream stream = new MemoryStream())
                {
                    dpList.WriteTo(stream);
                    responseBody = new ByteArrayContent(stream.ToArray());
                }
            }
            else
            {
                Assert.Contains($"{_project}/timeseries/data/delete", uri);

                dynamic missingData = new ExpandoObject();
                missingData.error = new ExpandoObject();
                missingData.error.code = 400;
                missingData.error.message = "Ids not found";
                missingData.error.missing = new List<ExpandoObject>();

                Assert.True(items.Count() <= 4);
                foreach (var item in items)
                {
                    string id = item.externalId;
                    if (id.StartsWith("missing"))
                    {
                        dynamic missingId = new ExpandoObject();
                        missingId.externalId = id;
                        missingData.error.missing.Add(missingId);
                    }

                }
                if (missingData.error.missing.Count > 0)
                {
                    responseBody = new StringContent(JsonConvert.SerializeObject(missingData));
                    statusCode = HttpStatusCode.BadRequest;
                }
            }

            var response = new HttpResponseMessage
            {
                StatusCode = statusCode,
                Content = responseBody
            };
            response.Content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
            response.Headers.Add("x-request-id", "1");
            return response;
        }
        #endregion
    }
}