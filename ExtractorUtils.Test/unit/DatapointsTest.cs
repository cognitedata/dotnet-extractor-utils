using System;
using System.Collections.Generic;
using System.Dynamic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extensions;
using Cognite.Extractor.Common;
using Cognite.Extractor.Logging;
using Cognite.Extractor.StateStorage;
using Cognite.Extractor.Utils;
using Cognite.Extractor.Testing;
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
        private bool _failInsert = false;

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

            var mocks = TestUtilities.GetMockedHttpClientFactory(mockInsertDataPointsAsync);
            var mockHttpMessageHandler = mocks.handler;
            var mockFactory = mocks.factory;

            // Setup services
            var services = new ServiceCollection();
            services.AddSingleton<IHttpClientFactory>(mockFactory.Object); // inject the mock factory
            services.AddConfig<BaseConfig>(path, 2);
            services.AddTestLogging(_output);
            services.AddCogniteClient("testApp");
            using (var provider = services.BuildServiceProvider()) {
                var cogniteDestination = provider.GetRequiredService<CogniteDestination>();

                double[] doublePoints = { 0.0, 1.1, 2.2, double.NaN, 3.3, 4.4, double.NaN, 5.5, double.NegativeInfinity };
                string[] stringPoints = { "0", null, "1", new string('!', CogniteUtils.StringLengthMax), new string('2', CogniteUtils.StringLengthMax + 1), "3"};
                
                var datapoints = new Dictionary<Identity, IEnumerable<Datapoint>>() {
                    { new Identity("A"), new Datapoint[] { new Datapoint(DateTime.UtcNow, "1"), new Datapoint(DateTime.UtcNow, "2") }},
                    { new Identity(1), doublePoints.Select(d => new Datapoint(DateTime.UtcNow, d))},
                    { new Identity(2), stringPoints.Select(s => new Datapoint(DateTime.UtcNow, s))},
                    { new Identity(3), new Datapoint[] { } },
                    { new Identity(4), new Datapoint[] { new Datapoint(CogniteTime.DateTimeEpoch, 1), new Datapoint(DateTime.MaxValue, 1)}}
                };
                _createdDataPoints.Clear();
                var result = await cogniteDestination.InsertDataPointsAsync(
                    datapoints,
                    SanitationMode.Clean,
                    RetryMode.OnError,
                    CancellationToken.None);
                Assert.False(_createdDataPoints.ContainsKey(3 + "")); // No data points
                Assert.False(_createdDataPoints.ContainsKey(4 + "")); // Invalid timestamps
                Assert.Equal(7, _createdDataPoints[1 + ""].Count());
                Assert.Equal(2, _createdDataPoints["A"].Count());
                Assert.Empty(_createdDataPoints[1 + ""]
                    .Where(dp => dp.NumericValue == null || dp.NumericValue == double.NaN || dp.NumericValue == double.NegativeInfinity));
                Assert.Equal(6, _createdDataPoints[2 + ""].Count());
                Assert.Empty(_createdDataPoints[2 + ""]
                    .Where(dp => dp.StringValue == null || dp.StringValue.Length > CogniteUtils.StringLengthMax));

                _createdDataPoints.Clear();
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

                Assert.Contains(new Identity("idMissing1"), notFoundErr);
                Assert.Contains(new Identity(-1), notFoundErr);
                Assert.Contains(mismatched.OfType<DataPointInsertError>(), err => err.Id.ExternalId == "idMismatchedString1");
                Assert.Contains(mismatched.OfType<DataPointInsertError>(), err => err.Id.ExternalId == "idMismatched2");

                Assert.Single(_createdDataPoints["idNumeric1"]);
                Assert.Single(_createdDataPoints["idNumeric2"]);
                Assert.Single(_createdDataPoints["idString1"]);
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
            using (var provider = services.BuildServiceProvider()) {
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

                ranges.Add(new Identity("missing-C"), new TimeRange[] { new TimeRange(DateTime.UtcNow - TimeSpan.FromDays(2), DateTime.UtcNow)});
                ranges.Add(new Identity("D"), new TimeRange[] { new TimeRange(DateTime.UtcNow - TimeSpan.FromDays(2), DateTime.UtcNow)});
                ranges.Add(new Identity("nc-E"), new TimeRange[] { new TimeRange(DateTime.UtcNow - TimeSpan.FromDays(2), DateTime.UtcNow)});
                ranges.Add(new Identity("missing-F"), new TimeRange[] { new TimeRange(DateTime.UtcNow - TimeSpan.FromDays(2), DateTime.UtcNow)});
                ranges.Add(new Identity("G"), new TimeRange[] { new TimeRange(DateTime.UtcNow - TimeSpan.FromDays(2), DateTime.UtcNow)});
                ranges.Add(new Identity("nc-H"), new TimeRange[] { new TimeRange(DateTime.UtcNow - TimeSpan.FromDays(2), DateTime.UtcNow)});
                
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

            var mocks = TestUtilities.GetMockedHttpClientFactory(mockInsertDataPointsAsync);
            var mockHttpMessageHandler = mocks.handler;
            var mockFactory = mocks.factory;

            var services = new ServiceCollection();
            services.AddSingleton<IHttpClientFactory>(mockFactory.Object); // inject the mock factory
            services.AddConfig<BaseConfig>(path, 2);
            services.AddTestLogging(_output);
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
                // queue with 1 sec upload interval
                await using (var queue = cogniteDestination.CreateTimeSeriesUploadQueue(TimeSpan.FromSeconds(1), 0, res =>
                {
                    dpCount += res.Uploaded?.Count() ?? 0;
                    cbCount++;
                    return Task.CompletedTask;
                }))
                {
                    queue.AddStateStorage(stateMap, stateStore, "test-states");
                    var enqueueTask = Task.Run(async () => {
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
                    var enqueueTask = Task.Run(async () => {
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
                                "    data-points: 2"};
            System.IO.File.WriteAllLines(path, lines);

            var mocks = TestUtilities.GetMockedHttpClientFactory(mockInsertDataPointsAsync);
            var mockHttpMessageHandler = mocks.handler;
            var mockFactory = mocks.factory;

            var services = new ServiceCollection();
            services.AddSingleton<IHttpClientFactory>(mockFactory.Object); // inject the mock factory
            services.AddConfig<BaseConfig>(path, 2);
            services.AddTestLogging(_output);
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
                // queue that will not upload automatically.
                await using (var queue = cogniteDestination.CreateTimeSeriesUploadQueue(TimeSpan.Zero, 0, null, "dp-buffer.bin"))
                {
                    var _ = queue.Start(CancellationToken.None);
                    for (int i = 0; i < 10; i++)
                    {
                        var dps = uploadGenerator(i);
                        foreach (var kvp in dps) queue.Enqueue(kvp.Key, kvp.Value);
                    }
                    _failInsert = true;
                    Assert.Equal(0, new FileInfo("dp-buffer.bin").Length);
                    await queue.Trigger(CancellationToken.None);
                    Assert.True(new FileInfo("dp-buffer.bin").Length > 0);
                    Assert.Empty(_createdDataPoints);
                    await queue.Trigger(CancellationToken.None);
                    Assert.True(new FileInfo("dp-buffer.bin").Length > 0);
                    Assert.Empty(_createdDataPoints);
                    _failInsert = false;
                    await queue.Trigger(CancellationToken.None);
                    Assert.Equal(0, new FileInfo("dp-buffer.bin").Length);
                    Assert.Equal(3, _createdDataPoints.Count);
                    Assert.Equal(10, _createdDataPoints["idNumeric1"].Count);
                    logger.LogInformation("Disposing of the upload queue");
                }
                logger.LogInformation("Upload queue disposed");

            }

            System.IO.File.Delete("dp-buffer.bin");
            System.IO.File.Delete(path);
        }


        #region mock
        private Dictionary<string, List<Datapoint>> _createdDataPoints = new Dictionary<string, List<Datapoint>>();

        private async Task<HttpResponseMessage> mockInsertDataPointsAsync(HttpRequestMessage message , CancellationToken token) {
            var uri = message.RequestUri.ToString();

            var responseBody = "{ }";

            if (_failInsert)
            {
                dynamic failResponse = new ExpandoObject();
                failResponse.error = new ExpandoObject();
                failResponse.error.code = 500;
                failResponse.error.message = "Something went wrong";

                responseBody = JsonConvert.SerializeObject(failResponse);
                var fail = new HttpResponseMessage
                {
                    StatusCode = HttpStatusCode.InternalServerError,
                    Content = new StringContent(responseBody)
                };
                fail.Content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
                fail.Headers.Add("x-request-id", "1");
                return fail;
            }

            if (uri.Contains("/timeseries/byids"))
            {
                return await CogniteTest.MockEnsureTimeSeriesSendAsync(message, token);
            }
            Assert.Contains($"{_project}/timeseries/data", uri);

            var statusCode = HttpStatusCode.OK;
            var bytes = await message.Content.ReadAsByteArrayAsync();
            var data = DataPointInsertionRequest.Parser.ParseFrom(bytes);
            Assert.True(data.Items.Count <= 2); // data-points-time-series chunk size
            Assert.True(data.Items
                .Select(i => i.DatapointTypeCase == DataPointInsertionItem.DatapointTypeOneofCase.NumericDatapoints ?
                        i.NumericDatapoints.Datapoints.Count : i.StringDatapoints.Datapoints.Count)
                .Sum() <= 4); // data-points chunk size
            



            dynamic missingResponse = new ExpandoObject();
            missingResponse.error = new ExpandoObject();
            missingResponse.error.missing = new List<ExpandoObject>();
            missingResponse.error.code = 400;
            missingResponse.error.message = "Time series ids not found";

            dynamic mismatchedResponse = new ExpandoObject();
            mismatchedResponse.error = new ExpandoObject();
            mismatchedResponse.error.code = 400;
            mismatchedResponse.error.message = "";

            foreach (var item in data.Items)
            {
                if (item.Id < 0 || item.ExternalId.StartsWith("idMissing"))
                {
                    dynamic id = new ExpandoObject();
                    if (!string.IsNullOrEmpty(item.ExternalId)) id.externalId = item.ExternalId;
                    else id.id = item.Id;
                    missingResponse.error.missing.Add(id);
                }
                else if (item.ExternalId.StartsWith("idMismatched"))
                {
                    if (item.NumericDatapoints != null)
                    {
                        mismatchedResponse.error.message = "Expected string value for datapoint";
                    }
                    else {
                        mismatchedResponse.error.message = "Expected numeric value for datapoint";
                    }
                    break;
                }
            }
            if (!string.IsNullOrEmpty(mismatchedResponse.error.message))
            {
                statusCode = HttpStatusCode.BadRequest;
                responseBody = JsonConvert.SerializeObject(mismatchedResponse);
            }
            else if (missingResponse.error.missing.Count > 0) {
                statusCode = HttpStatusCode.BadRequest;
                responseBody = JsonConvert.SerializeObject(missingResponse);
            }
            else 
            {
                foreach (var item in data.Items)
                {
                    var sId = string.IsNullOrEmpty(item.ExternalId) ? item.Id + "" : item.ExternalId;
                    if (!_createdDataPoints.TryGetValue(sId, out List<Datapoint> dps))
                    {
                        dps = new List<Datapoint>();
                        _createdDataPoints.TryAdd(sId, dps);
                    }
                    if (item.NumericDatapoints != null)
                    {
                        foreach (var dp in item.NumericDatapoints.Datapoints)
                        {
                            dps.Add(new Datapoint(CogniteTime.FromUnixTimeMilliseconds(dp.Timestamp), dp.Value));
                        }
                    }
                    else if (item.StringDatapoints != null)
                    {
                        foreach (var dp in item.StringDatapoints?.Datapoints)
                        {
                            dps.Add(new Datapoint(CogniteTime.FromUnixTimeMilliseconds(dp.Timestamp), dp.Value));
                        }
                    }
                }
            }

            var response = new HttpResponseMessage
            {
                StatusCode = statusCode,
                Content = new StringContent(responseBody)               
            };
            response.Content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
            response.Headers.Add("x-request-id", "1");
            return response;
        }

        private static async Task<HttpResponseMessage> mockDeleteDataPointsAsync(HttpRequestMessage message , CancellationToken token) {
            var uri = message.RequestUri.ToString();
            HttpContent responseBody = new StringContent("{ }");
            var statusCode = HttpStatusCode.OK;

            var content = await message.Content.ReadAsStringAsync();
            var ids = JsonConvert.DeserializeObject<dynamic>(content);
            IEnumerable<dynamic> items = ids.items;

            if (uri.Contains($"{_project}/timeseries/data/list")) {
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
                using(MemoryStream stream = new MemoryStream())
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
                Content = responseBody              
            };
            response.Content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
            response.Headers.Add("x-request-id", "1");
            return response;
        }
        #endregion
    }
}