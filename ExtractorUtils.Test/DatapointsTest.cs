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
using Cognite.Extractor.Common;
using Cognite.Extractor.Logging;
using Cognite.Extractor.Utils;
using CogniteSdk;
using Com.Cognite.V1.Timeseries.Proto;
using Google.Protobuf;
using Microsoft.Extensions.DependencyInjection;
using Moq;
using Moq.Protected;
using Newtonsoft.Json;
using Xunit;
using TimeRange = Cognite.Extractor.Common.TimeRange;

namespace ExtractorUtils.Test
{
    public class DatapointsTest
    {
        private const string _project = "someProject";

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
                               $"  api-key: someKey",
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
            services.AddLogger();
            services.AddCogniteClient("testApp");
            using (var provider = services.BuildServiceProvider()) {
                var cogniteDestination = provider.GetRequiredService<CogniteDestination>();

                double[] doublePoints = { 0.0, 1.1, 2.2, double.NaN, 3.3, 4.4, double.NaN, 5.5, double.NegativeInfinity };
                string[] stringPoints = { "0", null, "1", new string('!', CogniteUtils.StringLengthMax), new string('2', CogniteUtils.StringLengthMax + 1), "3"};
                
                var datapoints = new Dictionary<Identity, IEnumerable<Datapoint>>() {
                    { new Identity("A"), new Datapoint[] { new Datapoint(DateTime.UtcNow, "1")}},
                    { new Identity("A"), new Datapoint[] { new Datapoint(DateTime.UtcNow, "2")}},
                    { new Identity(1), doublePoints.Select(d => new Datapoint(DateTime.UtcNow, d))},
                    { new Identity(2), stringPoints.Select(s => new Datapoint(DateTime.UtcNow, s))},
                    { new Identity(3), new Datapoint[] { } },
                    { new Identity(4), new Datapoint[] { new Datapoint(CogniteTime.DateTimeEpoch, 1), new Datapoint(DateTime.MaxValue, 1)}}
                };
                _createdDataPoints.Clear();
                await cogniteDestination.InsertDataPointsAsync(
                    datapoints,
                    CancellationToken.None);
                Assert.False(_createdDataPoints.ContainsKey(3 + "")); // No data points
                Assert.False(_createdDataPoints.ContainsKey(4 + "")); // Invalid timestamps
                Assert.Equal(6, _createdDataPoints[1 + ""].Count());
                Assert.Equal(2, _createdDataPoints["A"].Count());
                Assert.Empty(_createdDataPoints[1 + ""]
                    .Where(dp => dp.NumericValue == null || dp.NumericValue == double.NaN || dp.NumericValue == double.NegativeInfinity));
                Assert.Equal(5, _createdDataPoints[2 + ""].Count());
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
                var errors = await cogniteDestination.InsertDataPointsIgnoreErrorsAsync(
                    datapoints,
                    CancellationToken.None);
                var comparer = new IdentityComparer();
                Assert.Contains(new Identity("idMissing1"), errors.IdsNotFound, comparer);
                Assert.Contains(new Identity(-1), errors.IdsNotFound, comparer);
                Assert.Contains(new Identity("idMismatchedString1"), errors.IdsWithMismatchedData, comparer);
                Assert.Contains(new Identity("idMismatched2"), errors.IdsWithMismatchedData, comparer);
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
                               $"  api-key: someKey",
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
            services.AddLogger();
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
                    Times.Exactly(3), // 1 delete and two list
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
                Assert.Contains(new Identity("nc-E"), errors.IdsDeleteNotConfirmed);
                Assert.Contains(new Identity("nc-H"), errors.IdsDeleteNotConfirmed);
            }

            System.IO.File.Delete(path);
        }

        #region mock
        private static Dictionary<string, List<Datapoint>> _createdDataPoints = new Dictionary<string, List<Datapoint>>();

        private static async Task<HttpResponseMessage> mockInsertDataPointsAsync(HttpRequestMessage message , CancellationToken token) {
            var uri = message.RequestUri.ToString();

            if (uri.Contains("/timeseries/byids"))
            {
                return await CogniteTest.MockEnsureTimeSeriesSendAsync(message, token);
            }
            Assert.Contains($"{_project}/timeseries/data", uri);

            var responseBody = "{ }";
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
                    if (id.StartsWith("nc"))
                    {
                        dp.NumericDatapoints.Datapoints.Add(new NumericDatapoint{Timestamp = DateTime.UtcNow.ToUnixTimeMilliseconds(), Value = 1.0});
                    }
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