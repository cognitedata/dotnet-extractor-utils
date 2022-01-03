using Cognite.Extensions;
using Cognite.Extractor.Logging;
using CogniteSdk;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace ExtractorUtils.Test.Unit
{
    public class CogniteResultTests
    {
        private ILogger<CogniteResultTests> logger;
        public CogniteResultTests()
        {
            var services = new ServiceCollection();
            services.AddSingleton(new LoggerConfig
            {
                Console = new ConsoleConfig { Level = "debug" }
            });
            services.AddLogging();
            logger = services.BuildServiceProvider().GetRequiredService<ILogger<CogniteResultTests>>();
        }
        [Fact]
        public void ParseUnknownException()
        {
            var ex = new Exception("Some error");
            var error = ResultHandlers.ParseException<AssetCreate>(ex, RequestType.CreateAssets);
            Assert.Equal(0, error.Status);
            Assert.Equal(ex, error.Exception);
            Assert.Equal("Some error", error.Message);
        }
        [Fact]
        public void ParseResponseException()
        {
            var ex = new ResponseException("Something bad happened")
            {
                Code = 500
            };
            var error = ResultHandlers.ParseException<AssetCreate>(ex, RequestType.CreateAssets);
            Assert.Equal(500, error.Status);
            Assert.Equal(ex, error.Exception);
            Assert.Equal("Something bad happened", error.Message);
        }

        [Fact]
        public void CleanAssetRequest()
        {
            var assets = new[]
            {
                new AssetCreate
                {
                    ExternalId = "duplicate",
                },
                new AssetCreate
                {
                    ExternalId = "duplicate2"
                },
                new AssetCreate
                {
                    DataSetId = 123
                },
                new AssetCreate
                {
                    DataSetId = 1234
                },
                new AssetCreate
                {
                    ParentExternalId = "missing"
                },
                new AssetCreate
                {
                    ParentExternalId = "missing"
                },
                new AssetCreate
                {
                    ParentId = 123
                },
                new AssetCreate
                {
                    ParentId = 1234
                },
                new AssetCreate
                {
                    ExternalId = "ImOk"
                }
            };
            var exceptions = new[]
            {
                new ResponseException
                {
                    Duplicated = new [] { new Dictionary<string, MultiValue> {
                        { "externalId", MultiValue.Create("duplicate") } },
                    new Dictionary<string, MultiValue> {
                        { "externalId", MultiValue.Create("duplicate2") } }},
                    Code = 409
                },
                new ResponseException("Invalid dataSetIds: 123, 1234") { Code = 400 },
                new ResponseException("Reference to unknown parent with externalId missing") { Code = 400 },
                new ResponseException("The given parent ids do not exist: 123, 1234") { Code = 400 }
            };
            var errors = new List<CogniteError>();
            for (int i = 0; i < exceptions.Length; i++)
            {
                var error = ResultHandlers.ParseException<AssetCreate>(exceptions[i], RequestType.CreateAssets);
                logger.LogCogniteError(error, RequestType.CreateAssets, false, LogLevel.Debug, LogLevel.Warning);
                assets = ResultHandlers.CleanFromError(error, assets).ToArray();
                Assert.Equal(9 - i * 2 - 2, assets.Count());
                errors.Add(error);
            }

            Assert.Single(assets);
            Assert.Equal("ImOk", assets[0].ExternalId);
            Assert.Equal(4, errors.Count);
            for (int i = 0; i < exceptions.Length; i++)
            {
                Assert.Equal(errors[i].Exception, exceptions[i]);
                Assert.Equal(exceptions[i].Message, errors[i].Message);
            }
        }

        [Fact]
        public void CleanTimeSeriesRequest()
        {
            var timeseries = new[]
            {
                new TimeSeriesCreate
                {
                    ExternalId = "duplicate",
                },
                new TimeSeriesCreate
                {
                    ExternalId = "duplicate2"
                },
                new TimeSeriesCreate
                {
                    LegacyName = "duplicate",
                },
                new TimeSeriesCreate
                {
                    LegacyName = "duplicate2"
                },
                new TimeSeriesCreate
                {
                    DataSetId = 123
                },
                new TimeSeriesCreate
                {
                    DataSetId = 1234
                },
                new TimeSeriesCreate
                {
                    AssetId = 123
                },
                new TimeSeriesCreate
                {
                    AssetId = 1234
                },
                new TimeSeriesCreate
                {
                    ExternalId = "ImOk"
                }
            };
            var exceptions = new[]
            {
                new ResponseException
                {
                    Duplicated = new [] { new Dictionary<string, MultiValue> {
                        { "externalId", MultiValue.Create("duplicate") } },
                    new Dictionary<string, MultiValue> {
                        { "externalId", MultiValue.Create("duplicate2") } }},
                    Code = 409
                },
                new ResponseException
                {
                    Duplicated = new [] { new Dictionary<string, MultiValue> {
                        { "legacyName", MultiValue.Create("duplicate") } },
                    new Dictionary<string, MultiValue> {
                        { "legacyName", MultiValue.Create("duplicate2") } }},
                    Code = 409
                },
                new ResponseException("datasets ids not found") {
                    Missing = new [] { new Dictionary<string, MultiValue> {
                        { "id", MultiValue.Create(123) } },
                    new Dictionary<string, MultiValue> {
                        { "id", MultiValue.Create(1234) } } },
                    Code = 400
                },
                new ResponseException("Asset ids not found") {
                    Missing = new [] { new Dictionary<string, MultiValue> {
                        { "id", MultiValue.Create(123) } },
                    new Dictionary<string, MultiValue> {
                        { "id", MultiValue.Create(1234) } } },
                    Code = 400
                }
            };
            var errors = new List<CogniteError>();
            for (int i = 0; i < exceptions.Length; i++)
            {
                var error = ResultHandlers.ParseException<TimeSeriesCreate>(exceptions[i], RequestType.CreateTimeSeries);
                logger.LogCogniteError(error, RequestType.CreateTimeSeries, false, LogLevel.Debug, LogLevel.Warning);
                timeseries = (ResultHandlers.CleanFromError(error, timeseries))
                    .ToArray();
                Assert.Equal(9 - i * 2 - 2, timeseries.Count());
                errors.Add(error);
            }

            Assert.Single(timeseries);
            Assert.Equal("ImOk", timeseries[0].ExternalId);
            Assert.Equal(4, errors.Count);
            for (int i = 0; i < exceptions.Length; i++)
            {
                Assert.Equal(errors[i].Exception, exceptions[i]);
                Assert.Equal(exceptions[i].Message, errors[i].Message);
            }
        }

        [Fact]
        public void CleanEventsRequest()
        {
            var events = new[]
            {
                new EventCreate
                {
                    ExternalId = "duplicate",
                },
                new EventCreate
                {
                    ExternalId = "duplicate2"
                },
                new EventCreate
                {
                    DataSetId = 123
                },
                new EventCreate
                {
                    DataSetId = 1234
                },
                new EventCreate
                {
                    AssetIds = new [] { 123L }
                },
                new EventCreate
                {
                    AssetIds = new [] { 1234L }
                },
                new EventCreate
                {
                    ExternalId = "ImOk"
                }
            };
            var exceptions = new[]
            {
                new ResponseException
                {
                    Duplicated = new [] { new Dictionary<string, MultiValue> {
                        { "externalId", MultiValue.Create("duplicate") } },
                    new Dictionary<string, MultiValue> {
                        { "externalId", MultiValue.Create("duplicate2") } }},
                    Code = 409
                },
                new ResponseException("Invalid dataSetIds: 123, 1234") { Code = 400 },
                new ResponseException("Asset ids not found") {
                    Missing = new [] { new Dictionary<string, MultiValue> {
                        { "id", MultiValue.Create(123) } },
                    new Dictionary<string, MultiValue> {
                        { "id", MultiValue.Create(1234) } } },
                    Code = 400
                }
            };
            var errors = new List<CogniteError>();
            for (int i = 0; i < exceptions.Length; i++)
            {
                var error = ResultHandlers.ParseException<EventCreate>(exceptions[i], RequestType.CreateEvents);
                logger.LogCogniteError(error, RequestType.CreateEvents, false, LogLevel.Debug, LogLevel.Warning);
                events = (ResultHandlers.CleanFromError(error, events))
                    .ToArray();
                Assert.Equal(7 - i * 2 - 2, events.Count());
                errors.Add(error);
            }

            Assert.Single(events);
            Assert.Equal("ImOk", events[0].ExternalId);
            Assert.Equal(3, errors.Count);
            for (int i = 0; i < exceptions.Length; i++)
            {
                Assert.Equal(errors[i].Exception, exceptions[i]);
                Assert.Equal(exceptions[i].Message, errors[i].Message);
            }
        }
        [Fact]
        public void TestThrowOnError()
        {
            var result = new CogniteResult<AssetCreate>(null);
            result.Throw();
            result.ThrowOnFatal();
            result.Errors = Enumerable.Empty<CogniteError<AssetCreate>>();
            result.Throw();
            result.ThrowOnFatal();
            // One non-fatal
            result.Errors = new[]
            {
                new CogniteError<AssetCreate> { Exception = new Exception("Test"), Type = ErrorType.SanitationFailed }
            };
            Assert.Throws<CogniteErrorException>(() => result.Throw());
            result.ThrowOnFatal();
            // Multiple non-fatal
            result.Errors = new[]
            {
                new CogniteError<AssetCreate> { Exception = new Exception("Test"), Type = ErrorType.SanitationFailed },
                new CogniteError<AssetCreate> { Exception = new Exception("Test2"), Type = ErrorType.ItemDuplicated }
            };
            Assert.Throws<AggregateException>(() => result.Throw());
            result.ThrowOnFatal();
            // One fatal, multiple non-fatal
            result.Errors = new[]
            {
                new CogniteError<AssetCreate> { Exception = new Exception("Test"), Type = ErrorType.SanitationFailed },
                new CogniteError<AssetCreate> { Exception = new Exception("Test2"), Type = ErrorType.ItemDuplicated },
                new CogniteError<AssetCreate> { Exception = new Exception("Test3"), Type = ErrorType.FatalFailure }
            };
            Assert.Throws<AggregateException>(() => result.Throw());
            Assert.Throws<CogniteErrorException>(() => result.ThrowOnFatal());
            // Multiple fatal, multiple non-fatal
            result.Errors = new[]
            {
                new CogniteError<AssetCreate> { Exception = new Exception("Test"), Type = ErrorType.SanitationFailed },
                new CogniteError<AssetCreate> { Exception = new Exception("Test2"), Type = ErrorType.ItemDuplicated },
                new CogniteError<AssetCreate> { Exception = new Exception("Test3"), Type = ErrorType.FatalFailure },
                new CogniteError<AssetCreate> { Exception = new Exception("Test3"), Type = ErrorType.FatalFailure }
            };
            Assert.Throws<AggregateException>(() => result.Throw());
            Assert.Throws<AggregateException>(() => result.ThrowOnFatal());
        }
        [Fact]
        public void TestGroupBySkipped()
        {
            var result = new CogniteResult<string>(new[]
            {
                new CogniteError<string>
                {
                    Skipped = new[] { "s1", "s2", "s3" },
                    Resource = ResourceType.ExternalId
                },
                new CogniteError<string>
                {
                    Skipped = new[] { "s2", "s4" },
                    Resource = ResourceType.DataSetId
                },
                new CogniteError<string>
                {
                    Skipped = new[] { "s4", "s2" },
                    Resource = ResourceType.Unit
                }
            });

            var groups = result.ErrorsBySkipped();
            var groupDict = groups.ToDictionary(pair => pair.Skipped, pair => pair.Errors.ToArray());

            Assert.Single(groupDict["s1"]);
            Assert.Equal(ResourceType.ExternalId, groupDict["s1"].First().Resource);

            Assert.Equal(3, groupDict["s2"].Length);
            Assert.Equal(ResourceType.ExternalId, groupDict["s2"][0].Resource);
            Assert.Equal(ResourceType.DataSetId, groupDict["s2"][1].Resource);
            Assert.Equal(ResourceType.Unit, groupDict["s2"][2].Resource);

            Assert.Single(groupDict["s3"]);
            Assert.Equal(ResourceType.ExternalId, groupDict["s3"].First().Resource);

            Assert.Equal(2, groupDict["s4"].Length);
            Assert.Equal(ResourceType.DataSetId, groupDict["s4"][0].Resource);
            Assert.Equal(ResourceType.Unit, groupDict["s4"][1].Resource);
        }
    }
}
