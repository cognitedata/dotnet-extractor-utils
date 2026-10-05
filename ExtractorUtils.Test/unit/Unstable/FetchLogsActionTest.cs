using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extensions;
using Cognite.Extensions.Unstable;
using Cognite.Extractor.Configuration;
using Cognite.Extractor.Logging;
using Cognite.Extractor.Testing;
using Cognite.Extractor.Utils;
using Cognite.Extractor.Utils.Unstable;
using Cognite.Extractor.Utils.Unstable.Configuration;
using Cognite.Extractor.Utils.Unstable.Tasks;
using CogniteSdk;
using CogniteSdk.Alpha;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Moq;
using Moq.Protected;
using Xunit;
using Xunit.Abstractions;

namespace ExtractorUtils.Test.unit.Unstable
{
    public class FetchLogsActionTest
    {
        private readonly ITestOutputHelper _output;

        public FetchLogsActionTest(ITestOutputHelper output)
        {
            _output = output;
        }

        /// <summary>
        /// Builds a real, working CogniteDestination backed by a mocked HTTP client factory --
        /// only the auth token endpoint is expected to be hit in tests that never reach an
        /// actual CDF API call (e.g. because no candidate log file exists on disk to upload).
        /// </summary>
        private (ServiceProvider, CogniteDestination) GetMockedDestination(
            Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>>? extraHandler = null)
        {
            var config = new ConnectionConfig
            {
                Project = "project",
                BaseUrl = "https://greenfield.cognitedata.com",
                Integration = new IntegrationConfig { ExternalId = "test-integration" },
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

            async Task<HttpResponseMessage> Handler(HttpRequestMessage message, CancellationToken token)
            {
                var uri = message.RequestUri!.ToString();
                if (uri == "http://example.url/token")
                {
                    var reply = "{\"token_type\": \"Bearer\", \"expires_in\": 2, \"access_token\": \"token\"}";
                    return new HttpResponseMessage { StatusCode = HttpStatusCode.OK, Content = new StringContent(reply) };
                }
                if (extraHandler != null)
                {
                    return await extraHandler(message, token).ConfigureAwait(false);
                }
                throw new InvalidOperationException($"Unexpected HTTP call in this test: {uri}");
            }

            var services = new ServiceCollection();
            services.AddConfig(config, typeof(ConnectionConfig));
            services.AddConfig(new BaseCogniteConfig(), typeof(BaseCogniteConfig));
            var mocks = TestUtilities.GetMockedHttpClientFactory(Handler);
            services.AddSingleton(mocks.factory.Object);
            services.AddTestLogging(_output);
            DestinationUtilsUnstable.AddCogniteClient(services, "myApp", null, setLogger: true, setMetrics: true, setHttpClient: true);
            services.AddCogniteDestination();
            var provider = services.BuildServiceProvider();
            return (provider, provider.GetRequiredService<CogniteDestination>());
        }

        private static Dictionary<string, string> Metadata(string start, string end) =>
            new Dictionary<string, string> { ["start_date"] = start, ["end_date"] = end };

        [Fact]
        public void TestValidateDateRangeMissingStartDateThrowsMissingParameter()
        {
            var err = Assert.Throws<ActionError>(() =>
                FetchLogsAction.ParseAndValidateDateRange(new Dictionary<string, string> { ["end_date"] = "2026-01-02" }));
            Assert.Equal("missing_parameter", err.ErrorType);
        }

        [Fact]
        public void TestValidateDateRangeMissingEndDateThrowsMissingParameter()
        {
            var err = Assert.Throws<ActionError>(() =>
                FetchLogsAction.ParseAndValidateDateRange(new Dictionary<string, string> { ["start_date"] = "2026-01-01" }));
            Assert.Equal("missing_parameter", err.ErrorType);
        }

        [Fact]
        public void TestValidateDateRangeNullCallMetadataThrowsMissingParameter()
        {
            var err = Assert.Throws<ActionError>(() => FetchLogsAction.ParseAndValidateDateRange(null));
            Assert.Equal("missing_parameter", err.ErrorType);
        }

        [Fact]
        public void TestValidateDateRangeMalformedStartDateThrowsInvalidParameter()
        {
            var err = Assert.Throws<ActionError>(() =>
                FetchLogsAction.ParseAndValidateDateRange(Metadata("not-a-date", "2026-01-02")));
            Assert.Equal("invalid_parameter", err.ErrorType);
        }

        [Fact]
        public void TestValidateDateRangeMalformedEndDateThrowsInvalidParameter()
        {
            var err = Assert.Throws<ActionError>(() =>
                FetchLogsAction.ParseAndValidateDateRange(Metadata("2026-01-01", "not-a-date")));
            Assert.Equal("invalid_parameter", err.ErrorType);
        }

        [Fact]
        public void TestValidateDateRangeEndBeforeStartThrowsInvalidDateRange()
        {
            var err = Assert.Throws<ActionError>(() =>
                FetchLogsAction.ParseAndValidateDateRange(Metadata("2026-01-05", "2026-01-01")));
            Assert.Equal("invalid_date_range", err.ErrorType);
        }

        [Fact]
        public void TestValidateDateRangeExceedingSevenDaysThrowsInvalidDateRange()
        {
            var err = Assert.Throws<ActionError>(() =>
                FetchLogsAction.ParseAndValidateDateRange(Metadata("2026-01-01", "2026-01-10")));
            Assert.Equal("invalid_date_range", err.ErrorType);
        }

        [Fact]
        public void TestValidateDateRangeStartInFutureThrowsInvalidDateRange()
        {
            var farFuture = DateTime.Now.AddYears(1).ToString("yyyy-MM-dd");
            var err = Assert.Throws<ActionError>(() =>
                FetchLogsAction.ParseAndValidateDateRange(Metadata(farFuture, farFuture)));
            Assert.Equal("invalid_date_range", err.ErrorType);
        }

        [Fact]
        public void TestValidateDateRangeExactlySevenDaysIsAccepted()
        {
            var (start, end) = FetchLogsAction.ParseAndValidateDateRange(Metadata("2026-01-01", "2026-01-08"));
            Assert.Equal(new DateTime(2026, 1, 1), start);
            Assert.Equal(new DateTime(2026, 1, 8), end);
        }

        [Fact]
        public void TestValidateDateRangeSameDayIsAccepted()
        {
            var (start, end) = FetchLogsAction.ParseAndValidateDateRange(Metadata("2026-01-01", "2026-01-01"));
            Assert.Equal(start, end);
        }

        [Fact]
        public void TestValidateDateRangeConvertsUtcTimestampToLocalDateBeforeTruncating()
        {
            // A "Z"-suffixed timestamp parses as Kind == Utc, not auto-converted to local time --
            // must match ToLocalTime().Date, not a hardcoded date, so this holds in any timezone
            // (a UTC-offset-zero runner is the one case this can't catch a regression in).
            const string timestamp = "2026-01-01T23:30:00Z";
            var expected = DateTime.Parse(timestamp, System.Globalization.CultureInfo.InvariantCulture,
                System.Globalization.DateTimeStyles.RoundtripKind).ToLocalTime().Date;

            var (start, end) = FetchLogsAction.ParseAndValidateDateRange(Metadata(timestamp, timestamp));

            Assert.Equal(expected, start);
            Assert.Equal(expected, end);
        }

        [Fact]
        public void TestGetLogFilePathDayRollingMatchesEmpiricallyVerifiedSerilogFormat()
        {
            // Confirmed empirically against the actual Serilog.Sinks.File version this repo
            // uses (7.0.0): day rolling produces "log20260911.txt", *not* the hyphenated
            // "log-20260911.txt" a naive port of Python's TimedRotatingFileHandler convention
            // would assume, and *not* a distinct "current file has no suffix" scheme --
            // today's file already carries the same date suffix as any rotated one.
            var path = FetchLogsAction.GetLogFilePath("/var/log/log.txt", new DateTime(2026, 9, 11), hourly: false);
            Assert.Equal(Path.Combine("/var/log", "log20260911.txt"), path);
        }

        [Fact]
        public void TestGetLogFilePathHourRollingMatchesEmpiricallyVerifiedSerilogFormat()
        {
            var path = FetchLogsAction.GetLogFilePath("/var/log/log.txt", new DateTime(2026, 9, 11, 14, 0, 0), hourly: true);
            Assert.Equal(Path.Combine("/var/log", "log2026091114.txt"), path);
        }

        [Fact]
        public void TestGetLogFilePathPreservesExtensionAndHandlesNoDirectory()
        {
            var path = FetchLogsAction.GetLogFilePath("log.txt", new DateTime(2026, 1, 1), hourly: false);
            Assert.Equal("log20260101.txt", path);
        }

        [Fact]
        public void TestGetCandidateFilesDayRollingSingleDay()
        {
            var config = new FileConfig { Path = "/logs/log.txt", RollingInterval = "day" };
            var date = DateTime.Now.Date;
            var candidates = FetchLogsAction.GetCandidateFiles(config, date, date).ToList();

            Assert.Single(candidates);
            Assert.True(candidates[0].IsLive);
            Assert.Equal(Path.Combine("/logs", $"log{date:yyyyMMdd}.txt"), candidates[0].Path);
        }

        [Fact]
        public void TestGetCandidateFilesDayRollingMultiDayRangeOnlyTodayIsLive()
        {
            var config = new FileConfig { Path = "/logs/log.txt", RollingInterval = "day" };
            var today = DateTime.Now.Date;
            var start = today.AddDays(-2);
            var candidates = FetchLogsAction.GetCandidateFiles(config, start, today).ToList();

            Assert.Equal(3, candidates.Count);
            Assert.False(candidates[0].IsLive);
            Assert.False(candidates[1].IsLive);
            Assert.True(candidates[2].IsLive);
        }

        [Fact]
        public void TestGetCandidateFilesHourRollingOneDayProducesUpToNowNotFullDay()
        {
            var config = new FileConfig { Path = "/logs/log.txt", RollingInterval = "hour" };
            var today = DateTime.Now.Date;
            var candidates = FetchLogsAction.GetCandidateFiles(config, today, today).ToList();

            // Only hours up to and including the current one for today -- not all 24, since the
            // rest haven't happened yet.
            Assert.Equal(DateTime.Now.Hour + 1, candidates.Count);
            Assert.True(candidates.Last().IsLive);
            Assert.All(candidates.Take(candidates.Count - 1), c => Assert.False(c.IsLive));
        }

        [Fact]
        public void TestGetCandidateFilesHourRollingPastDayProducesAll24Hours()
        {
            var config = new FileConfig { Path = "/logs/log.txt", RollingInterval = "hour" };
            var yesterday = DateTime.Now.Date.AddDays(-1);
            var candidates = FetchLogsAction.GetCandidateFiles(config, yesterday, yesterday).ToList();

            Assert.Equal(24, candidates.Count);
            Assert.All(candidates, c => Assert.False(c.IsLive));
        }

        [Fact]
        public async Task TestBoundedFileStreamCapsReadsAtSnapshottedLengthEvenIfFileGrowsDuringRead()
        {
            var path = Path.GetTempFileName();
            try
            {
                var initialContent = new byte[1000];
                new Random(42).NextBytes(initialContent);
                await System.IO.File.WriteAllBytesAsync(path, initialContent);

                using var bounded = new BoundedFileStream(path);
                Assert.Equal(1000, bounded.Length);

                var readSoFar = new List<byte>();
                var buffer = new byte[100];

                // Read half, then grow the file *while the stream is still open and being read
                // from* -- simulating the extractor continuing to write to today's log file
                // during an upload.
                int n;
                while (readSoFar.Count < 500 && (n = await bounded.ReadAsync(buffer, 0, buffer.Length, CancellationToken.None)) > 0)
                {
                    readSoFar.AddRange(buffer.Take(n));
                }

                using (var fs = new FileStream(path, FileMode.Append, FileAccess.Write, FileShare.ReadWrite))
                {
                    var extra = new byte[5000];
                    new Random(99).NextBytes(extra);
                    await fs.WriteAsync(extra, 0, extra.Length);
                }

                // Keep reading from the *same* already-open bounded stream -- it must stop at the
                // original 1000 bytes, never reading into the newly-appended 5000 bytes, even
                // though the underlying file is now 6000 bytes long.
                while ((n = await bounded.ReadAsync(buffer, 0, buffer.Length, CancellationToken.None)) > 0)
                {
                    readSoFar.AddRange(buffer.Take(n));
                }

                Assert.Equal(1000, readSoFar.Count);
                Assert.Equal(initialContent, readSoFar.ToArray());
            }
            finally
            {
                System.IO.File.Delete(path);
            }
        }

        [Fact]
        public async Task TestBoundedFileStreamSeeksRelativeToSnapshotAfterFileGrows()
        {
            var path = Path.GetTempFileName();
            try
            {
                await System.IO.File.WriteAllBytesAsync(path, new byte[] { 1, 2, 3 });
                using var bounded = new BoundedFileStream(path);

                using (var writer = new FileStream(path, FileMode.Append, FileAccess.Write, FileShare.ReadWrite))
                {
                    writer.WriteByte(4);
                    writer.WriteByte(5);
                }

                Assert.Equal(3, bounded.Length);
                Assert.Equal(3, bounded.Seek(0, SeekOrigin.End));
                Assert.Equal(bounded.Length, bounded.Position);
                Assert.Equal(-1, bounded.ReadByte());

                Assert.Equal(2, bounded.Seek(-1, SeekOrigin.End));
                var buffer = new byte[10];
                Assert.Equal(1, await bounded.ReadAsync(buffer, 0, buffer.Length, CancellationToken.None));
                Assert.Equal(3, buffer[0]);
                Assert.Equal(-1, bounded.ReadByte());

                Assert.Equal(4, bounded.Seek(1, SeekOrigin.End));
                Assert.Equal(-1, bounded.ReadByte());
                Assert.Equal(1, bounded.Seek(1, SeekOrigin.Begin));
                Assert.Equal(0, bounded.Seek(-1, SeekOrigin.Current));
                Assert.Equal(1, bounded.ReadByte());
            }
            finally
            {
                System.IO.File.Delete(path);
            }
        }

        [Fact]
        public async Task TestRunAsyncThrowsActionErrorWhenNoFileHandlerConfigured()
        {
            var ctx = new ActionContext<string>("config", null, "integration-1", "action-1", null, new DummySink());
            var err = await Assert.ThrowsAsync<ActionError>(() =>
                FetchLogsAction.RunAsync(ctx, null, null, CancellationToken.None));
            Assert.Equal("no_file_handler_configured", err.ErrorType);
        }

        [Fact]
        public async Task TestRunAsyncThrowsActionErrorWhenFileLoggingHasNoPath()
        {
            var ctx = new ActionContext<string>("config", null, "integration-1", "action-1", null, new DummySink());
            var loggerConfig = new LoggerConfig { File = new FileConfig { Path = null } };
            var err = await Assert.ThrowsAsync<ActionError>(() =>
                FetchLogsAction.RunAsync(ctx, loggerConfig, null, CancellationToken.None));
            Assert.Equal("no_file_handler_configured", err.ErrorType);
        }

        [Fact]
        public async Task TestRunAsyncThrowsActionErrorWhenNoCdfClientConfigured()
        {
            var ctx = new ActionContext<string>("config", null, "integration-1", "action-1", Metadata("2026-01-01", "2026-01-02"), new DummySink());
            var loggerConfig = new LoggerConfig { File = new FileConfig { Path = "/logs/log.txt", RollingInterval = "day" } };
            var err = await Assert.ThrowsAsync<ActionError>(() =>
                FetchLogsAction.RunAsync(ctx, loggerConfig, null, CancellationToken.None));
            Assert.Equal("no_cdf_client_configured", err.ErrorType);
        }

        [Fact]
        public async Task TestRunAsyncSkipsMissingFilesAndReportsSuccessWithZeroFiles()
        {
            // A date range where the configured log directory exists but has no matching files
            // (e.g. retention already rotated them out) -- must report a real, non-error result,
            // not fail, and never attempt a CDF upload. Uses a real (mocked-HTTP) CogniteDestination
            // that would fail this test immediately if anything beyond the auth token endpoint
            // were called, since the mock handler throws on any unexpected request -- proving no
            // upload was attempted for a date range with nothing to upload.
            var tempDir = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
            Directory.CreateDirectory(tempDir);
            try
            {
                var (provider, destination) = GetMockedDestination();
                using var p = provider;

                var ctx = new ActionContext<string>("config", destination, "integration-1", "action-1", Metadata("2026-01-01", "2026-01-02"), new DummySink());
                var loggerConfig = new LoggerConfig { File = new FileConfig { Path = Path.Combine(tempDir, "log.txt"), RollingInterval = "day" } };

                await FetchLogsAction.RunAsync(ctx, loggerConfig, null, CancellationToken.None);

                Assert.Contains("No log files found", ctx.ResultMessage);
                Assert.Equal("0", ctx.ResultMetadata!["fileCount"]);
                Assert.Equal("0", ctx.ResultMetadata!["totalBytes"]);
            }
            finally
            {
                Directory.Delete(tempDir, true);
            }
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task TestRunAsyncSkipsFilesThatCannotBeOpened(bool isLive)
        {
            var tempDir = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
            Directory.CreateDirectory(tempDir);
            try
            {
                var day = DateTime.Now.Date.AddDays(isLive ? 0 : -1);
                var date = day.ToString("yyyy-MM-dd");
                var logPath = Path.Combine(tempDir, $"log{day:yyyyMMdd}.txt");
                await System.IO.File.WriteAllTextAsync(logPath, "log content");
                using var lockedFile = new FileStream(logPath, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
                Assert.True(System.IO.File.Exists(logPath));

                // Any upload request is unexpected: the file exists but cannot be opened.
                var (provider, destination) = GetMockedDestination();
                using var p = provider;
                var sink = new DummySink();
                var ctx = new ActionContext<string>("config", destination, "test-integration", "action-1", Metadata(date, date), sink);
                var loggerConfig = new LoggerConfig { File = new FileConfig { Path = Path.Combine(tempDir, "log.txt"), RollingInterval = "day" } };

                await FetchLogsAction.RunAsync(ctx, loggerConfig, provider.GetRequiredService<IHttpClientFactory>(), CancellationToken.None);

                Assert.Equal("0", ctx.ResultMetadata!["fileCount"]);
                Assert.Equal("0", ctx.ResultMetadata["totalBytes"]);
                Assert.Contains("Skipped 1 missing/unavailable date(s)", ctx.ResultMessage);
                Assert.Empty(sink.ActionUpdates);
            }
            finally
            {
                Directory.Delete(tempDir, true);
            }
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task TestRunAsyncPropagatesUploadIOExceptionAndDisposesStream(bool isLive)
        {
            var tempDir = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
            Directory.CreateDirectory(tempDir);
            try
            {
                var day = DateTime.Now.Date.AddDays(isLive ? 0 : -1);
                var date = day.ToString("yyyy-MM-dd");
                var logPath = Path.Combine(tempDir, $"log{day:yyyyMMdd}.txt");
                await System.IO.File.WriteAllTextAsync(logPath, "log content");
                var failure = new IOException("Upload failed");
                Stream uploadStream = null;

                async Task<HttpResponseMessage> ExtraHandler(HttpRequestMessage message, CancellationToken token)
                {
                    var uri = message.RequestUri!.ToString();
                    if (uri.Contains("/files") && message.Method == HttpMethod.Post)
                    {
                        var body = "{\"id\": 1, \"uploaded\": false, \"createdTime\": 0, \"lastUpdatedTime\": 0, " +
                                   "\"uploadUrl\": \"http://example.url/upload-blob\"}";
                        var response = new HttpResponseMessage { StatusCode = HttpStatusCode.OK, Content = new StringContent(body) };
                        response.Content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
                        return response;
                    }
                    if (uri == "http://example.url/upload-blob" && message.Method == HttpMethod.Put)
                    {
                        uploadStream = await message.Content!.ReadAsStreamAsync(token).ConfigureAwait(false);
                        // A raw handler I/O failure must not be treated as a missing log file.
                        throw failure;
                    }
                    throw new InvalidOperationException($"Unexpected HTTP call in this test: {uri}");
                }

                var (provider, destination) = GetMockedDestination(ExtraHandler);
                using var p = provider;
                var sink = new DummySink();
                var ctx = new ActionContext<string>("config", destination, "test-integration", "action-1", Metadata(date, date), sink);
                var loggerConfig = new LoggerConfig { File = new FileConfig { Path = Path.Combine(tempDir, "log.txt"), RollingInterval = "day" } };

                var err = await Assert.ThrowsAsync<IOException>(() =>
                    FetchLogsAction.RunAsync(ctx, loggerConfig, provider.GetRequiredService<IHttpClientFactory>(), CancellationToken.None));

                Assert.Same(failure, err);
                Assert.Null(ctx.ResultMessage);
                Assert.Null(ctx.ResultMetadata);
                Assert.Empty(sink.ActionUpdates);
                Assert.NotNull(uploadStream);
                Assert.Throws<ObjectDisposedException>(() => uploadStream.ReadByte());
            }
            finally
            {
                Directory.Delete(tempDir, true);
            }
        }

        [Fact]
        public async Task TestRunAsyncUploadsFileAndReportsProgressAndAggregateMetadata()
        {
            var tempDir = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
            Directory.CreateDirectory(tempDir);
            try
            {
                var yesterday = DateTime.Now.Date.AddDays(-1);
                var logPath = Path.Combine(tempDir, $"log{yesterday:yyyyMMdd}.txt");
                var content = "some log content for yesterday";
                await System.IO.File.WriteAllTextAsync(logPath, content);

                var uploadCalls = new List<string>();
                var blobBytesReceived = new List<byte[]>();

                async Task<HttpResponseMessage> ExtraHandler(HttpRequestMessage message, CancellationToken token)
                {
                    var uri = message.RequestUri!.ToString();
                    if (uri.Contains("/files") && message.Method == HttpMethod.Post)
                    {
                        uploadCalls.Add(uri);
                        var body = "{\"externalId\": \"extractor-logs-test-integration-" + yesterday.ToString("yyyy-MM-dd") + "\", " +
                                   "\"id\": 1, \"uploaded\": false, \"createdTime\": 0, \"lastUpdatedTime\": 0, " +
                                   "\"uploadUrl\": \"http://example.url/upload-blob\"}";
                        var response = new HttpResponseMessage { StatusCode = HttpStatusCode.OK, Content = new StringContent(body) };
                        response.Content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
                        return response;
                    }
                    if (uri == "http://example.url/upload-blob")
                    {
                        var bytes = await message.Content!.ReadAsByteArrayAsync(token).ConfigureAwait(false);
                        blobBytesReceived.Add(bytes);
                        return new HttpResponseMessage { StatusCode = HttpStatusCode.OK };
                    }
                    throw new InvalidOperationException($"Unexpected HTTP call in this test: {uri}");
                }

                var (provider, destination) = GetMockedDestination(ExtraHandler);
                using var p = provider;

                var progressMessages = new List<string>();
                var sink = new DummySink();
                var ctx = new ActionContext<string>(
                    "config", destination, "test-integration", "action-1", Metadata(yesterday.ToString("yyyy-MM-dd"), yesterday.ToString("yyyy-MM-dd")), sink);

                var httpClientFactory = provider.GetRequiredService<IHttpClientFactory>();
                await FetchLogsAction.RunAsync(ctx, new LoggerConfig { File = new FileConfig { Path = Path.Combine(tempDir, "log.txt"), RollingInterval = "day" } }, httpClientFactory, CancellationToken.None);

                Assert.Single(uploadCalls);
                Assert.Single(blobBytesReceived);
                Assert.Equal(content, System.Text.Encoding.UTF8.GetString(blobBytesReceived[0]));

                Assert.Single(sink.ActionUpdates);
                Assert.Equal(ActionStatus.running, sink.ActionUpdates[0].Status);
                Assert.Contains("1/1", sink.ActionUpdates[0].ResultMessage);

                Assert.Contains("Uploaded 1 log file", ctx.ResultMessage);
                Assert.Equal("1", ctx.ResultMetadata!["fileCount"]);
                Assert.Equal(content.Length.ToString(), ctx.ResultMetadata!["totalBytes"]);
            }
            finally
            {
                Directory.Delete(tempDir, true);
            }
        }

        [Fact]
        public async Task TestRunAsyncCatchesUploadFailureAndStillUploadsRemainingFiles()
        {
            var tempDir = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
            Directory.CreateDirectory(tempDir);
            try
            {
                var dayFail = DateTime.Now.Date.AddDays(-2);
                var dayOk = DateTime.Now.Date.AddDays(-1);
                await System.IO.File.WriteAllTextAsync(Path.Combine(tempDir, $"log{dayFail:yyyyMMdd}.txt"), "will fail");
                await System.IO.File.WriteAllTextAsync(Path.Combine(tempDir, $"log{dayOk:yyyyMMdd}.txt"), "will succeed");

                // Each file's upload URL is keyed by date, so PutHandler can fail just one.
                async Task<HttpResponseMessage> ExtraHandler(HttpRequestMessage message, CancellationToken token)
                {
                    var uri = message.RequestUri!.ToString();
                    if (uri.Contains("/files") && message.Method == HttpMethod.Post)
                    {
                        var requestBody = await message.Content!.ReadAsStringAsync(token).ConfigureAwait(false);
                        var uploadUrl = requestBody.Contains(dayFail.ToString("yyyy-MM-dd"))
                            ? "http://example.url/upload-blob-fail"
                            : "http://example.url/upload-blob-ok";
                        var body = "{\"id\": 1, \"uploaded\": false, \"createdTime\": 0, \"lastUpdatedTime\": 0, " +
                                   $"\"uploadUrl\": \"{uploadUrl}\"}}";
                        var response = new HttpResponseMessage { StatusCode = HttpStatusCode.OK, Content = new StringContent(body) };
                        response.Content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
                        return response;
                    }
                    throw new InvalidOperationException($"Unexpected HTTP call in this test: {uri}");
                }

                // Needs its own fresh-client-per-call mock: TestUtilities.GetMockedHttpClientFactory
                // reuses one HttpClient, which UploadLogFileAsync disposes after each file's PUT
                // -- that would break the second file's call too.
                async Task<HttpResponseMessage> PutHandler(HttpRequestMessage message, CancellationToken token)
                {
                    var uri = message.RequestUri!.ToString();
                    if (uri == "http://example.url/upload-blob-fail")
                    {
                        // Network-level failure, not EnsureSuccessStatusCode -- same type HttpClient itself throws.
                        await message.Content!.ReadAsByteArrayAsync(token).ConfigureAwait(false);
                        throw new HttpRequestException("Connection reset");
                    }
                    if (uri == "http://example.url/upload-blob-ok")
                    {
                        await message.Content!.ReadAsByteArrayAsync(token).ConfigureAwait(false);
                        return new HttpResponseMessage { StatusCode = HttpStatusCode.OK };
                    }
                    throw new InvalidOperationException($"Unexpected HTTP call in this test: {uri}");
                }
                var putMockHandler = new Mock<HttpMessageHandler>();
                putMockHandler.Protected()
                    .Setup<Task<HttpResponseMessage>>("SendAsync", ItExpr.IsAny<HttpRequestMessage>(), ItExpr.IsAny<CancellationToken>())
                    .Returns<HttpRequestMessage, CancellationToken>(PutHandler);
                var putMockFactory = new Mock<IHttpClientFactory>();
                putMockFactory.Setup(f => f.CreateClient(It.IsAny<string>())).Returns(() => new HttpClient(putMockHandler.Object));

                var (provider, destination) = GetMockedDestination(ExtraHandler);
                using var p = provider;
                var sink = new DummySink();
                var ctx = new ActionContext<string>(
                    "config", destination, "test-integration", "action-1",
                    Metadata(dayFail.ToString("yyyy-MM-dd"), dayOk.ToString("yyyy-MM-dd")), sink);

                // Must not throw: one file failing shouldn't stop the other from uploading.
                await FetchLogsAction.RunAsync(ctx, new LoggerConfig { File = new FileConfig { Path = Path.Combine(tempDir, "log.txt"), RollingInterval = "day" } },
                    putMockFactory.Object, CancellationToken.None);

                Assert.Equal("1", ctx.ResultMetadata!["fileCount"]);
                Assert.Contains("Uploaded 1 log file", ctx.ResultMessage);
                Assert.Contains("Failed to upload 1 file(s) due to a network or CDF error", ctx.ResultMessage);
            }
            finally
            {
                Directory.Delete(tempDir, true);
            }
        }

        [Fact]
        public async Task TestRunAsyncThrowsActionErrorWhenEveryUploadFails()
        {
            var tempDir = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
            Directory.CreateDirectory(tempDir);
            try
            {
                var day = DateTime.Now.Date.AddDays(-1);
                var date = day.ToString("yyyy-MM-dd");
                await System.IO.File.WriteAllTextAsync(Path.Combine(tempDir, $"log{day:yyyyMMdd}.txt"), "content");

                async Task<HttpResponseMessage> ExtraHandler(HttpRequestMessage message, CancellationToken token)
                {
                    var uri = message.RequestUri!.ToString();
                    if (uri.Contains("/files") && message.Method == HttpMethod.Post)
                    {
                        var body = "{\"id\": 1, \"uploaded\": false, \"createdTime\": 0, \"lastUpdatedTime\": 0, " +
                                   "\"uploadUrl\": \"http://example.url/upload-blob\"}";
                        var response = new HttpResponseMessage { StatusCode = HttpStatusCode.OK, Content = new StringContent(body) };
                        response.Content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
                        return response;
                    }
                    if (uri == "http://example.url/upload-blob")
                    {
                        if (message.Content != null) await message.Content.ReadAsByteArrayAsync(token).ConfigureAwait(false);
                        throw new HttpRequestException("Connection reset");
                    }
                    throw new InvalidOperationException($"Unexpected HTTP call in this test: {uri}");
                }

                var (provider, destination) = GetMockedDestination(ExtraHandler);
                using var p = provider;
                var sink = new DummySink();
                var ctx = new ActionContext<string>("config", destination, "test-integration", "action-1", Metadata(date, date), sink);

                // All uploads failed -- must report failed, not succeeded with zero files.
                var err = await Assert.ThrowsAsync<ActionError>(() =>
                    FetchLogsAction.RunAsync(ctx, new LoggerConfig { File = new FileConfig { Path = Path.Combine(tempDir, "log.txt"), RollingInterval = "day" } },
                        provider.GetRequiredService<IHttpClientFactory>(), CancellationToken.None));

                Assert.Equal("upload_failed", err.ErrorType);
            }
            finally
            {
                Directory.Delete(tempDir, true);
            }
        }
    }
}
