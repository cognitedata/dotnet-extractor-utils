using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extractor.Logging;
using Cognite.Extractor.Utils.Unstable.Tasks;
using Xunit;

namespace ExtractorUtils.Test.unit.Unstable
{
    public class FetchLogsActionTest
    {

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
                FetchLogsAction.ParseAndValidateDateRange(Metadata("2026-01-01", "2026-01-08")));
            Assert.Equal("invalid_date_range", err.ErrorType);
        }

        [Fact]
        public void TestValidateDateRangeEndInFutureThrowsInvalidDateRange()
        {
            var today = DateTime.Now.Date;
            var err = Assert.Throws<ActionError>(() =>
                FetchLogsAction.ParseAndValidateDateRange(Metadata(today.ToString("yyyy-MM-dd"), today.AddDays(1).ToString("yyyy-MM-dd"))));
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
            var (start, end) = FetchLogsAction.ParseAndValidateDateRange(Metadata("2026-01-01", "2026-01-07"));
            Assert.Equal(new DateTime(2026, 1, 1), start);
            Assert.Equal(new DateTime(2026, 1, 7), end);
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
        public void TestGetCandidateFilesNullFileConfigReturnsEmpty()
        {
            var date = DateTime.Now.Date;
            var candidates = FetchLogsAction.GetCandidateFiles(null, date, date).ToList();

            Assert.Empty(candidates);
        }

        [Fact]
        public void TestGetCandidateFilesNullPathReturnsEmpty()
        {
            var config = new FileConfig { Path = null, RollingInterval = "day" };
            var date = DateTime.Now.Date;
            var candidates = FetchLogsAction.GetCandidateFiles(config, date, date).ToList();

            Assert.Empty(candidates);
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
    }
}
