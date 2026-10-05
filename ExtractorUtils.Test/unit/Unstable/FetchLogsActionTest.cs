using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Cognite.Extensions.Unstable;
using Cognite.Extractor.Utils.Unstable.Tasks;
using Xunit;

namespace ExtractorUtils.Test.Unit.Unstable
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
        public void TestValidateDateRangeExceedingSevenDaysThrowsInvalidDateRange()
        {
            var err = Assert.Throws<ActionError>(() =>
                FetchLogsAction.ParseAndValidateDateRange(Metadata("2026-01-01", "2026-01-10")));
            Assert.Equal("invalid_date_range", err.ErrorType);
        }

        [Fact]
        public void TestValidateDateRangeConvertsUtcTimestampToLocalDateBeforeTruncating()
        {
            // A "Z"-suffixed timestamp parses with Kind == Utc and isn't auto-converted, so this
            // must compare against ToLocalTime().Date rather than a hardcoded date -- that way the
            // test still catches a regression on any timezone other than UTC+0.
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
            // Verified against the Serilog.Sinks.File version this repo uses (7.0.0): day rolling
            // produces "log20260911.txt", not the hyphenated "log-20260911.txt" a naive port of
            // Python's rotation convention would assume.
            var path = FetchLogsAction.GetLogFilePath("/var/log/log.txt", new DateTime(2026, 9, 11), hourly: false);
            Assert.Equal(Path.Combine("/var/log", "log20260911.txt"), path);
        }

        [Fact]
        public async Task TestBoundedFileStreamSeeksRelativeToSnapshotAfterFileGrows()
        {
            var path = Path.GetTempFileName();
            try
            {
                await File.WriteAllBytesAsync(path, new byte[] { 1, 2, 3 });
                using var bounded = new BoundedFileStream(path);

                using (var writer = new FileStream(path, FileMode.Append, FileAccess.Write, FileShare.ReadWrite))
                {
                    writer.WriteByte(4);
                    writer.WriteByte(5);
                }

                // Must stay bounded to the original 3 bytes snapshotted at open time, not the
                // file's new 5-byte length.
                Assert.Equal(3, bounded.Length);
                Assert.Equal(3, bounded.Seek(0, SeekOrigin.End));
                Assert.Equal(-1, bounded.ReadByte());

                Assert.Equal(2, bounded.Seek(-1, SeekOrigin.End));
                Assert.Equal(3, bounded.ReadByte());
            }
            finally
            {
                File.Delete(path);
            }
        }
    }
}
