using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extractor.Logging;

namespace Cognite.Extractor.Utils.Unstable.Tasks
{
    /// <summary>
    /// A stream over a file that may still be growing (e.g. the log file the extractor is
    /// currently writing to), which caps reads at the length the file had when this stream was
    /// opened, so an upload never reads past the point it declared as the content length even if
    /// the underlying file keeps growing concurrently. Only needed for the current day's (or
    /// current hour's, under hourly rolling) still-open log file -- rotated files are already
    /// closed and static, and can be read directly.
    /// </summary>
    internal sealed class BoundedFileStream : Stream
    {
        private readonly FileStream _inner;
        private readonly long _boundedLength;

        public BoundedFileStream(string path)
        {
            _inner = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            try
            {
                _boundedLength = _inner.Length;
            }
            catch
            {
                _inner.Dispose();
                throw;
            }
        }

        public override bool CanRead => true;
        public override bool CanSeek => true;
        public override bool CanWrite => false;
        public override long Length => _boundedLength;

        public override long Position
        {
            get => _inner.Position;
            set => _inner.Position = value;
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            var remaining = _boundedLength - _inner.Position;
            if (remaining <= 0) return 0;
            return _inner.Read(buffer, offset, (int)Math.Min(count, remaining));
        }

        public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            var remaining = _boundedLength - _inner.Position;
            if (remaining <= 0) return 0;
            return await _inner.ReadAsync(buffer, offset, (int)Math.Min(count, remaining), cancellationToken).ConfigureAwait(false);
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            // Resolve End against the snapshot, not the possibly growing file.
            if (origin == SeekOrigin.End)
            {
                return _inner.Seek(_boundedLength + offset, SeekOrigin.Begin);
            }
            return _inner.Seek(offset, origin);
        }

        public override void Flush() => _inner.Flush();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        protected override void Dispose(bool disposing)
        {
            if (disposing) _inner.Dispose();
            base.Dispose(disposing);
        }
    }

    /// <summary>
    /// Built-in custom action, registered automatically for every extractor (see
    /// <see cref="BaseExtractor{TConfig}"/>), that uploads rotated log files covering a
    /// requested date range to CDF Files.
    /// </summary>
    internal static class FetchLogsAction
    {
        /// <summary>
        /// Name this action is registered and advertised under.
        /// </summary>
        public const string Name = "fetch_logs";

        private const int MaxDateRangeDays = 7;

        internal static (DateTime Start, DateTime End) ParseAndValidateDateRange(IReadOnlyDictionary<string, string>? callMetadata)
        {
            if (callMetadata == null || !callMetadata.TryGetValue("start_date", out var startStr) || string.IsNullOrEmpty(startStr))
            {
                throw new ActionError("missing_parameter", "start_date is required");
            }
            if (!callMetadata.TryGetValue("end_date", out var endStr) || string.IsNullOrEmpty(endStr))
            {
                throw new ActionError("missing_parameter", "end_date is required");
            }

            if (!DateTime.TryParse(startStr, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var start))
            {
                throw new ActionError("invalid_parameter", $"start_date '{startStr}' is not a valid ISO 8601 date", "Expected e.g. '2026-01-01'");
            }
            if (!DateTime.TryParse(endStr, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var end))
            {
                throw new ActionError("invalid_parameter", $"end_date '{endStr}' is not a valid ISO 8601 date", "Expected e.g. '2026-01-07'");
            }

            // Log files roll on the extractor host's local time, so a caller-supplied UTC
            // timestamp (Kind == Utc, e.g. a trailing "Z") must be converted to local time before
            // truncating to a date -- DateTime comparisons compare raw Ticks and ignore Kind
            // entirely, so an unconverted Utc value compared against DateTime.Now below could be
            // off by the local UTC offset.
            if (start.Kind == DateTimeKind.Utc) start = start.ToLocalTime();
            if (end.Kind == DateTimeKind.Utc) end = end.ToLocalTime();

            start = start.Date;
            end = end.Date;

            if (end < start)
            {
                throw new ActionError("invalid_date_range", "end_date must not be before start_date");
            }
            if ((end - start).TotalDays > MaxDateRangeDays)
            {
                throw new ActionError("invalid_date_range", $"Date range must not exceed {MaxDateRangeDays} days");
            }
            if (start > DateTime.Now.Date)
            {
                throw new ActionError("invalid_date_range", "start_date must not be in the future");
            }

            return (start, end);
        }

        /// <summary>
        /// Enumerate candidate log file paths for the given (inclusive) date range, matching
        /// Serilog's actual RollingInterval.Day/Hour on-disk naming -- confirmed empirically
        /// (base filename, no separator, then the date suffix, then the original extension:
        /// e.g. "log.txt" with day rolling produces "log20260911.txt", not the hyphenated
        /// "log-20260911.txt" a naive port of Python's TimedRotatingFileHandler convention would
        /// assume) rather than relied on Serilog's documented default without checking it.
        /// </summary>
        internal static IEnumerable<(string Path, string Date, bool IsLive)> GetCandidateFiles(FileConfig fileConfig, DateTime start, DateTime end)
        {
            var isHourly = string.Equals(fileConfig.RollingInterval, "hour", StringComparison.OrdinalIgnoreCase);
            var now = DateTime.Now;

            for (var day = start; day <= end; day = day.AddDays(1))
            {
                if (isHourly)
                {
                    for (int h = 0; h < 24; h++)
                    {
                        var timestamp = day.AddHours(h);
                        if (timestamp > now) yield break;
                        var isLive = timestamp.Date == now.Date && timestamp.Hour == now.Hour;
                        yield return (GetLogFilePath(fileConfig.Path!, timestamp, true), timestamp.ToString("yyyy-MM-ddTHH", CultureInfo.InvariantCulture), isLive);
                    }
                }
                else
                {
                    if (day > now) yield break;
                    var isLive = day.Date == now.Date;
                    yield return (GetLogFilePath(fileConfig.Path!, day, false), day.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture), isLive);
                }
            }
        }

        internal static string GetLogFilePath(string basePath, DateTime timestamp, bool hourly)
        {
            var dir = Path.GetDirectoryName(basePath) ?? string.Empty;
            var fileNameWithoutExt = Path.GetFileNameWithoutExtension(basePath);
            var ext = Path.GetExtension(basePath);
            var suffix = hourly ? timestamp.ToString("yyyyMMddHH", CultureInfo.InvariantCulture) : timestamp.ToString("yyyyMMdd", CultureInfo.InvariantCulture);
            return Path.Combine(dir, $"{fileNameWithoutExt}{suffix}{ext}");
        }

        /// <summary>
        /// Open a log file, bounding reads for live files. Returns null if the file is unavailable.
        /// The caller owns the returned stream.
        /// </summary>
        private static Stream? TryOpenLogFileStream(string path, bool isLive)
        {
            try
            {
                if (isLive) return new BoundedFileStream(path);
                return new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            }
            catch (Exception ex) when (ex is IOException || ex is UnauthorizedAccessException)
            {
                // Rotation or permissions can make an existing file unavailable. Only catch
                // file-open failures here; read and upload failures must propagate.
                return null;
            }
        }

    }
}
