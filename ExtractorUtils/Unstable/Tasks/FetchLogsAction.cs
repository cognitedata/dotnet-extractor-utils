using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extractor.Logging;
using CogniteSdk;

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
            _boundedLength = _inner.Length;
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

        public override long Seek(long offset, SeekOrigin origin) => _inner.Seek(offset, origin);
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

        /// <summary>
        /// Run the fetch_logs action.
        /// </summary>
        /// <typeparam name="TConfig">Extractor configuration type -- irrelevant to this action's
        /// own logic, but required since <see cref="ActionContext{TConfig}"/> is generic over it.</typeparam>
        public static async Task RunAsync<TConfig>(ActionContext<TConfig> ctx, LoggerConfig? loggerConfig, IHttpClientFactory? httpClientFactory, CancellationToken token)
        {
            var fileConfig = loggerConfig?.File;
            if (fileConfig?.Path == null)
            {
                throw new ActionError("no_file_handler_configured", "This extractor is not configured to log to a file.");
            }
            if (ctx.CdfClient == null)
            {
                throw new ActionError("no_cdf_client_configured", "This extractor has no CDF client configured, cannot upload log files.");
            }

            var (start, end) = ParseAndValidateDateRange(ctx.CallMetadata);

            var candidates = GetCandidateFiles(fileConfig, start, end).ToList();
            var uploaded = new List<(string Date, long Bytes)>();
            var skippedDates = new List<string>();

            for (int i = 0; i < candidates.Count; i++)
            {
                token.ThrowIfCancellationRequested();
                var (path, date, isLive) = candidates[i];

                if (!System.IO.File.Exists(path))
                {
                    // Expected and normal: a requested date may be outside the retention window,
                    // or (for the most recent hour/day) not written yet.
                    skippedDates.Add(date);
                    continue;
                }

                long bytes;
                try
                {
                    bytes = await UploadLogFileAsync(ctx, httpClientFactory, path, date, isLive, token).ConfigureAwait(false);
                }
                catch (IOException)
                {
                    // The file existed a moment ago but vanished or became inaccessible before
                    // we could open it (e.g. rotated out by RetentionLimit in the meantime) --
                    // treat the same as "missing" rather than failing the whole action over one
                    // date.
                    skippedDates.Add(date);
                    continue;
                }

                uploaded.Add((date, bytes));
                ctx.ReportProgress($"Uploading: {i + 1}/{candidates.Count} files complete");
            }

            var metadata = new Dictionary<string, string>
            {
                ["fileCount"] = uploaded.Count.ToString(CultureInfo.InvariantCulture),
                ["totalBytes"] = uploaded.Sum(u => u.Bytes).ToString(CultureInfo.InvariantCulture),
            };
            // Per-file breakdown is included unconditionally -- if it doesn't fit within odin's
            // metadata limits, CheckInWorker.QueueActionUpdate (EDG-880) already reduces it and
            // notes as much, without turning this into a failure. No need to duplicate that
            // "does it fit" logic here.
            foreach (var (date, bytes) in uploaded)
            {
                metadata[$"file:{date}"] = bytes.ToString(CultureInfo.InvariantCulture);
            }

            var message = uploaded.Count == 0
                ? $"No log files found for {start:yyyy-MM-dd} to {end:yyyy-MM-dd}."
                : $"Uploaded {uploaded.Count} log file(s) covering {start:yyyy-MM-dd} to {end:yyyy-MM-dd}.";
            if (skippedDates.Count > 0)
            {
                message += $" Skipped {skippedDates.Count} missing/unavailable date(s).";
            }

            ctx.SetResult(message, metadata);
        }

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

        private static async Task<long> UploadLogFileAsync<TConfig>(
            ActionContext<TConfig> ctx, IHttpClientFactory? httpClientFactory, string path, string date, bool isLive, CancellationToken token)
        {
            // Only the file matching today's date (or the current hour, under hourly rolling) is
            // still being written to -- everything else is already closed and static, and can be
            // read directly without the bounded-length wrapper. Cast one branch explicitly to
            // Stream: BoundedFileStream and FileStream share no common ancestor closer than
            // Stream itself, and netstandard2.0's C# 8 language version can't infer that common
            // type from a bare conditional expression the way C# 9+ target-typing would.
            using Stream stream = isLive ? (Stream)new BoundedFileStream(path) : System.IO.File.OpenRead(path);
            var length = stream.Length;

            var uploadRead = await ctx.CdfClient!.CogniteClient.Files.UploadAsync(new FileCreate
            {
                ExternalId = $"extractor-logs-{ctx.IntegrationExternalId}-{date}",
                Name = Path.GetFileName(path),
                MimeType = "text/plain",
                Source = "extractor",
            }, overwrite: true, token).ConfigureAwait(false);

            using var content = new StreamContent(stream);
            content.Headers.ContentLength = length;
            // Prefer the shared, pooled client from DI (already registered elsewhere in this
            // repo's DI setup) over a fresh HttpClient per upload, to avoid socket exhaustion --
            // falls back to a throwaway instance only if unavailable (e.g. a minimal test setup).
            // Disposing an IHttpClientFactory-created HttpClient is safe and expected either way
            // -- per Microsoft's documented design, it only releases the short-lived wrapper, not
            // the pooled HttpMessageHandler underneath it.
            using var httpClient = httpClientFactory?.CreateClient() ?? new HttpClient();
            var response = await httpClient.PutAsync(uploadRead.UploadUrl, content, token).ConfigureAwait(false);
            response.EnsureSuccessStatusCode();

            return length;
        }
    }
}
