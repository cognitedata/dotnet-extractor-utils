using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extractor.Logging;
using CogniteSdk;

namespace Cognite.Extractor.Utils.Unstable.Tasks
{
    /// <summary>
    /// Read-only stream over a file that may still be growing (the live log file), capped at the
    /// length it had when opened so reads never exceed the declared upload Content-Length.
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
    /// Built-in action, registered for every extractor, that uploads log files covering a
    /// requested date range to CDF Files.
    /// </summary>
    internal static class FetchLogsAction
    {
        /// <summary>
        /// Name this action is registered and advertised under.
        /// </summary>
        public const string Name = "fetch_logs";

        private const int MaxDateRangeDays = 7;

        // CDF's single-request upload limit.
        private const long MaxFileSizeBytes = 4L * 1024 * 1024 * 1024;

        // Mirrors CheckInWorker's private MAX_ACTION_METADATA_VALUE_BYTES.
        private const int MaxMetadataValueBytes = 512;

        // Shared, never-disposed fallback for when IHttpClientFactory isn't registered.
        private static readonly HttpClient FallbackHttpClient = new HttpClient();

        /// <summary>
        /// Run the fetch_logs action.
        /// </summary>
        public static async Task RunAsync<TConfig>(ActionContext<TConfig> ctx, LoggerConfig? loggerConfig, IHttpClientFactory? httpClientFactory, CancellationToken token)
        {
            var fileConfig = loggerConfig?.File;
            if (fileConfig == null || string.IsNullOrWhiteSpace(fileConfig.Path))
            {
                throw new ActionError("no_file_handler_configured", "This extractor is not configured to log to a file.");
            }
            if (ctx.CdfClient == null)
            {
                throw new ActionError("no_cdf_client_configured", "This extractor has no CDF client configured, cannot upload log files.");
            }

            var (start, end) = ParseAndValidateDateRange(ctx.CallMetadata);

            var candidates = GetCandidateFiles(fileConfig, start, end).ToList();
            var fileResults = new List<Dictionary<string, string>>(candidates.Count);
            int uploadedCount = 0, missingCount = 0, skippedTooLargeCount = 0, failedCount = 0;

            for (int i = 0; i < candidates.Count; i++)
            {
                token.ThrowIfCancellationRequested();
                var (path, date, isLive) = candidates[i];

                if (!System.IO.File.Exists(path))
                {
                    // Normal: outside the retention window, or not written yet.
                    missingCount++;
                    fileResults.Add(new Dictionary<string, string> { ["date"] = date, ["status"] = "skipped" });
                    continue;
                }

                using var stream = TryOpenLogFileStream(path, isLive);
                if (stream == null)
                {
                    missingCount++;
                    fileResults.Add(new Dictionary<string, string> { ["date"] = date, ["status"] = "skipped" });
                    continue;
                }

                if (stream.Length > MaxFileSizeBytes)
                {
                    skippedTooLargeCount++;
                    fileResults.Add(new Dictionary<string, string>
                    {
                        ["date"] = date,
                        ["status"] = "skipped_too_large",
                        ["size_bytes"] = stream.Length.ToString(CultureInfo.InvariantCulture),
                    });
                    continue;
                }

                long fileId;
                try
                {
                    fileId = await UploadLogFileAsync(ctx, httpClientFactory, stream, Path.GetFileName(path), date, token).ConfigureAwait(false);
                }
                catch (Exception ex) when (ex is HttpRequestException || ex is ResponseException || ex is TimeoutException
                    || (ex is OperationCanceledException && !token.IsCancellationRequested))
                {
                    // One file's failure must not void the others. OperationCanceledException is
                    // only caught for HttpClient timeouts, not cancellation of the outer token.
                    failedCount++;
                    fileResults.Add(new Dictionary<string, string> { ["date"] = date, ["status"] = "failed", ["error"] = ex.Message });
                    continue;
                }

                uploadedCount++;
                fileResults.Add(new Dictionary<string, string>
                {
                    ["date"] = date,
                    ["status"] = "uploaded",
                    ["id"] = fileId.ToString(CultureInfo.InvariantCulture),
                });
                ctx.ReportProgress($"Uploading: {i + 1}/{candidates.Count} files complete");
            }

            fileResults.Sort((a, b) => string.CompareOrdinal(a["date"], b["date"]));

            var message = $"{uploadedCount} of {candidates.Count} log files uploaded to CDF Files";
            var metadata = new Dictionary<string, string>
            {
                ["total_files"] = candidates.Count.ToString(CultureInfo.InvariantCulture),
                ["uploaded_files"] = uploadedCount.ToString(CultureInfo.InvariantCulture),
                ["missing_files"] = missingCount.ToString(CultureInfo.InvariantCulture),
                ["skipped_too_large_files"] = skippedTooLargeCount.ToString(CultureInfo.InvariantCulture),
                ["failed_files"] = failedCount.ToString(CultureInfo.InvariantCulture),
            };

            // Only attach the per-file breakdown if it fits in one metadata value; the counts
            // above always do.
            var filesJson = JsonSerializer.Serialize(fileResults);
            if (Encoding.UTF8.GetByteCount(filesJson) <= MaxMetadataValueBytes)
            {
                metadata["files"] = filesJson;
            }
            else
            {
                message += " (per-file detail omitted from result metadata -- see extractor logs for full detail)";
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

            // Log files roll on host-local time, so convert UTC inputs before truncating to a date.
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
        /// Enumerate candidate log file paths for the (inclusive) date range, using Serilog's
        /// on-disk naming: "log.txt" with day rolling gives "log20260911.txt" (no separator).
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
        /// Open a log file, bounding reads for live files. Returns null if unavailable.
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
                // Only open failures are swallowed; read and upload failures must propagate.
                return null;
            }
        }

        private static async Task<long> UploadLogFileAsync<TConfig>(
            ActionContext<TConfig> ctx, IHttpClientFactory? httpClientFactory, Stream stream, string fileName, string date, CancellationToken token)
        {
            var length = stream.Length;

            var uploadRead = await ctx.CdfClient!.CogniteClient.Files.UploadAsync(new FileCreate
            {
                ExternalId = $"extractor-logs-{ctx.IntegrationExternalId}-{date}",
                Name = fileName,
                MimeType = "text/plain",
                Source = "extractor",
            }, overwrite: true, token).ConfigureAwait(false);

            using var content = new StreamContent(stream);
            content.Headers.ContentLength = length;
            // Factory clients are safe to dispose; FallbackHttpClient is shared and must not be.
            var httpClient = httpClientFactory?.CreateClient();
            try
            {
                using var response = await (httpClient ?? FallbackHttpClient).PutAsync(uploadRead.UploadUrl, content, token).ConfigureAwait(false);
                response.EnsureSuccessStatusCode();
            }
            finally
            {
                httpClient?.Dispose();
            }

            return uploadRead.Id;
        }
    }
}
