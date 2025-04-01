using System;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extractor.Common;
using CogniteSdk.Alpha;

namespace Cognite.Extractor.Utils.Unstable.Tasks
{
    /// <summary>
    /// Utility type for reporting errors to integrations.
    /// 
    /// After creation you should immediately either `Dispose`,
    /// call `Finish`, or call `Instant`.
    /// 
    /// Typically created through utility methods on `IErrorReporter`
    /// </summary>
    public class ExtractorError : IDisposable
    {
        /// <summary>
        /// Error level.
        /// </summary>
        public ErrorLevel Level { get; }
        /// <summary>
        /// Generated error external ID.
        /// </summary>
        public string ExternalId { get; }
        /// <summary>
        /// Short error description.
        /// </summary>
        public string Description { get; }
        /// <summary>
        /// Task that generated this error.
        /// 
        /// If left out, the error is assigned to the extractor itself.
        /// </summary>
        public string? TaskName { get; }
        /// <summary>
        /// Long error details.
        /// </summary>
        public string? Details { get; set; }

        /// <summary>
        /// Time the error started.
        /// </summary>
        public DateTime StartTime { get; }
        /// <summary>
        /// Time the error ended, if it has ended yet.
        /// </summary>
        public DateTime? EndTime { get; private set; }

        private IIntegrationSink _sink;
        private bool disposedValue;


        /// <summary>
        /// Create a new extractor error.
        /// </summary>
        /// <param name="level">Error level.</param>
        /// <param name="description">Short error description.</param>
        /// <param name="sink">Error sink this is written to.</param>
        /// <param name="details">Long error details.</param>
        /// <param name="taskName">Task that generated this error. If left out, the error is assigned to the extractor itself.</param>
        /// <param name="now">Current time, for synchronization. Defaults to DateTime.UtcNow</param>
        public ExtractorError(
            ErrorLevel level,
            string description,
            IIntegrationSink sink,
            string? details = null,
            string? taskName = null,
            DateTime? now = null)
        {
            Level = level;
            Description = description;
            _sink = sink;
            Details = details;
            TaskName = taskName;
            StartTime = now ?? DateTime.UtcNow;
            ExternalId = Guid.NewGuid().ToString();

            _sink.ReportError(this);
        }

        /// <summary>
        /// Immediately mark this error as completed.
        /// </summary>
        public void Instant()
        {
            if (EndTime != null) return;

            EndTime = StartTime;

            // The error might have been reported already, so try re-adding it.
            _sink.ReportError(this);
        }

        /// <summary>
        /// Mark this error as completed and report it.
        /// </summary>
        /// <param name="now">Current time, for synchronization. Defaults to DateTime.UtcNow</param>
        public void Finish(DateTime? now = null)
        {
            if (EndTime != null) return;

            EndTime = now ?? DateTime.UtcNow;
            // The error might have been reported already, so try re-adding it.
            _sink.ReportError(this);
        }

        /// <summary>
        /// Return an ErrorWithTask for writing to the integrations API.
        /// </summary>
        /// <returns>ErrorWithTask object</returns>
        public ErrorWithTask ToSdk()
        {
            return new ErrorWithTask
            {
                ExternalId = ExternalId,
                Level = Level,
                Description = Description,
                Details = Details,
                Task = TaskName,
                StartTime = StartTime.ToUnixTimeMilliseconds(),
                EndTime = EndTime?.ToUnixTimeMilliseconds(),
            };
        }

        /// <summary>
        /// Dispose the error, just calls `Finish`.
        /// </summary>
        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    Finish();
                }

                disposedValue = true;
            }
        }
        /// <summary>
        /// Dispose the error, just calls `Finish`.
        /// </summary>
        public void Dispose()
        {
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }
    }

    /// <summary>
    /// Interface for types that can consume errors and write them to some sink (integrations API).
    /// </summary>
    public interface IIntegrationSink
    {
        /// <summary>
        /// Write an error to the sink.
        /// </summary>
        /// <param name="error"></param>
        void ReportError(ExtractorError error);

        /// <summary>
        /// Report that a task has ended.
        /// </summary>
        /// <param name="taskName">Name of task that ended.</param>
        /// <param name="update">Content of the task update.</param>
        /// <param name="timestamp">When the task ended, defaults to current time.</param>
        void ReportTaskEnd(string taskName, TaskUpdatePayload? update = null, DateTime? timestamp = null);

        /// <summary>
        /// Report that a task has started.
        /// </summary>
        /// <param name="taskName">Name of task that started.</param>
        /// <param name="update">Content of the task update.</param>
        /// <param name="timestamp">When the task started, defaults to current time.</param>
        void ReportTaskStart(string taskName, TaskUpdatePayload? update = null, DateTime? timestamp = null);

        /// <summary>
        /// Flush the sink, ensuring all errors and tasks are written.
        /// </summary>
        /// <param name="token"></param>
        /// <returns></returns>
        Task Flush(CancellationToken token);

        /// <summary>
        /// Run the sink, automatically flushing at regular intervals.
        /// </summary>
        /// <param name="token">Cancellation token.</param>
        /// <param name="startupPayload">Payload to send to the startup endpoint before beginning to
        /// report periodic check-ins.</param>
        /// <param name="interval">Interval. If left out, uses an implementation-defined default.</param>
        /// <returns></returns>
        Task RunPeriodicCheckIn(CancellationToken token, StartupRequest startupPayload, TimeSpan? interval = null);
    }

    /// <summary>
    /// Base class for classes that report errors to integrations,
    /// implemented by tasks and extractors respectively.
    /// </summary>
    public abstract class BaseErrorReporter
    {
        /// <summary>
        /// Create a new extractor error belonging to this reporter.
        /// </summary>
        /// <param name="level">Error level.</param>
        /// <param name="description">Short error description.</param>
        /// <param name="details">Long error details.</param>
        /// <param name="now">Optional current timestamp.</param>
        /// <returns></returns>
        public abstract ExtractorError NewError(
            ErrorLevel level,
            string description,
            string? details = null,
            DateTime? now = null
        );

        /// <summary>
        /// Begin a new warning starting now.
        /// </summary>
        /// <param name="description">Warning description.</param>
        /// <param name="details">Long warning details.</param>
        /// <param name="now">Current time, for synchronization. Defaults to DateTime.UtcNow</param>
        /// <returns>An error that should be completed later.</returns>
        public ExtractorError BeginWarning(string description, string? details = null, DateTime? now = null)
        {
            return NewError(ErrorLevel.warning, description, details, now);
        }

        /// <summary>
        /// Report a warning that starts and ends now.
        /// </summary>
        /// <param name="description">Warning description.</param>
        /// <param name="details">Long warning details.</param>
        /// <param name="now">Current time, for synchronization. Defaults to DateTime.UtcNow</param>
        public void Warning(string description, string? details = null, DateTime? now = null)
        {
            BeginWarning(description, details, now).Instant();
        }

        /// <summary>
        /// Begin a new error starting now.
        /// </summary>
        /// <param name="description">Error description.</param>
        /// <param name="details">Long error details.</param>
        /// <param name="now">Current time, for synchronization. Defaults to DateTime.UtcNow</param>
        /// <returns>An error that should be completed later.</returns>
        public ExtractorError BeginError(string description, string? details = null, DateTime? now = null)
        {
            return NewError(ErrorLevel.error, description, details, now);
        }

        /// <summary>
        /// Report an error that starts and ends now.
        /// </summary>
        /// <param name="description">Error description.</param>
        /// <param name="details">Long error details.</param>
        /// <param name="now">Current time, for synchronization. Defaults to DateTime.UtcNow</param>
        public void Error(string description, string? details = null, DateTime? now = null)
        {
            BeginError(description, details, now).Instant();
        }

        /// <summary>
        /// Begin a new fatal error starting now.
        /// </summary>
        /// <param name="description">Fatal error description.</param>
        /// <param name="details">Long fatal error details.</param>
        /// <param name="now">Current time, for synchronization. Defaults to DateTime.UtcNow</param>
        /// <returns>An error that should be completed later.</returns>
        public ExtractorError BeginFatal(string description, string? details = null, DateTime? now = null)
        {
            return NewError(ErrorLevel.fatal, description, details, now);
        }

        /// <summary>
        /// Report a fatal error that starts and ends now.
        /// </summary>
        /// <param name="description">Fatal error description.</param>
        /// <param name="details">Long fatal error details.</param>
        /// <param name="now">Current time, for synchronization. Defaults to DateTime.UtcNow</param>
        public void Fatal(string description, string? details = null, DateTime? now = null)
        {
            BeginFatal(description, details, now).Instant();
        }
    }
}