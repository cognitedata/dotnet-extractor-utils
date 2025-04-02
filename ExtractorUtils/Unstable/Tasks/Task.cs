using System;
using System.Threading;
using CogniteSdk.Alpha;

namespace Cognite.Extractor.Utils.Unstable.Tasks
{
    /// <summary>
    /// Payload for task run.
    /// </summary>
    public class TaskUpdatePayload
    {
        /// <summary>
        /// String message.
        /// </summary>
        public string? Message { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="message">Message giving info about task run.</param>
        public TaskUpdatePayload(string? message = null)
        {
            Message = message;
        }
    }

    /// <summary>
    /// Task metadata object.
    /// </summary>
    public class TaskMetadata
    {
        /// <summary>
        /// Type of task.
        /// </summary>
        public TaskType Type { get; private set; }
        /// <summary>
        /// Whether the task can be triggered by an action or not.
        /// </summary>
        public bool Action { get; set; }
        /// <summary>
        /// Task description.
        /// </summary>
        public string? Description { get; set; }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="type">Task type.</param>
        public TaskMetadata(TaskType type)
        {
            Type = type;
        }
    }

    /// <summary>
    /// Type handling reporting of events related to a single task.
    /// </summary>
    public class TaskReporter : BaseErrorReporter
    {
        private string TaskName { get; }

        private IIntegrationSink _sink;

        // Current state, for sanity checking. Integer because .NET does not support atomic operations on bool.
        private int _isRunning;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="taskName">Name of the task, must be unique.</param>
        /// <param name="sink">Sink for task events.</param>
        public TaskReporter(string taskName, IIntegrationSink sink)
        {
            TaskName = taskName;
            _sink = sink;
        }

        /// <inheritdoc />
        public override ExtractorError NewError(ErrorLevel level, string description, string? details = null, DateTime? now = null)
        {
            return new ExtractorError(level, description, _sink, details, TaskName, now);
        }

        /// <summary>
        /// Report that this task has started.
        /// </summary>
        /// <param name="update">Message and context tied to task run</param>
        /// <param name="timestamp"></param>
        public void ReportStart(TaskUpdatePayload? update = null, DateTime? timestamp = null)
        {
            if (Interlocked.CompareExchange(ref _isRunning, 1, 0) == 1) throw new InvalidOperationException("Attempted to start task that is already running");
            _sink.ReportTaskStart(TaskName, update, timestamp);
        }

        /// <summary>
        /// Report that this task has ended.
        /// </summary>
        /// <param name="update">Message and context tied to task run</param>
        /// <param name="timestamp"></param>
        public void ReportEnd(TaskUpdatePayload? update = null, DateTime? timestamp = null)
        {
            if (Interlocked.CompareExchange(ref _isRunning, 0, 1) == 0) throw new InvalidOperationException("Attempted to end task that is not running");
            _sink.ReportTaskEnd(TaskName, update, timestamp);
        }
    }
}