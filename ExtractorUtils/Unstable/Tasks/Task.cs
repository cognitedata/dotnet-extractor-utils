using System;
using CogniteSdk.Alpha;

namespace Cognite.ExtractorUtils.Unstable.Tasks
{
    /// <summary>
    /// Type handling reporting of events related to a single task.
    /// </summary>
    public class TaskReporter : BaseErrorReporter
    {
        private string TaskName { get; }

        private IIntegrationSink _sink;

        // Current state, for sanity checking.
        private bool _isRunning;

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
        /// <param name="timestamp"></param>
        public void ReportStart(DateTime? timestamp = null)
        {
            if (_isRunning) throw new InvalidOperationException("Attempted to start task that is already running");
            _isRunning = true;
            _sink.ReportTaskStart(TaskName, timestamp);
        }

        /// <summary>
        /// Report that this task has ended.
        /// </summary>
        /// <param name="timestamp"></param>
        public void ReportEnd(DateTime? timestamp = null)
        {
            if (!_isRunning) throw new InvalidOperationException("Attempted to end task that is not running");
            _isRunning = false;
            _sink.ReportTaskEnd(TaskName, timestamp);
        }
    }
}