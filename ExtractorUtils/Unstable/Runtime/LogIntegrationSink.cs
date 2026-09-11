using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extractor.Utils.Unstable.Tasks;
using CogniteSdk.Alpha;
using Microsoft.Extensions.Logging;

namespace Cognite.Extractor.Utils.Unstable.Runtime
{
    /// <summary>
    /// An extractor integration sink that simply logs the incoming events.
    /// 
    /// Useful for providing the extractor with a sink, even when it's running without
    /// a configured cognite client.
    /// </summary>
    public class LogIntegrationSink : IIntegrationSink
    {
        private ILogger _logger;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="logger">Logger to write log events to.</param>
        public LogIntegrationSink(ILogger logger)
        {
            _logger = logger;
        }

        /// <inheritdoc />
        public Task Flush(CancellationToken token)
        {
            return Task.CompletedTask;
        }

        /// <inheritdoc />
        public void ReportError(ExtractorError error)
        {
            if (error is null) throw new ArgumentNullException(nameof(error));
            var level = error.Level switch
            {
                ErrorLevel.warning => LogLevel.Warning,
                ErrorLevel.error => LogLevel.Error,
                _ => LogLevel.Critical
            };
            if (error.TaskName != null)
            {
                _logger.Log(level, "Error on task {TaskName}: {Message}. {Details}", error.TaskName, error.Description, error.Details);
            }
            else
            {
                _logger.Log(level, "Error: {Message}. {Details}", error.Description, error.Details);
            }
        }

        /// <inheritdoc />
        public void ReportTaskEnd(string taskName, TaskUpdatePayload? update = null, DateTime? timestamp = null)
        {
            _logger.LogInformation("Task {TaskName} ended", taskName);
        }

        /// <inheritdoc />
        public void ReportTaskStart(string taskName, TaskUpdatePayload? update = null, DateTime? timestamp = null)
        {
            _logger.LogInformation("Task {TaskName} started", taskName);
        }

        /// <inheritdoc />
        public Task RunPeriodicCheckIn(CancellationToken token, StartupRequest startupPayload, TimeSpan? interval = null)
        {
            return Task.Delay(Timeout.InfiniteTimeSpan);
        }

        /// <inheritdoc />
        public void QueueActionUpdate(ActionUpdate update)
        {
            // This sink never talks to a real backend (see class remarks), so it can never
            // receive a pending action to act on in the first place -- there is nothing to do
            // here beyond making that visible in the logs.
            _logger.LogInformation("Action {ExternalId} update queued with status {Status}, but no backend is configured to send it to", update?.ExternalId, update?.Status);
        }

        /// <inheritdoc />
        public void SetActionDispatcher(Func<IReadOnlyList<IntegrationAction>, Task> dispatcher)
        {
            // No-op: since this sink never calls a real checkin/startup endpoint, it will never
            // observe a pending action to hand to this dispatcher.
        }
    }
}