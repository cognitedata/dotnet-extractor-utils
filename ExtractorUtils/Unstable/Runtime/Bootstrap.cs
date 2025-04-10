using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extractor.Common;
using Cognite.Extractor.Utils.Unstable.Tasks;
using CogniteSdk;
using CogniteSdk.Alpha;
using Microsoft.Extensions.Logging;

namespace Cognite.Extractor.Utils.Unstable.Runtime
{
    /// <summary>
    /// Error reporter used during runtime bootstrap.
    /// </summary>
    internal class BootstrapErrorReporter : BaseErrorReporter, IIntegrationSink
    {
        private readonly Dictionary<string, ErrorWithTask> _pendingErrors = new Dictionary<string, ErrorWithTask>();
        private Client? _client;
        private string? _integrationId;
        private ILogger _logger;
        private object _lock = new object();

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="client">Cognite client without extra configuration used during bootstrap.
        /// If this is not provided, errors will just be logged.</param>
        /// <param name="integrationId">Integration ID</param>
        /// <param name="logger">Logger for errors</param>
        public BootstrapErrorReporter(Client? client, string? integrationId, ILogger logger)
        {
            _client = client;
            _integrationId = integrationId;
            _logger = logger;
        }


        /// <inheritdoc />
        public async Task Flush(CancellationToken token)
        {
            if (_pendingErrors.Count == 0)
            {
                return;
            }

            try
            {
                if (_client != null && _integrationId != null)
                {
                    await _client.Alpha.Integrations
                        .CheckInAsync(new CheckInRequest
                        {
                            ExternalId = _integrationId,
                            Errors = _pendingErrors.Values.ToList()
                        }, token).ConfigureAwait(false);
                }
                else
                {
                    _logger.LogWarning("No integration provided for error reporting. Errors will not be sent to CDF.");
                }

                _pendingErrors.Clear();
            }
            catch (Exception ex)
            {
                _logger.LogError("Failed to report errors to CDF: {Message}", ex.Message);
            }
        }

        /// <inheritdoc />
        public override ExtractorError NewError(ErrorLevel level, string description, string? details = null, DateTime? now = null)
        {
            return new ExtractorError(level, description, this, details, null, now);
        }

        /// <inheritdoc />
        public void ReportError(ExtractorError error)
        {
            if (error == null) throw new ArgumentNullException(nameof(error));

            lock (_lock)
            {
                _pendingErrors[error.ExternalId] = error.ToSdk();
            }
        }

        /// <inheritdoc />
        public void ReportTaskEnd(string taskName, TaskUpdatePayload? update = null, DateTime? timestamp = null)
        {
            throw new InvalidOperationException("Attempted to report a task end during bootstrap");
        }

        /// <inheritdoc />
        public void ReportTaskStart(string taskName, TaskUpdatePayload? update = null, DateTime? timestamp = null)
        {
            throw new InvalidOperationException("Attempted to report a task start during bootstrap");
        }

        /// <inheritdoc />
        public Task RunPeriodicCheckIn(CancellationToken token, StartupRequest startupPayload, TimeSpan? interval = null)
        {
            throw new InvalidOperationException("Attempted to start periodic checkin during bootstrap");
        }
    }

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
    }
}