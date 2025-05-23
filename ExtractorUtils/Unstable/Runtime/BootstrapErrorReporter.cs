using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
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

            List<ErrorWithTask> errorsToFlush;
            lock (_lock)
            {
                if (_pendingErrors.Count == 0)
                {
                    return;
                }
                errorsToFlush = _pendingErrors.Values.ToList();
                _pendingErrors.Clear();
            }

            try
            {
                if (_client != null && _integrationId != null)
                {
                    await _client.Alpha.Integrations
                        .CheckInAsync(new CheckInRequest
                        {
                            ExternalId = _integrationId,
                            Errors = errorsToFlush
                        }, token).ConfigureAwait(false);
                }
                else
                {
                    _logger.LogWarning("No integration provided for error reporting. Errors will not be sent to CDF.");
                }
            }
            catch (Exception ex)
            {
                _logger.LogError("Failed to report errors to CDF: {Message}", ex.Message);
                // If flushing failed these will probably never be pushed. For consistency we re-queue them like
                // the normal check-in worker.
                lock (_lock)
                {
                    foreach (var error in errorsToFlush)
                    {
                        if (!_pendingErrors.ContainsKey(error.ExternalId)) _pendingErrors.Add(error.ExternalId, error);
                    }
                }

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
}