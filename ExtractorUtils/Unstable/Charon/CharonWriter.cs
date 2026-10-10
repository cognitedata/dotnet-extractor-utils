using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extensions;
using Cognite.Extractor.Common;
using Cognite.Extractor.Utils.Unstable.Configuration;
using CogniteSdk.Alpha;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Cognite.Extractor.Utils.Unstable.Charon
{
    /// <summary>
    /// Façade extractor authors use to write through Charon: declare the task set, register it with
    /// Charon at startup, and create upload queues bound to those tasks.
    /// </summary>
    public class CharonWriter
    {
        private readonly ICharonClient _client;
        private readonly string _integrationId;
        private readonly BaseCogniteConfig _cogniteConfig;
        private readonly ILogger<CharonWriter> _logger;
        private readonly List<CharonSetupItem> _tasks = new List<CharonSetupItem>();
        private readonly HashSet<string> _taskNames = new HashSet<string>();

        /// <summary>
        /// Create a Charon writer.
        /// </summary>
        /// <param name="client">Charon client.</param>
        /// <param name="connectionConfig">Connection config supplying the integration external id.</param>
        /// <param name="cogniteConfig">Cognite config supplying CDF chunk limits.</param>
        /// <param name="logger">Logger.</param>
        /// <exception cref="ConfigurationException">Thrown if no integration external id is configured.</exception>
        public CharonWriter(
            ICharonClient client,
            ConnectionConfig connectionConfig,
            BaseCogniteConfig cogniteConfig,
            ILogger<CharonWriter>? logger)
        {
            _client = client ?? throw new ArgumentNullException(nameof(client));
            if (connectionConfig == null) throw new ArgumentNullException(nameof(connectionConfig));
            _cogniteConfig = cogniteConfig ?? throw new ArgumentNullException(nameof(cogniteConfig));
            _logger = logger ?? new NullLogger<CharonWriter>();
            var integrationId = connectionConfig.Integration?.ExternalId?.TrimToNull();
            if (integrationId == null)
            {
                throw new ConfigurationException("Charon CDF-writer requires an integration external id to be configured");
            }
            _integrationId = integrationId;
        }

        /// <summary>
        /// Register a task to be sent to Charon at setup. Build the item with <see cref="CharonTask"/>.
        /// </summary>
        /// <param name="item">Task registration.</param>
        /// <exception cref="InvalidOperationException">Thrown if a task with the same name is already registered.</exception>
        public void RegisterTask(CharonSetupItem item)
        {
            if (item == null) throw new ArgumentNullException(nameof(item));
            if (!_taskNames.Add(item.TaskName))
            {
                throw new InvalidOperationException($"A Charon task named '{item.TaskName}' is already registered");
            }
            _tasks.Add(item);
        }

        /// <summary>
        /// Register multiple tasks.
        /// </summary>
        /// <param name="items">Task registrations.</param>
        public void RegisterTasks(IEnumerable<CharonSetupItem> items)
        {
            if (items == null) throw new ArgumentNullException(nameof(items));
            foreach (var item in items) RegisterTask(item);
        }

        /// <summary>
        /// Register the full task set with Charon (<c>/cdfwriter/setup</c>). Call once at startup,
        /// typically from the extractor's task-init step.
        /// </summary>
        /// <param name="version">Extractor id/version to report.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>A task that completes when setup succeeds.</returns>
        /// <exception cref="CharonSetupException">Thrown with every failure if Charon rejects the setup.</exception>
        public async Task SetupAsync(ExtractorId version, CancellationToken token)
        {
            if (version == null) throw new ArgumentNullException(nameof(version));
            var request = new CharonSetupRequest
            {
                IntegrationId = _integrationId,
                ExtractorType = version.ExternalId ?? string.Empty,
                ExtractorVersion = version.Version ?? string.Empty,
                Items = new List<CharonSetupItem>(_tasks),
            };
            _logger.LogInformation("Registering {Count} task(s) with Charon for integration {IntegrationId}",
                request.Items.Count, _integrationId);
            await _client.SetupAsync(request, token).ConfigureAwait(false);
        }

        /// <summary>
        /// Create an upload queue for a timeseries create/update task.
        /// </summary>
        /// <param name="taskName">Registered task name.</param>
        /// <param name="interval">Max interval between uploads.</param>
        /// <param name="maxSize">Max queue size before an upload is forced.</param>
        /// <param name="logger">Logger for the queue.</param>
        /// <param name="callback">Optional callback after each upload.</param>
        /// <returns>A new upload queue.</returns>
        public CharonUploadQueue CreateQueue(
            string taskName,
            TimeSpan interval,
            int maxSize,
            ILogger logger,
            Func<QueueUploadResult<JsonElement>, Task>? callback = null)
        {
            return new CharonUploadQueue(_client, _integrationId, taskName, interval, maxSize,
                _cogniteConfig.CdfChunking.TimeSeries, logger, callback);
        }

        /// <summary>
        /// Create an upload queue for a datapoints task.
        /// </summary>
        /// <param name="taskName">Registered task name.</param>
        /// <param name="interval">Max interval between uploads.</param>
        /// <param name="maxSize">Max queue size before an upload is forced.</param>
        /// <param name="logger">Logger for the queue.</param>
        /// <param name="callback">Optional callback after each upload.</param>
        /// <returns>A new datapoints upload queue.</returns>
        public CharonDatapointsUploadQueue CreateDatapointsQueue(
            string taskName,
            TimeSpan interval,
            int maxSize,
            ILogger logger,
            Func<QueueUploadResult<(CogniteSdk.Identity id, Datapoint dp)>, Task>? callback = null)
        {
            return new CharonDatapointsUploadQueue(_client, _integrationId, taskName, interval, maxSize,
                _cogniteConfig.CdfChunking.DataPoints, _cogniteConfig.CdfChunking.DataPointTimeSeries, logger, callback);
        }
    }
}
