using System;
using System.Threading;
using System.Linq;
using System.Threading.Tasks;
using System.Collections.Generic;
using CogniteSdk;
using Cognite.Extractor.Common;
using Microsoft.Extensions.Logging;

namespace Cognite.Extractor.Utils
{
    /// <summary>
    /// Class used to manage an extractor using a Raw database.
    /// </summary>
    public class RawHighAvailabilityManager : HighAvailabilityManager
    {
        private readonly CogniteDestination _destination;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="config">Configuration object.</param>
        /// <param name="destination">Cognite destination.</param>
        /// <param name="logger">Logger.</param>
        /// <param name="scheduler">Scheduler.</param>
        /// <param name="source">CancellationToken source.</param>
        /// <param name="interval">Optional update state interval.</param>
        /// <param name="inactivityThreshold">Optional threshold for extractor being inactive.</param>
        public RawHighAvailabilityManager(
            HighAvailabilityConfig config, 
            CogniteDestination destination,
            ILogger<HighAvailabilityManager> logger,
            PeriodicScheduler scheduler,
            CancellationTokenSource source,
            TimeSpan? interval = null,
            TimeSpan? inactivityThreshold = null) 
            : base(config, logger, scheduler, source, interval, inactivityThreshold)
        {   
            _destination = destination;
        }

        internal override async Task UploadLogToState()
        {
            var now = DateTime.UtcNow;
            var log = new RawLogData(now, _state.UpdatedStatus);
            var state = new RawExtractorInstance(_config.Index, now, _state.UpdatedStatus);

            _state.CurrentState[_config.Index] = state;

            var rows = new List<RawRowCreate<RawLogData>>() {
                new RawRowCreate<RawLogData>() { Key = _config.Index.ToString(), Columns = log }
            };

            try
            {
                await _destination.CogniteClient.Raw.CreateRowsAsync<RawLogData>(_config.Raw?.DatabaseName, _config.Raw?.TableName, rows, ensureParent: true).ConfigureAwait(false);
                _logger.LogTrace("State has been updated.");
            }
            catch (Exception ex)
            {
                _logger.LogError("Error when uploading log to state: {Message}", ex.Message);
            }
        }

        internal override async Task UpdateExtractorState()
        {
            try
            {
                var rows = await _destination.CogniteClient.Raw.ListRowsAsync<RawLogData>(_config.Raw?.DatabaseName, _config.Raw?.TableName).ConfigureAwait(false);

                // Converting Raw data to ExtractorInstance object.
                foreach (var extractor in rows.Items)
                {
                    var instance = new RawExtractorInstance(int.Parse(extractor.Key), extractor.Columns.TimeStamp, extractor.Columns.Active);
                    if (instance.Index != _config.Index)
                    {
                        _state.CurrentState[instance.Index] = instance;
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError("Error when updating state: {Message}", ex.Message);
            }
        }
    }
}