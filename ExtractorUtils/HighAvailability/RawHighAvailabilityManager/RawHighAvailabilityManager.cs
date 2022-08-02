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
    public class RawHighAvailabilityManager : IHighAvailabilityManager
    {
        private readonly HighAvailabilityConfig _config;

        private readonly CogniteDestination _destination;

        private readonly ILogger<RawHighAvailabilityManager> _logger;

        private readonly PeriodicScheduler _scheduler;

        private readonly CancellationTokenSource _source;

        private readonly CronTimeSpanWrapper _cronWrapper;

        internal readonly ExtractorState _state;

        private readonly TimeSpan? _interval;

        private readonly TimeSpan _offset = new TimeSpan(0, 0, 3);

        private readonly TimeSpan _inactivityThreshold = new TimeSpan(0, 0, 100);

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
            ILogger<RawHighAvailabilityManager> logger,
            PeriodicScheduler scheduler,
            CancellationTokenSource source,
            TimeSpan? interval = null,
            TimeSpan? inactivityThreshold = null)
        {
            _config = config;
            _destination = destination;
            _logger = logger;
            _scheduler = scheduler;
            _source = source;
            _cronWrapper = new CronTimeSpanWrapper(true, true, "s", "1");
            _state = new ExtractorState();
            _interval = interval;
            if (inactivityThreshold != null) _inactivityThreshold = (TimeSpan)inactivityThreshold;
            SetCronWrapperRawValue();
        }

        /// <summary>
        /// Method used to add high availability to an extractor.
        /// Will update the extractor state at an interval and check whether it
        /// should become active. If the extractor becomes active it will 
        /// start a periodic task that will continue updating the state.
        /// </summary>
        /// <returns></returns>
        public async Task WaitToBecomeActive()
        {
            bool firstRun = true;
            while (!_source.IsCancellationRequested)
            {
                await Task.Delay(_cronWrapper.Value, _source.Token).ConfigureAwait(false);

                await UpdateState().ConfigureAwait(false);

                if (firstRun)
                {
                    firstRun = false;
                    continue;
                }

                if (ShouldBecomeActive())
                {
                    _state.UpdatedStatus = true;
                    break;
                }
            }

            UpdateStateAtInterval();
        }

        internal bool ShouldBecomeActive()
        {            
            var now = DateTime.UtcNow;

            // Checking if there is currently an active extractor.
            // An extractor is considered active if it is marked as active and it has been responsive within the inactivty threshold.
            bool activeExtractor = _state.CurrentState
                .Any(extractor => extractor.Active && IsResponsive(extractor.TimeStamp, now));
                
            var responsiveStandbyExtractors = _state.CurrentState
                .Where(extractor => !extractor.Active && IsResponsive(extractor.TimeStamp, now))
                .Select(extractor => extractor.Index);
                    
            // If there are no active extractors, start the standby extractor with highest priority.
            if (!activeExtractor && responsiveStandbyExtractors.Any() && responsiveStandbyExtractors.Min() == _config.Index)
            {
                _logger.LogInformation("Extractor is starting.");
                return true;
            }

            _logger.LogTrace("Waiting to become active.");
            return false;
        }


        internal void UpdateStateAtInterval()
        {
            _scheduler.SchedulePeriodicTask("Updating state", _cronWrapper, async (token) => await UpdateState().ConfigureAwait(false));
        }

        // Uploading log to state, updating local state and checking for multiple active extractors.
        internal async Task UpdateState()
        {
            await UploadLogToState().ConfigureAwait(false);
            await UpdateExtractorState().ConfigureAwait(false);
            CheckForMultipleActiveExtractors();
        }

        internal async Task UploadLogToState()
        {
            var log = new RawLogData(DateTime.UtcNow, _state.UpdatedStatus);

            var rows = new List<RawRowCreate<RawLogData>>() {
                new RawRowCreate<RawLogData>() { Key = _config.Index.ToString(), Columns = log }
            };

            try
            {
                await _destination.CogniteClient.Raw.CreateRowsAsync<RawLogData>(_config.Raw?.DatabaseName, _config.Raw?.TableName, rows, ensureParent: true).ConfigureAwait(false);
                _logger.LogInformation("State has been updated.");
            }
            catch (Exception ex)
            {
                _logger.LogError("Error when uploading log to state: {Message}", ex.Message);
            }
        }

        internal async Task UpdateExtractorState()
        {
            try
            {
                var rows = await _destination.CogniteClient.Raw.ListRowsAsync<RawLogData>(_config.Raw?.DatabaseName, _config.Raw?.TableName).ConfigureAwait(false);

                // Converting Raw data to ExtractorInstance object.
                var extractorInstances = new List<IExtractorInstance>();
                foreach (var extractor in rows.Items)
                {
                    var instance = new RawExtractorInstance(Int32.Parse(extractor.Key), extractor.Columns.TimeStamp, extractor.Columns.Active);
                    extractorInstances.Add(instance);
                }

                // Checking if a row is missing from the Raw database.
                var keys = rows.Items.Select(extractor => Int32.Parse(extractor.Key));
                if (extractorInstances.Count < _state.CurrentState.Count)
                {
                    foreach(var extractor in _state.CurrentState)
                    {
                        // Adding missing row.
                        if (!keys.Contains(extractor.Index)) extractorInstances.Add(extractor);
                    };
                }

                // Updating state.
                _state.CurrentState = extractorInstances;
            }
            catch (Exception ex)
            {
                _logger.LogError("Error when updating state: {Message}", ex.Message);
            }
        }

        internal void CheckForMultipleActiveExtractors()
        {
            var now = DateTime.UtcNow;

            // Creating a list of all the active extractors.
            var activeExtractors = _state.CurrentState
                .Where(extractor => extractor.Active && IsResponsive(extractor.TimeStamp, now))
                .Select(extractor => extractor.Index);

            // Turning off extractor if there are multiple active and it does not have the highest priority.
            if (activeExtractors.Count() > 1 && activeExtractors.Contains(_config.Index) && (activeExtractors.Min() != _config.Index))
            {
                _logger.LogInformation("Turning off extractor.");
                _source.Cancel();
            }
        }
        
        private bool IsResponsive(DateTime lastActive, DateTime now) => now.Subtract(lastActive) < _inactivityThreshold;

        private void SetCronWrapperRawValue()
        {
            int offset = (int)_offset.TotalSeconds * _config.Index;

            if (_interval != null)
            {
                var interval = (TimeSpan)_interval;
                int value = (int)interval.TotalSeconds;
                _cronWrapper.RawValue = $"{offset}/{value} * * * * *";
            }
            else
            {
                _cronWrapper.RawValue = $"{offset} * * * * *";
            }
        }
    }
}