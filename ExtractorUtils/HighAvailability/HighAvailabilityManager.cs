using System;
using System.Threading;
using System.Linq;
using System.Threading.Tasks;
using Cognite.Extractor.Common;
using Microsoft.Extensions.Logging;

namespace Cognite.Extractor.Utils
{
    /// <summary>
    /// Abstract class for a high availability manager.
    /// </summary>
    public abstract class HighAvailabilityManager : IHighAvailabilityManager
    {
        internal readonly HighAvailabilityConfig _config;
        internal readonly CogniteDestination _destination;
        internal readonly ILogger<HighAvailabilityManager> _logger;
        private readonly PeriodicScheduler _scheduler;
        private readonly CancellationTokenSource _source;
        internal readonly ExtractorState _state;
        private readonly CronTimeSpanWrapper _cronWrapper;
        private readonly TimeSpan _offset = TimeSpan.FromSeconds(3);
        private readonly TimeSpan _inactivityThreshold = TimeSpan.FromSeconds(150);

        internal HighAvailabilityManager(
            HighAvailabilityConfig config,
            CogniteDestination destination,
            ILogger<HighAvailabilityManager> logger,
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
            _state = new ExtractorState();
            _cronWrapper = HighAvailabilityUtils.CreateCronWrapper(_config.Index, _offset, interval);
            if (inactivityThreshold != null) _inactivityThreshold = (TimeSpan)inactivityThreshold;
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

            if (!activeExtractor)
            {
                var responsiveStandbyExtractors = _state.CurrentState
                    .Where(extractor => !extractor.Active && IsResponsive(extractor.TimeStamp, now))
                    .Select(extractor => extractor.Index);
                        
                // If there are no active extractors, start the standby extractor with highest priority.
                if (responsiveStandbyExtractors.Any() && responsiveStandbyExtractors.Min() == _config.Index)
                {
                    _logger.LogInformation("Extractor is starting.");
                    return true;
                }
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

        internal void CheckForMultipleActiveExtractors()
        {
            var now = DateTime.UtcNow;

            // Creating a list of all the active extractors.
            var activeExtractors = _state.CurrentState
                .Where(extractor => extractor.Active && IsResponsive(extractor.TimeStamp, now))
                .Select(extractor => extractor.Index)
                .ToList();

            // Turning off extractor if there are multiple active and it does not have the highest priority.
            if (activeExtractors.Count > 1 && activeExtractors.Contains(_config.Index) && (activeExtractors.Min() != _config.Index))
            {
                _logger.LogInformation("Turning off extractor.");
                _source.Cancel();
            }
        }

        internal abstract Task UploadLogToState();

        internal abstract Task UpdateExtractorState();

        private bool IsResponsive(DateTime lastActive, DateTime now) => now.Subtract(lastActive) < _inactivityThreshold;
    }
}