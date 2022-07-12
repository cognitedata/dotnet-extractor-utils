using System;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using CogniteSdk;
using Cognite.Extractor.Common;
using Microsoft.Extensions.Logging;

namespace Cognite.Extractor.Utils
{
    /// <summary>
    /// Implementation of an ExtractorManager in Raw
    /// </summary>
    public class RawExtractorManager : IExtractorManager
    {
        private readonly RawManagerConfig _config;

        private readonly CogniteDestination _destination;

        private readonly ILogger<RawExtractorManager> _logger;

        private readonly CancellationTokenSource _source;

        private readonly PeriodicScheduler _scheduler;

        private readonly CronTimeSpanWrapper _cronWrapper;

        internal readonly ExtractorState _state;

        /// <summary>
        /// The time interval the state should be updated at
        /// </summary>
        public TimeSpan Interval { get; set; } = new TimeSpan(0, 0, 5);

        /// <summary>
        /// The time offset between each extracor used in the CronTimeSpanWrapper
        /// </summary>
        public TimeSpan Offset { get; set; } = new TimeSpan(0, 0, 3);

        /// <summary>
        /// The minimum time threshold for an extractor to be considered unresponsive
        /// </summary>
        public TimeSpan InactivityThreshold { get; set; } = new TimeSpan(0, 0, 15);

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="config">Configuration object</param>
        /// <param name="destination">Cognite destination</param>
        /// <param name="logger">Logger</param>
        /// <param name="scheduler">Scheduler</param>
        /// <param name="source">CancellationToken source</param>
        public RawExtractorManager(
            RawManagerConfig config,
            CogniteDestination destination,
            ILogger<RawExtractorManager> logger,
            PeriodicScheduler scheduler,
            CancellationTokenSource source)
        {
            _config = config;
            _destination = destination;
            _logger = logger;
            _source = source;
            _scheduler = scheduler;
            _cronWrapper = new CronTimeSpanWrapper(true, true, "s", "1");
            _state = new ExtractorState();

            SetCronWrapperRawValue();
        }

        /// <summary>
        /// Method used to add high availability to an extractor.
        /// Will update the extractor state at an interval and check whether the
        /// given extractor should become active.
        /// If the given extractor becomes active it will start a periodic task that will
        /// continue updating the state at the same interval.
        /// </summary>
        /// <returns></returns>
        public async Task WaitToBecomeActive()
        {
            bool firstRun = true;
            while (!_source.IsCancellationRequested)
            {
                await Task.Delay(_cronWrapper.Value).ConfigureAwait(false);
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
            //Checking if there is currently an active extractor
            List<int> responsiveStandbyExtractors = new List<int>();
            bool activeExtractorResponsive = false;
            foreach (RawExtractorInstance extractor in _state.CurrentState)
            {
                double timeSinceActive = DateTime.UtcNow.Subtract(extractor.TimeStamp).TotalSeconds;
                if (timeSinceActive < InactivityThreshold.TotalSeconds)
                {
                    if (extractor.Active) activeExtractorResponsive = true;
                    else responsiveStandbyExtractors.Add(extractor.Index);
                }
            }

            //If there are no active extractors, start the standby extractor with highest priority
            if (!activeExtractorResponsive && responsiveStandbyExtractors.Count > 0)
            {
                responsiveStandbyExtractors.Sort();

                if (responsiveStandbyExtractors[0] == _config.Index)
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
            _scheduler.ScheduleTask("Updating state", async (token) =>
            {
                while (!_source.IsCancellationRequested)
                {
                    await Task.Delay(_cronWrapper.Value, token).ConfigureAwait(false);
                    await UpdateState().ConfigureAwait(false);
                }
            });
        }

        internal async Task UpdateState()
        {
            await UploadLogToState().ConfigureAwait(false);
            await UpdateExtractorState().ConfigureAwait(false);

            CheckForMultipleActiveExtractors();
        }

        internal async Task UploadLogToState()
        {
            RawLogData log = new RawLogData(DateTime.UtcNow, _state.UpdatedStatus);
            RawRowCreate<RawLogData> row = new RawRowCreate<RawLogData>() { Key = _config.Index.ToString(), Columns = log };
            List<RawRowCreate<RawLogData>> rows = new List<RawRowCreate<RawLogData>>() { row };

            try
            {
                await _destination.CogniteClient.Raw.CreateRowsAsync<RawLogData>(_config.DatabaseName, _config.TableName, rows, ensureParent: true).ConfigureAwait(false);
                _logger.LogTrace("State has been updated.");
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
                ItemsWithCursor<RawRow<RawLogData>> rows = await _destination.CogniteClient.Raw.ListRowsAsync<RawLogData>(_config.DatabaseName, _config.TableName).ConfigureAwait(false);

                List<int> keys = new List<int>();
                List<IExtractorInstance> extractorInstances = new List<IExtractorInstance>();
                foreach (RawRow<RawLogData> extractor in rows.Items)
                {
                    int key = Int16.Parse(extractor.Key);
                    keys.Add(key);

                    RawExtractorInstance instance = new RawExtractorInstance(key, extractor.Columns.TimeStamp, extractor.Columns.Active);
                    extractorInstances.Add(instance);
                }

                //Checking if a row is missing from the Raw database
                if (keys.Count < _state.CurrentState.Count)
                {
                    foreach (RawExtractorInstance extractor in _state.CurrentState)
                    {
                        //Adding the missing row from the current state
                        if (!keys.Contains(extractor.Index))
                        {
                            extractorInstances.Add(extractor);
                        }
                    }
                }

                _state.CurrentState = extractorInstances;
            }
            catch (Exception ex)
            {
                _logger.LogError("Error when updating state: {Message}", ex.Message);
            }
        }

        internal void CheckForMultipleActiveExtractors()
        {
            //Creating a list of all the active extractors
            List<int> activeExtractors = new List<int>();
            foreach (RawExtractorInstance extractor in _state.CurrentState)
            {
                double timeSinceActive = DateTime.UtcNow.Subtract(extractor.TimeStamp).TotalSeconds;
                if (extractor.Active && timeSinceActive < InactivityThreshold.TotalSeconds) activeExtractors.Add(extractor.Index);
            }

            if (activeExtractors.Count > 1)
            {
                activeExtractors.Sort();

                //Turning off extractor if it does not have the highest priority
                if (activeExtractors[0] != _config.Index)
                {
                    _logger.LogInformation("Turning off extractor.");
                    _source.Cancel();
                }
            }
        }

        internal void SetCronWrapperRawValue()
        {
            int offset = (int)Offset.TotalSeconds * _config.Index;
            int interval = (int)Interval.TotalSeconds;
            _cronWrapper.RawValue = $"{offset}/{interval} * * * * *";
        }
    }
}