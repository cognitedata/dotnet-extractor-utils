using System;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using CogniteSdk;
using Cognite.Extractor.Common;
using Microsoft.Extensions.Logging;

namespace Cognite.Extractor.Utils
{
    ///
    public class RawExtractorManager : IExtractorManager
    {
        private readonly RawManagerConfig _config;

        private readonly CogniteDestination _destination;

        private readonly ILogger<RawExtractorManager> _logger;

        private readonly CancellationTokenSource _source;

        private readonly PeriodicScheduler _scheduler;

        private readonly CronTimeSpanWrapper _cronWrapper;

        internal readonly ExtractorState _state;

        ///
        public TimeSpan Interval { get; set; } = new TimeSpan(0, 0, 5);

        ///
        public TimeSpan Offset { get; set; } = new TimeSpan(0, 0, 3);

        ///
        public TimeSpan InactivityThreshold { get; set; } = new TimeSpan(0, 0, 15);

        ///
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

        ///
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

                Console.WriteLine("\nExtractor " + _config.Index + " waiting to become active... \n");

                List<int> responsiveStandbyExtractors = new List<int>();
                bool activeExtractorResponsive = false;
                foreach (RawExtractorInstance extractor in _state.CurrentState)
                {
                    double timeSinceActive = DateTime.UtcNow.Subtract(extractor.TimeStamp).TotalSeconds;
                    if (timeSinceActive < InactivityThreshold.TotalSeconds)
                    {
                        if (extractor.Active == true) activeExtractorResponsive = true;
                        else responsiveStandbyExtractors.Add(extractor.Key);
                    }

                    Console.WriteLine("Key: " + extractor.Key + ", Active: " + extractor.Active + ", " + +Math.Floor(timeSinceActive) + "s\n");
                }
                if (!activeExtractorResponsive)
                {
                    if (responsiveStandbyExtractors.Count > 0)
                    {
                        responsiveStandbyExtractors.Sort();

                        if (responsiveStandbyExtractors[0] == _config.Index)
                        {
                            Console.WriteLine("\nExtractor " + _config.Index + " is starting... \n");
                            _state.UpdatedStatus = true;
                            break;
                        }
                    }
                }
            }

            UpdateStateAtInterval();
        }

        internal void UpdateStateAtInterval()
        {
            _scheduler.ScheduleTask("Upload log to state", async (token) =>
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
            Console.WriteLine("\nExtractor " + _config.Index + " updating state...\n");

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

                List<IExtractorInstance> extractorInstances = new List<IExtractorInstance>();
                List<int> keys = new List<int>();
                foreach (RawRow<RawLogData> extractor in rows.Items)
                {
                    _logger.LogInformation(extractor.Columns.TimeStamp.ToString());
                    int key = Int16.Parse(extractor.Key);
                    keys.Add(key);

                    RawExtractorInstance instance = new RawExtractorInstance(key, extractor.Columns.TimeStamp, extractor.Columns.Active);
                    extractorInstances.Add(instance);
                }

                if (keys.Count < _state.CurrentState.Count)
                {
                    foreach (RawExtractorInstance extractor in _state.CurrentState)
                    {
                        if (!keys.Contains(extractor.Key))
                        {
                            extractorInstances.Add(extractor);
                            Console.WriteLine("Missing row for extractor with index " + extractor.Key);
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
            List<int> activeExtractors = new List<int>();
            foreach (RawExtractorInstance extractor in _state.CurrentState)
            {
                double timeSinceActive = DateTime.UtcNow.Subtract(extractor.TimeStamp).TotalSeconds;
                if (extractor.Active == true && timeSinceActive < InactivityThreshold.TotalSeconds) activeExtractors.Add(extractor.Key);
            }

            if (activeExtractors.Count > 1)
            {
                activeExtractors.Sort();

                if (activeExtractors[0] != _config.Index)
                {
                    Console.WriteLine("\nMultiple active extractors, turning off extractor " + _config.Index + "\n");
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