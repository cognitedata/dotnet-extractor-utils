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
    /// General interface for an extractor manager
    /// </summary>
    public interface IExtractorManager 
    {
        /// <summary>
        /// Method called by standby extractor to wait until it should become active
        /// </summary>
        public Task WaitToBecomeActive(); 
    }

    internal interface IExtractorInstance
    {
        int Key { get; set; }
        DateTime TimeStamp { get; set; }
        bool Active { get; set; }  
    }

    /// <summary>
    /// Class for an extractor manager using Raw
    /// </summary> 
    public class RawExtractorManager : IExtractorManager
    {
        /// <summary>
        /// Constructor for creating a Raw extractor manager
        /// </summary>   
        public RawExtractorManager(
            RawManagerConfig config, 
            CogniteDestination destination,
            ILogger<RawExtractorManager> logger,
            CancellationToken token)
        {
            _config = config;
            _destination = destination;
            _logger = logger;
            _source = CancellationTokenSource.CreateLinkedTokenSource(token);
            _scheduler = new PeriodicScheduler(_source.Token);
            _state = new ExtractorState(false);
            _cronWrapper =  new CronTimeSpanWrapper(true, true, "s", "1");
            SetCronRawValue();
        }

        private readonly RawManagerConfig _config;
        private readonly CogniteDestination _destination;
        private readonly ILogger<RawExtractorManager> _logger;
        private readonly CancellationTokenSource _source;
        private readonly PeriodicScheduler _scheduler;
        private readonly ExtractorState _state;
        private readonly CronTimeSpanWrapper _cronWrapper;

        ///InactivityThreshold
        public TimeSpan InactivityThreshold { get; set; } = new TimeSpan(0,0,15);
        ///Interval
        public TimeSpan Interval { get; set; } = new TimeSpan(0,0,5);
        ///Offset
        public TimeSpan Offset { get; set; } = new TimeSpan(0,0,3);

        /// <summary>
        /// Method called by standby extractor to wait until it should become active
        /// </summary>
        public async Task WaitToBecomeActive()
        {
            while (!_source.IsCancellationRequested)
            {
                if (_cronWrapper.Value.TotalMilliseconds < 10) continue;
                await Task.Delay(_cronWrapper.Value).ConfigureAwait(false);

                await UpdateState().ConfigureAwait(false);
               
                Console.WriteLine("\nExtractor " +_config.Index+ " waiting to become active... \n");

                List<int> responsiveExtractorIndexes = new List<int>();
                bool responsive = false;
                foreach (RawExtractorInstance extractor in _state.CurrentState)
                {
                    double timeSinceActive = DateTime.UtcNow.Subtract(extractor.TimeStamp).TotalSeconds;
                    if (timeSinceActive < InactivityThreshold.TotalSeconds)
                    {
                        if (extractor.Active == true) responsive = true;
                        else responsiveExtractorIndexes.Add(extractor.Key);
                    }  
                              
                    //Console.WriteLine("\nExtractor key: " + extractor.Key +"\n"+Math.Floor(timeSinceActive) + " sec since last activity \nActive: " + extractor.Active +"\n");
                }
                if (!responsive)
                {
                    if (responsiveExtractorIndexes.Count > 0)
                    {
                        responsiveExtractorIndexes.Sort();
                        
                        if (responsiveExtractorIndexes[0] == _config.Index)
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
            _scheduler.ScheduleTask("Upload log to state", async (token) => {
                while (!_source.IsCancellationRequested)
                {
                    if (_cronWrapper.Value.TotalMilliseconds < 10) continue;
                    await Task.Delay(_cronWrapper.Value).ConfigureAwait(false);

                    await UpdateState().ConfigureAwait(false);
                }
            });
            /*
            _scheduler.SchedulePeriodicTask("Upload log to state", _cronWrapper, async (token) => {
                Console.WriteLine(_cronWrapper.Value.TotalMilliseconds);               

                await UpdateState().ConfigureAwait(false);
                           
            });
            */   
        }
        internal async Task UpdateState()
        {
            Console.WriteLine("\nExtractor " + _config.Index + " updating state...\n");

            await UploadLogToState().ConfigureAwait(false); 
            await UpdateExtractorState().ConfigureAwait(false);

            CheckIfMultipleActiveExtractors();
        }
        internal void CheckIfMultipleActiveExtractors()
        {
            List<int> activeExtractorIndexes = new List<int>();
            foreach (RawExtractorInstance extractor in _state.CurrentState)
            {            
                double timeSinceActive = DateTime.UtcNow.Subtract(extractor.TimeStamp).TotalSeconds;
                if (extractor.Active == true && timeSinceActive < InactivityThreshold.TotalSeconds) activeExtractorIndexes.Add(extractor.Key);
            }

            if (activeExtractorIndexes.Count > 1)
            {
                activeExtractorIndexes.Sort();
                activeExtractorIndexes.Reverse();

                if (activeExtractorIndexes[0] == _config.Index)
                {
                    Console.WriteLine("\nMultiple active extractors, turning off extractor " + _config.Index +"\n");

                    _source.Cancel();
                }
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
                    int key = Int16.Parse(extractor.Key);
                    RawExtractorInstance instance = new RawExtractorInstance(key, extractor.Columns.TimeStamp, extractor.Columns.Active);
                    extractorInstances.Add(instance);
                    keys.Add(key);
                }
                
                if (keys.Count < _state.CurrentState.Count)
                {
                    Console.WriteLine("FEIL!");
                    foreach (RawExtractorInstance extractor in _state.CurrentState)
                    {            
                        if (!keys.Contains(extractor.Key)) extractorInstances.Add(extractor); 
                        Console.WriteLine("Missing extractor with index " + extractor.Key);
                    }
                }

                _state.CurrentState = extractorInstances;
            }
            catch (Exception ex)
            {
                _logger.LogError("Error when updating state: {msg}", ex.Message);
            }
        }
        internal async Task UploadLogToState()
        {
            RawLogData log = new RawLogData(DateTime.UtcNow, _state.UpdatedStatus);
            RawRowCreate<RawLogData> row = new RawRowCreate<RawLogData>() { Key = _config.Index.ToString(), Columns = log };
            List<RawRowCreate<RawLogData>> rows = new List<RawRowCreate<RawLogData>>(){row};

            try 
            {
                await _destination.CogniteClient.Raw.CreateRowsAsync<RawLogData>(_config.DatabaseName, _config.TableName, rows).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogError("Error when uploading log to state: {msg}", ex.Message);
            }
        }

        internal void SetCronRawValue()
        {
            int offset = ((int) Offset.TotalSeconds * _config.Index);
            _cronWrapper.RawValue = $"{offset}/{(int) Interval.TotalSeconds} * * * * *";
        }
    }

    internal class ExtractorState 
    {
        public ExtractorState(bool initialStatus = false)
        {
            CurrentState = new List<IExtractorInstance>();
            UpdatedStatus = initialStatus;
        }
        public List<IExtractorInstance> CurrentState { get; set; }
        public bool UpdatedStatus { get; set; }
    }

    internal class RawExtractorInstance : IExtractorInstance
    {
        internal RawExtractorInstance(
            int key, 
            DateTime timeStamp, 
            bool active)
        {
            Key = key;
            TimeStamp = timeStamp;
            Active = active;
        }
        public int Key { get; set; }
        public DateTime TimeStamp { get; set; }
        public bool Active { get; set; }  
    }

    internal class RawLogData
    {
        public RawLogData(DateTime timeStamp, bool active)
        {
            TimeStamp = timeStamp;
            Active = active;
        }
        public DateTime TimeStamp { get; }
        public bool Active { get; }
    }
    /// <summary>
    /// Config for a Raw manager
    /// </summary>
    public class RawManagerConfig
    {
        /// <summary>
        /// Constructor
        /// </summary>
        public RawManagerConfig(
            int index, 
            string databaseName, 
            string tableName)
        {
            Index = index;
            DatabaseName = databaseName;
            TableName = tableName;
        }
        ///Index
        public int Index { get; set; }   
        ///DatabaseName
        public string DatabaseName { get; set; }
        ///TableName
        public string TableName { get; set; }  
    }
}