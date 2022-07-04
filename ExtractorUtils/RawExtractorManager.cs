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
        /// Method used to update the extractor state
        /// </summary>
        public void UpdateStateAtInterval();
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
            PeriodicScheduler scheduler, 
            CogniteDestination destination,
            ILogger<RawExtractorManager> logger,
            CancellationTokenSource source)
        {
            _config = config;
            _scheduler = scheduler;
            _destination = destination;
            _logger = logger;
            _source = source;
            _state = new ExtractorState(false);
        }
        private readonly RawManagerConfig _config;
        private readonly PeriodicScheduler _scheduler;
        private readonly CogniteDestination _destination;
        private readonly ILogger<RawExtractorManager> _logger;
        private readonly CancellationTokenSource _source;
        private readonly ExtractorState _state;
        /// <summary>
        /// Method used to update the extractor state
        /// </summary>
        public void UpdateStateAtInterval()
        {
            _scheduler.SchedulePeriodicTask("Upload log to state", _config.UpdateStateInterval, async (token) => {
                await UploadLogToState().ConfigureAwait(false); 
                await UpdateExtractorState().ConfigureAwait(false);

                CheckIfMultipleActiveExtractors();
            });
        }
        /// <summary>
        /// Method called by standby extractor to wait until it should become active
        /// </summary>
       
        public async Task WaitToBecomeActive()
        {
            while (!_source.IsCancellationRequested)
            {
                Console.WriteLine("\nExtractor " +_config.Index+ " waiting to become active... \n");

                List<int> responsiveExtractorIndexes = new List<int>();
                bool responsive = false;
                foreach (RawExtractorInstance extractor in _state.CurrentState)
                {
                    double timeSinceActive = DateTime.UtcNow.Subtract(extractor.TimeStamp).TotalSeconds;
                    if (timeSinceActive < _config.InactivityThreshold.TotalSeconds)
                    {
                        if (extractor.Active == true) responsive = true;
                        else responsiveExtractorIndexes.Add(extractor.Key);
                    }  
                              
                    Console.WriteLine("\nExtractor key: " + extractor.Key +"\n"+Math.Floor(timeSinceActive) + " sec since last activity \nActive: " + extractor.Active +"\n");
                }
                if (!responsive)
                {
                    if (responsiveExtractorIndexes.Count > 0)
                    {
                        responsiveExtractorIndexes.Sort();
                        
                        if (responsiveExtractorIndexes[0] == _config.Index)
                        {
                            _state.UpdatedStatus = true;
                          
                            Console.WriteLine("\nExtractor " + _config.Index + " is starting... \n");
                            
                            break;
                        }
                    }   
                }
                await Task.Delay(_config.WaitToBecomeActiveInterval).ConfigureAwait(false);
            }
        }
        internal void CheckIfMultipleActiveExtractors()
        {
            List<int> activeExtractorIndexes = new List<int>();
            foreach (RawExtractorInstance extractor in _state.CurrentState)
            {            
                double timeSinceActive = DateTime.UtcNow.Subtract(extractor.TimeStamp).TotalSeconds;
                if (extractor.Active == true && timeSinceActive < _config.InactivityThreshold.TotalSeconds) activeExtractorIndexes.Add(extractor.Key);
            }

            if (activeExtractorIndexes.Count > 1)
            {
                activeExtractorIndexes.Sort();
                activeExtractorIndexes.Reverse();

                if (activeExtractorIndexes[0] == _config.Index)
                {
                    Console.WriteLine("\nMultiple active extractors, turning off extractor " + _config.Index +"\n");

                    _state.UpdatedStatus = false;
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
                    RawExtractorInstance instance = new RawExtractorInstance(Int32.Parse(extractor.Key), extractor.Columns.TimeStamp, extractor.Columns.Active);
                    extractorInstances.Add(instance);

                    keys.Add(Int32.Parse(extractor.Key));
                }

                //Check if extractor data is missing a key, if missing insert prev state
                
                if (keys.Count < _state.CurrentState.Count)
                {
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
            string tableName, 
            TimeSpan inactivityThreshold, 
            TimeSpan updateStateInterval, 
            TimeSpan waitToBecomeActiveInterval)
        {
            Index = index;
            DatabaseName = databaseName;
            TableName = tableName;
            InactivityThreshold = inactivityThreshold;
            UpdateStateInterval = updateStateInterval;
            WaitToBecomeActiveInterval = waitToBecomeActiveInterval;
        }
        ///Index
        public int Index { get; }   
        ///DatabaseName
        public string DatabaseName { get; }
        ///TableName
        public string TableName { get; }  
        ///InactivityThreshold
        public TimeSpan InactivityThreshold { get; } 
        ///UpdateStateInterval
        public TimeSpan UpdateStateInterval { get; } 
        ///WaitToBecomeActiveInterval
        public TimeSpan WaitToBecomeActiveInterval { get; } 
    }
}