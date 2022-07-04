using System;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using CogniteSdk;
using Cognite.Extractor.Common;

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
        void UpdateStateAtInterval();
        /// <summary>
        /// Method called by standby extractor to wait until it should become active
        /// </summary>
        Task WaitToBecomeActive(); 
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
            CancellationTokenSource source)
        {
            _index = config.Index;
            _databaseName = config.DatabaseName;
            _tableName = config.TableName;
            _updateStateInterval = config.UpdateStateInterval;
            _waitToBecomeActiveInterval = config.WaitToBecomeActiveInterval;
            _inactivityThreshold = config.InactivityThreshold;
            _source = source;
            _scheduler = scheduler;
            _destination = destination;
            _state = new ExtractorState(false);
        }
        private int _index { get; }   
        private string _databaseName { get; }
        private string _tableName { get; }  
        private ExtractorState _state { get; set; }
        private TimeSpan _inactivityThreshold { get; } 
        private TimeSpan _updateStateInterval { get; } 
        private TimeSpan _waitToBecomeActiveInterval { get; } 
        private PeriodicScheduler _scheduler { get; }
        private CogniteDestination _destination { get; }
        private CancellationTokenSource _source { get; }
        /// <summary>
        /// Method used to update the extractor state
        /// </summary>
        public void UpdateStateAtInterval()
        {
            _scheduler.SchedulePeriodicTask("Upload log to state", _updateStateInterval, async (token) => {
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
                Console.WriteLine("\nExtractor " +_index+ " waiting to become active... \n");

                List<int> responsiveExtractorIndexes = new List<int>();
                bool responsive = false;
                foreach (RawExtractorInstance extractor in _state.CurrentState)
                {
                    double timeSinceActive = DateTime.UtcNow.Subtract(extractor.TimeStamp).TotalSeconds;
                    if (timeSinceActive < _inactivityThreshold.TotalSeconds)
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
                        
                        if (responsiveExtractorIndexes[0] == _index)
                        {
                            _state.UpdatedStatus = true;
                          
                            Console.WriteLine("\nExtractor " + _index + " is starting... \n");
                            
                            break;
                        }
                    }   
                }
                await Task.Delay(_waitToBecomeActiveInterval).ConfigureAwait(false);
            }
        }
        internal void CheckIfMultipleActiveExtractors()
        {
            List<int> activeExtractorIndexes = new List<int>();
            foreach (RawExtractorInstance extractor in _state.CurrentState)
            {            
                double timeSinceActive = DateTime.UtcNow.Subtract(extractor.TimeStamp).TotalSeconds;
                if (extractor.Active == true && timeSinceActive < _inactivityThreshold.TotalSeconds) activeExtractorIndexes.Add(extractor.Key);
            }

            if (activeExtractorIndexes.Count > 1)
            {
                activeExtractorIndexes.Sort();
                activeExtractorIndexes.Reverse();

                if (activeExtractorIndexes[0] == _index)
                {
                    Console.WriteLine("\nMultiple active extractors, turning off extractor " + _index +"\n");

                    _state.UpdatedStatus = false;
                    _source.Cancel();
                }
            }
        }
        internal async Task UpdateExtractorState()
        {
            try
            {
                ItemsWithCursor<RawRow<RawLogData>> rows = await _destination.CogniteClient.Raw.ListRowsAsync<RawLogData>(_databaseName, _tableName).ConfigureAwait(false);

                List<IExtractorInstance> extractorInstances = new List<IExtractorInstance>();
                foreach (RawRow<RawLogData> extractor in rows.Items)
                {
                    RawExtractorInstance instance = new RawExtractorInstance(Int32.Parse(extractor.Key), extractor.Columns.TimeStamp, extractor.Columns.Active);
                    extractorInstances.Add(instance);
                }
                _state.CurrentState = extractorInstances;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        internal async Task UploadLogToState()
        {
            RawLogData log = new RawLogData(DateTime.UtcNow, _state.UpdatedStatus);
            RawRowCreate<RawLogData> row = new RawRowCreate<RawLogData>() { Key = _index.ToString(), Columns = log };
            List<RawRowCreate<RawLogData>> rows = new List<RawRowCreate<RawLogData>>(){row};

            try 
            {
                await _destination.CogniteClient.Raw.CreateRowsAsync<RawLogData>(_databaseName, _tableName, rows).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
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