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
        void UpdateStateAtInterval(TimeSpan interval, PeriodicScheduler scheduler);
        /// <summary>
        /// Method called by standby extractor to wait until it should become active
        /// </summary>
        /// <param name="interval">The interval the method should run at</param>
        Task WaitToBecomeActive(TimeSpan interval); 
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
        public RawExtractorManager(int index, string databaseName, string tableName, TimeSpan inactivityThreshold, CogniteDestination destination, CancellationTokenSource source)
        {
            Index = index;
            InactivityThreshold = inactivityThreshold;
            DatabaseName = databaseName;
            TableName = tableName;
            Source = source;
            Destination = destination;
            State = new ExtractorState(false);
        }
        internal int Index { get; }   
        internal string DatabaseName { get; }
        internal string TableName { get; }  
        internal ExtractorState State { get; set; }
        internal TimeSpan InactivityThreshold { get; } 
        internal CogniteDestination Destination { get; }
        internal CancellationTokenSource Source { get; }
        /// <summary>
        /// Method used to update the extractor state
        /// </summary>
        public void UpdateStateAtInterval(TimeSpan interval, PeriodicScheduler scheduler)
        {
            if (scheduler != null)
            {
                scheduler.SchedulePeriodicTask("Upload log to state", interval, async (token) => {
                    await UploadLogToState().ConfigureAwait(false); 
                    await UpdateExtractorState().ConfigureAwait(false);

                    CheckIfMultipleActiveExtractors();
                });
            }
        }
        /// <summary>
        /// Method called by standby extractor to wait until it should become active
        /// </summary>
        /// <param name="interval">The interval the method should run at</param>
        public async Task WaitToBecomeActive(TimeSpan interval)
        {
            while (!Source.IsCancellationRequested)
            {
                Console.WriteLine("\nExtractor " +Index+ " waiting to become active... \n");

                List<int> responsiveExtractorIndexes = new List<int>();
                bool responsive = false;
                foreach (RawExtractorInstance extractor in State.CurrentState)
                {
                    double timeDifference = DateTime.UtcNow.Subtract(extractor.TimeStamp).TotalSeconds;
                    if (timeDifference < InactivityThreshold.TotalSeconds)
                    {
                        if (extractor.Active == true) responsive = true;
                        else responsiveExtractorIndexes.Add(extractor.Key);
                    }
                    /*
                    Console.WriteLine("Extractor key: " + extractor.Key);
                    Console.WriteLine(Math.Floor(timeDifference) + " sec since last activity");
                    Console.WriteLine("Active: " + extractor.Active +"\n");
                    */
                    
                }
                if (!responsive)
                {
                    if (responsiveExtractorIndexes.Count > 0)
                    {
                        responsiveExtractorIndexes.Sort();
                        
                        if (responsiveExtractorIndexes[0] == Index)
                        {
                            Console.WriteLine("\nExtractor " + Index + " is starting... \n");
                            
                            State.UpdatedStatus = true;
                            break;
                        }
                    }   
                }
                await Task.Delay(interval).ConfigureAwait(false);
            }
        }
        internal void CheckIfMultipleActiveExtractors()
        {
            List<int> activeExtractorIndexes = new List<int>();
            foreach (RawExtractorInstance extractor in State.CurrentState)
            {            
                double timeDifference = DateTime.UtcNow.Subtract(extractor.TimeStamp).TotalSeconds;
                if (extractor.Active == true && timeDifference < InactivityThreshold.TotalSeconds) activeExtractorIndexes.Add(extractor.Key);
            }

            if (activeExtractorIndexes.Count > 1)
            {
                activeExtractorIndexes.Sort();
                activeExtractorIndexes.Reverse();

                if (activeExtractorIndexes[0] == Index)
                {
                    Console.WriteLine("\nMultiple active extractors, turning off extractor " + Index +"\n");

                    State.UpdatedStatus = false;
                    Source.Cancel();
                }
            }
        }
        internal async Task UpdateExtractorState()
        {
            try
            {
                ItemsWithCursor<RawRow<RawLogData>> rows = await Destination.CogniteClient.Raw.ListRowsAsync<RawLogData>(DatabaseName, TableName).ConfigureAwait(false);

                List<IExtractorInstance> extractorInstances = new List<IExtractorInstance>();
                foreach (RawRow<RawLogData> extractor in rows.Items)
                {
                    RawExtractorInstance instance = new RawExtractorInstance(Int32.Parse(extractor.Key), extractor.Columns.TimeStamp, extractor.Columns.Active);
                    extractorInstances.Add(instance);
                }
                State.CurrentState = extractorInstances;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        internal async Task UploadLogToState()
        {
            RawLogData log = new RawLogData(DateTime.UtcNow, State.UpdatedStatus);
            RawRowCreate<RawLogData> row = new RawRowCreate<RawLogData>() { Key = Index.ToString(), Columns = log };
            List<RawRowCreate<RawLogData>> rows = new List<RawRowCreate<RawLogData>>(){row};

            try 
            {
                await Destination.CogniteClient.Raw.CreateRowsAsync<RawLogData>(DatabaseName, TableName, rows).ConfigureAwait(false);
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
        internal RawExtractorInstance(int key, DateTime timeStamp, bool active)
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
}