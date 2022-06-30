using System;
using System.Threading.Tasks;
using System.Threading;
using System.Collections.Generic;
using CogniteSdk;
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
        /// <param name="interval">The interval the method should run at</param>
        Task WaitToBecomeActive(TimeSpan interval); 
        /// <summary>
        /// Method used to update the extractor state
        /// </summary>
        Task UpdateStateAtInterval();
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
        public async Task UpdateStateAtInterval()
        {
            Console.WriteLine();
            Console.WriteLine("Extractor " + Index);
            Console.WriteLine("Uploading log to shared state...");
            Console.WriteLine("Checking for multiple active extractors...");
            Console.WriteLine();

            await State.UploadLogToState(Index, DatabaseName, TableName, Destination); 
            await State.UpdateExtractorState(DatabaseName, TableName, Destination);

            CheckIfMultipleActiveExtractors();
        }
        /// <summary>
        /// Method called by standby extractor to wait until it should become active
        /// </summary>
        /// <param name="interval">The interval the method should run at</param>
        public async Task WaitToBecomeActive(TimeSpan interval)
        {
            while (!Source.IsCancellationRequested)
            {
                List<int> responsiveExtractorIndexes = new List<int>();
                bool responsive = false;
                foreach (RawRow<RawLogData> extractor in State.CurrentState)
                {
                    RawLogData extractorData = extractor.Columns;
                    double timeDifference = DateTime.UtcNow.Subtract(extractorData.TimeStamp).TotalSeconds;

                    if (timeDifference < InactivityThreshold.TotalSeconds)
                    {
                        if (extractorData.Active == true) responsive = true;
                        else responsiveExtractorIndexes.Add(Int32.Parse(extractor.Key));
                    }

                    Console.WriteLine("Extractor key: " + extractor.Key);
                    Console.WriteLine(Math.Floor(timeDifference) + " sec since last activity");
                    Console.WriteLine("Active: " + extractorData.Active);
                    Console.WriteLine();
                }
                if (!responsive)
                {
                    if (responsiveExtractorIndexes.Count > 0)
                    {
                        responsiveExtractorIndexes.Sort();
                        
                        if (responsiveExtractorIndexes[0] == Index)
                        {
                            State.UpdatedStatus = true;
                            return;
                        }
                    }   
                }
                await Task.Delay(interval).ConfigureAwait(false);
            }
        }
        internal void CheckIfMultipleActiveExtractors()
        {
            List<int> activeExtractorIndexes = new List<int>();
            foreach (RawRow<RawLogData> extractor in State.CurrentState)
            {            
                RawLogData extractorData = extractor.Columns;
                DateTime currentTime = DateTime.UtcNow;
                double timeDifference = currentTime.Subtract(extractorData.TimeStamp).TotalSeconds;

                if (extractorData.Active == true && timeDifference < InactivityThreshold.TotalSeconds) activeExtractorIndexes.Add(Int32.Parse(extractor.Key));
            }
            if (activeExtractorIndexes.Count > 1)
            {
                activeExtractorIndexes.Sort();
                activeExtractorIndexes.Reverse();

                if (activeExtractorIndexes[0] == Index)
                {
                    State.UpdatedStatus = false;
                    Source.Cancel();
                }
            }
        }
        /*
        private async Task<bool> CheckIfIndexIsUsed()
        {
            foreach (RawRow<LogData> extractor in State.CurrentState)
            {            
                LogData extractorData = extractor.Columns;
                DateTime currentTime = DateTime.UtcNow;
                double timeDifference = currentTime.Subtract(extractorData.TimeStamp).TotalSeconds;

                if (Int32.Parse(extractor.Key) == Index && timeDifference < InactivityThreshold.TotalSeconds) return true;
            }
            return false;
        }
        */
    }

    internal class ExtractorState 
    {
        public ExtractorState(bool initialStatus)
        {
            UpdatedStatus = initialStatus;
            CurrentState = new List<RawRow<RawLogData>>();
        }
        public IEnumerable<RawRow<RawLogData>> CurrentState { get; set; }
        public bool UpdatedStatus { get; set; }
        public async Task UpdateExtractorState(string databaseName, string tableName, CogniteDestination destination)
        {
            ItemsWithCursor<RawRow<RawLogData>> rows = await destination.CogniteClient.Raw.ListRowsAsync<RawLogData>(databaseName, tableName).ConfigureAwait(false);
            CurrentState = rows.Items;
        }

        public async Task UploadLogToState(int index, string databaseName, string tableName, CogniteDestination destination)
        {
            RawLogData log = new RawLogData(DateTime.UtcNow, UpdatedStatus);
            RawRowCreate<RawLogData> row = new RawRowCreate<RawLogData>() { Key = index.ToString(), Columns = log };

            List<RawRowCreate<RawLogData>> rows = new List<RawRowCreate<RawLogData>>(){row};
            await destination.CogniteClient.Raw.CreateRowsAsync<RawLogData>(databaseName, tableName, rows).ConfigureAwait(false);
        }
    }

    interface IExtractorInstance
    {
        int Key { get; set; }
        DateTime timeStamp { get; set; }
        bool Active { get; set; }  
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