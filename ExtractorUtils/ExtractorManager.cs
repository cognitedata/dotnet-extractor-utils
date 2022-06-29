using System;
using System.Threading.Tasks;
using System.Threading;
using System.Collections.Generic;
using CogniteSdk;
namespace Cognite.Extractor.Utils
{
    public interface IExtractorManager 
    {
        Task InitState();
        Task WaitToBecomeActive(TimeSpan interval); 
        Task UpdateStateAtInterval();
        int Index { get; }
        ExtractorState State { get; set; }
        TimeSpan InactivityThreshold { get; }

        CancellationTokenSource Source { get; }
    }
    public class ExtractorManager : IExtractorManager
    {   
        public ExtractorManager(int index, string databaseName, string tableName, TimeSpan inactivityThreshold, CogniteDestination destination, CancellationTokenSource source)
        {
            Index = index;
            InactivityThreshold = inactivityThreshold;
            Source = source;
            State = new ExtractorState(databaseName, tableName, destination);
        }
        public int Index { get; }   
        public ExtractorState State { get; set; }
        public TimeSpan InactivityThreshold { get; } 
        public CancellationTokenSource Source { get; }

        public async Task InitState()
        {
            await State.UpdateExtractorState();

            bool indexUsed = await CheckIfIndexIsUsed();
            if (indexUsed) Source.Cancel();

            State.UpdatedStatus = false;
        }
        public async Task UpdateStateAtInterval()
        {
            Console.WriteLine("Uploading log to shared state...");

            await State.UpdateExtractorState();
            await State.UploadLogToState(Index); 

            await CheckIfMultipleActiveExtractors();
        }
        public async Task WaitToBecomeActive(TimeSpan interval)
        {
            while (!Source.IsCancellationRequested)
            {
                Console.WriteLine();
                Console.WriteLine("Current status, extractor " + Index);

                List<int> responsiveExtractorIndexes = new List<int>();
                bool responsive = false;
                foreach (RawRow<LogData> extractor in State.CurrentState)
                {
                    LogData extractorData = extractor.Columns;
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
        private async Task CheckIfMultipleActiveExtractors()
        {
            Console.WriteLine("Checking for multiple active extractors...");
    
            List<int> activeExtractorIndexes = new List<int>();
            foreach (RawRow<LogData> extractor in State.CurrentState)
            {            
                LogData extractorData = extractor.Columns;
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
    }

    public class ExtractorState 
    {
        public ExtractorState(string databaseName, string tableName, CogniteDestination destination)
        {
            DatabaseName = databaseName;
            TableName = tableName;
            Destination = destination;
        }
        public IEnumerable<RawRow<LogData>> CurrentState { get; set; }

        public bool UpdatedStatus { get; set; }

        public CogniteDestination Destination { get; }

        public string DatabaseName { get; }
        public string TableName { get; }  

        public async Task UpdateExtractorState()
        {
            ItemsWithCursor<RawRow<LogData>> rows = await Destination.CogniteClient.Raw.ListRowsAsync<LogData>(DatabaseName, TableName).ConfigureAwait(false);
            CurrentState = rows.Items;
        }

        public async Task UploadLogToState(int index)
        {
            LogData log = new LogData(DateTime.UtcNow, UpdatedStatus);
            RawRowCreate<LogData> row = new RawRowCreate<LogData>() { Key = index.ToString(), Columns = log };

            List<RawRowCreate<LogData>> rows = new List<RawRowCreate<LogData>>(){row};
            await Destination.CogniteClient.Raw.CreateRowsAsync<LogData>(DatabaseName, TableName, rows).ConfigureAwait(false);
        }
    }

    public class LogData
    {
        public LogData(DateTime timeStamp, bool active)
        {
            TimeStamp = timeStamp;
            Active = active;
        }
        public DateTime TimeStamp { get; }
        public bool Active { get; }
    }
}