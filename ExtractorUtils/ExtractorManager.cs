using System;
using System.Threading.Tasks;
using System.Threading;
using System.Collections.Generic;
using CogniteSdk;
namespace Cognite.Extractor.Utils
{
    public interface IExtractorManager 
    {
        Task WaitToBecomeActive(TimeSpan interval); 
        Task UploadLogToStateAtInterval(bool firstRun);
        Task CheckIfMultipleActiveExtractors(TimeSpan interval);
        Task<bool> CheckIfIndexIsUsed();
        int Index { get; }

        string DatabaseName { get; }
        string TableName { get; }

        TimeSpan InactivityThreshold { get; }

        CogniteDestination Destination { get; }

        CancellationTokenSource Source { get; }
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
    public class ExtractorManager : IExtractorManager
    {   
        public ExtractorManager(int index, string databaseName, string tableName, TimeSpan inactivityThreshold, CogniteDestination destination, CancellationTokenSource source)
        {
            Index = index;
            DatabaseName = databaseName;
            TableName = tableName;
            InactivityThreshold = inactivityThreshold;
            Destination = destination;
            Source = source;
        }
        public int Index { get; }
        public string DatabaseName { get; }
        public string TableName { get; }      
        public TimeSpan InactivityThreshold { get; } 
        public CogniteDestination Destination { get; }
        public CancellationTokenSource Source { get; }
        public async Task WaitToBecomeActive(TimeSpan interval)
        {
            while (!Source.IsCancellationRequested)
            {
                Console.WriteLine();
                Console.WriteLine("Current status, extractor " + Index);

                var allRows = await Destination.CogniteClient.Raw.ListRowsAsync<LogData>(DatabaseName, TableName).ConfigureAwait(false);
                
                List<int> responsiveExtractorIndexes = new List<int>();
                bool responsive = false;
                foreach (RawRow<LogData> extractor in allRows.Items)
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
                            await UploadLogToState(true);
                            return;
                        }
                    }   
                }
                await Task.Delay(interval).ConfigureAwait(false);
            }
        }
        public async Task UploadLogToStateAtInterval(bool firstRun)
        {
            Console.WriteLine("Uploading log to shared state...");

            bool active = false;
            if (!firstRun)
            {
                var allRows = await Destination.CogniteClient.Raw.ListRowsAsync<LogData>(DatabaseName, TableName).ConfigureAwait(false);

                foreach (RawRow<LogData> extractor in allRows.Items)
                {
                    int keyIndex = Int32.Parse(extractor.Key);
                    if (keyIndex == Index) active = extractor.Columns.Active;
                }
            }
            await UploadLogToState(active);    
        }
        public async Task CheckIfMultipleActiveExtractors(TimeSpan interval)
        {
            var allRows = await Destination.CogniteClient.Raw.ListRowsAsync<LogData>(DatabaseName, TableName).ConfigureAwait(false);
    
            List<int> activeExtractorIndexes = new List<int>();
            foreach (RawRow<LogData> extractor in allRows.Items)
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
                    await UploadLogToState(false);
                    Source.Cancel();
                    return;
                }
            }
        }
        public async Task<bool> CheckIfIndexIsUsed()
        {
            var allRows = await Destination.CogniteClient.Raw.ListRowsAsync<LogData>(DatabaseName, TableName).ConfigureAwait(false);
            foreach (RawRow<LogData> extractor in allRows.Items)
            {            
                LogData extractorData = extractor.Columns;
                DateTime currentTime = DateTime.UtcNow;
                double timeDifference = currentTime.Subtract(extractorData.TimeStamp).TotalSeconds;

                if (Int32.Parse(extractor.Key) == Index && timeDifference < InactivityThreshold.TotalSeconds) return true;
            }
            return false;
        }
        private async Task UploadLogToState(bool activeStatus)
        {
            LogData log = new LogData(DateTime.UtcNow, activeStatus);
            RawRowCreate<LogData> row = new RawRowCreate<LogData>() { Key = Index.ToString(), Columns = log };

            List<RawRowCreate<LogData>> rows = new List<RawRowCreate<LogData>>(){row};
            await Destination.CogniteClient.Raw.CreateRowsAsync<LogData>(DatabaseName, TableName, rows).ConfigureAwait(false);
        }
    }
}