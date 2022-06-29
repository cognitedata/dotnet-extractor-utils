using System;
using System.Threading.Tasks;
using System.Threading;
using System.Collections.Generic;
using CogniteSdk;
namespace Cognite.Extractor.Utils
{
    /// <summary>
    /// Interface for an extractor manager
    /// </summary>
    public interface IExtractorManager 
    {
        /// <summary>
        /// True to require a CogniteDestination to be set.
        /// </summary>
        Task WaitToBecomeActive(TimeSpan interval); 
        /// <summary>
        /// True to require a CogniteDestination to be set.
        /// </summary>
        Task UploadLogToStateAtInterval(bool firstRun);
        /// <summary>
        /// True to require a CogniteDestination to be set.
        /// </summary>
        Task CheckIfMultipleActiveExtractors();
        /// <summary>
        /// True to require a CogniteDestination to be set.
        /// </summary>
        int Index { get; }
        /// <summary>
        /// True to require a CogniteDestination to be set.
        /// </summary>
        string DatabaseName { get; }
        /// <summary>
        /// True to require a CogniteDestination to be set.
        /// </summary>
        string TableName { get; }
        /// <summary>
        /// True to require a CogniteDestination to be set.
        /// </summary>
        TimeSpan InactivityThreshold { get; }
        /// <summary>
        /// True to require a CogniteDestination to be set.
        /// </summary>
        CogniteDestination Destination { get; }
        /// <summary>
        /// True to require a CogniteDestination to be set.
        /// </summary>
        CancellationTokenSource Source { get; }
    }
    /// <summary>
    /// True to require a CogniteDestination to be set.
    /// </summary>

    public class LogData
    {
        /// <summary>
        /// True to require a CogniteDestination to be set.
        /// </summary>
        public LogData(DateTime timeStamp, bool active)
        {
            TimeStamp = timeStamp;
            Active = active;
        }
        /// <summary>
        /// True to require a CogniteDestination to be set.
        /// </summary>
        public DateTime TimeStamp { get; }
        /// <summary>
        /// True to require a CogniteDestination to be set.
        /// </summary>
        public bool Active { get; }
    }
    /// <summary>
    /// True to require a CogniteDestination to be set.
    /// </summary>
    public class ExtractorManager : IExtractorManager
    {   
        /// <summary>
        /// True to require a CogniteDestination to be set.
        /// </summary>
        public ExtractorManager(int index, string databaseName, string tableName, TimeSpan inactivityThreshold, CogniteDestination destination, CancellationTokenSource source)
        {
            Index = index;
            DatabaseName = databaseName;
            TableName = tableName;
            InactivityThreshold = inactivityThreshold;
            Destination = destination;
            Source = source;
        }
        /// <summary>
        /// True to require a CogniteDestination to be set.
        /// </summary>
        public int Index { get; }
         /// <summary>
        /// True to require a CogniteDestination to be set.
        /// </summary>
        public string DatabaseName { get; }
        /// <summary>
        /// True to require a CogniteDestination to be set.
        /// </summary>
        public string TableName { get; }
        /// <summary>
        /// True to require a CogniteDestination to be set.
        /// </summary>        
        public TimeSpan InactivityThreshold { get; }
        /// <summary>
        /// True to require a CogniteDestination to be set.
        /// </summary>   
        public CogniteDestination Destination { get; }
        /// <summary>
        /// True to require a CogniteDestination to be set.
        /// </summary> 
        public CancellationTokenSource Source { get; }
        /// <summary>
        /// True to require a CogniteDestination to be set.
        /// </summary>
        public async Task WaitToBecomeActive(TimeSpan interval)
        {
            while (!Source.IsCancellationRequested)
            {
                var allRows = await Destination.CogniteClient.Raw.ListRowsAsync<LogData>(DatabaseName, TableName).ConfigureAwait(false);
                
                Console.WriteLine();
                Console.WriteLine("Current status, extractor " + Index);

                bool responsive = false;
                List<int> responsiveExtractorIndexes = new List<int>();

                foreach (RawRow<LogData> extractor in allRows.Items)
                {
                    LogData extractorData = extractor.Columns;
                    double timeDifference = DateTime.UtcNow.Subtract(extractorData.TimeStamp).TotalSeconds;

                    if (timeDifference < InactivityThreshold.TotalSeconds)
                    {
                         if (extractorData.Active == true) responsive = true;
                         else responsiveExtractorIndexes.Add(Int32.Parse(extractor.Key));
                    }
                    
                    /*
                    if (extractorData.Active == true && timeDifference < InactivityThreshold) responsive = true;
                    if (extractorData.Active == false && timeDifference < InactivityThreshold) responsiveExtractorIndexes.Add(Int32.Parse(extractor.Key));
                    */

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

        private async Task UploadLogToState(bool activeStatus)
        {
            LogData log = new LogData(DateTime.UtcNow, activeStatus);
            RawRowCreate<LogData> row = new RawRowCreate<LogData>() { Key = Index.ToString(), Columns = log };

            List<RawRowCreate<LogData>> rows = new List<RawRowCreate<LogData>>(){row};
            await Destination.CogniteClient.Raw.CreateRowsAsync<LogData>(DatabaseName, TableName, rows).ConfigureAwait(false);
        }
        /// <summary>
        /// True to require a CogniteDestination to be set.
        /// </summary>
        public async Task UploadLogToStateAtInterval(bool firstRun)
        {
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
            Console.WriteLine("Uploading log to shared state...");
            await UploadLogToState(active);    
        }
        /// <summary>
        /// True to require a CogniteDestination to be set.
        /// </summary>
        public async Task<bool> CurrentlyActiveExtractor()
        {
            var allRows = await Destination.CogniteClient.Raw.ListRowsAsync<LogData>(DatabaseName, TableName).ConfigureAwait(false);
            bool responsive = false;
            foreach (RawRow<LogData> extractor in allRows.Items)
            {            
                LogData extractorData = extractor.Columns;
                DateTime currentTime = DateTime.UtcNow;
                double timeDifference = currentTime.Subtract(extractorData.TimeStamp).TotalSeconds;
                if (extractorData.Active == true && timeDifference < InactivityThreshold.TotalSeconds) responsive = true;
            }
            return responsive;
        }

        /// <summary>
        /// True to require a CogniteDestination to be set.
        /// </summary>
        public async Task CheckIfMultipleActiveExtractors()
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

                if (activeExtractorIndexes[0] == Index) Source.Cancel(); //restart extractor Source.Cancel();
            }
        }
    }
}