using System;
using System.Text.Json;
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
        Task WaitToBecomeActive(int index,  string databaseName, string tableName, int sleepTime, int inactivityThreshold, CancellationTokenSource source); 
    }
    /// <summary>
    /// True to require a CogniteDestination to be set.
    /// </summary>

    public class LogData
    {
        /// <summary>
        /// True to require a CogniteDestination to be set.
        /// </summary>
        public DateTime TimeStamp {get; set;}
        /// <summary>
        /// True to require a CogniteDestination to be set.
        /// </summary>
        public bool Active { get; set; }
    }
    /// <summary>
    /// True to require a CogniteDestination to be set.
    /// </summary>
    public class ExtractorManager
    {   
        /// <summary>
        /// True to require a CogniteDestination to be set.
        /// </summary>
        public ExtractorManager(string databaseName, string tableName, int inactivityThreshold, CogniteDestination destination)
        {
            DatabaseName = databaseName;
            TableName = tableName;
            InactivityThreshold = inactivityThreshold;
            Destination = destination;
        }
        private int InactivityThreshold { get; }
        private string DatabaseName { get; }
        private string TableName { get; }
        private CogniteDestination Destination { get; }
        /// <summary>
        /// True to require a CogniteDestination to be set.
        /// </summary>
        public async Task WaitToBecomeActive(int index, int sleepTime, CancellationTokenSource source)
        {
            while (!source.IsCancellationRequested)
            {
                var allRows = await Destination.CogniteClient.Raw.ListRowsAsync<LogData>(DatabaseName, TableName).ConfigureAwait(false);
                
                bool active = index == 0 ? true : false;

                Console.WriteLine();
                Console.WriteLine("Current status:");
                bool responsive = false;
                List<int> responsiveExtractorIndexes = new List<int>();
                foreach (RawRow<LogData> extractor in allRows.Items)
                {
                    LogData extractorData = extractor.Columns;
                    DateTime currentTime = DateTime.Now;
                    double timeDifference = currentTime.Subtract(extractorData.TimeStamp).TotalSeconds;
                    
                    Console.WriteLine("Extractor key: " + extractor.Key);
                    Console.WriteLine(Math.Floor(timeDifference) + " sec since last activity");
                    Console.WriteLine("Active: " + extractorData.Active);
                    Console.WriteLine();

                    if (extractorData.Active == true && timeDifference < InactivityThreshold) responsive = true;
                    if (extractorData.Active == false && timeDifference < InactivityThreshold) responsiveExtractorIndexes.Add(Int32.Parse(extractor.Key));
                }
                if (!responsive)
                {
                    if (responsiveExtractorIndexes.Count > 0)
                    {
                        responsiveExtractorIndexes.Sort();

                        if (responsiveExtractorIndexes[0] == index)
                        {
                            await UploadLogToState(true, index);
                            return;
                        }
                    }   
                }
                await Task.Delay(sleepTime).ConfigureAwait(false);
            }
        }

        async Task UploadLogToState(bool active, int index)
        {
            LogData log = new LogData();
            log.TimeStamp = DateTime.Now;
            log.Active = active;

            RawRowCreate<LogData> row = new RawRowCreate<LogData>() { Key = index.ToString(), Columns = log };

            List<RawRowCreate<LogData>> rows = new List<RawRowCreate<LogData>>(){row};
            var insert = await Destination.CogniteClient.Raw.CreateRowsAsync<LogData>(DatabaseName, TableName, rows).ConfigureAwait(false);
        }
        /// <summary>
        /// True to require a CogniteDestination to be set.
        /// </summary>
        public async Task UploadLogToStateAtInterval(bool initialStatus, int index, int sleepTime, CancellationTokenSource source)
        {
            Console.WriteLine("This is extractor " + index);
            bool active = initialStatus;
            bool firstRun = true;
            while (!source.IsCancellationRequested)
            {
                if (!firstRun)
                {
                    var allRows = await Destination.CogniteClient.Raw.ListRowsAsync<LogData>(DatabaseName, TableName).ConfigureAwait(false);
                    foreach (var extractor in allRows.Items)
                    {
                        int keyIndex = Int32.Parse(extractor.Key);
                        if (keyIndex == index) active = extractor.Columns.Active;
                    }
                }
                Console.WriteLine("Uploading log to shared state...");

                await UploadLogToState(active, index);
                await Task.Delay(sleepTime).ConfigureAwait(false);
                firstRun = false;
            }
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
                DateTime currentTime = DateTime.Now;
                double timeDifference = currentTime.Subtract(extractorData.TimeStamp).TotalSeconds;
                if (extractorData.Active == true && timeDifference < InactivityThreshold) responsive = true;
            }
            return responsive;
        }
    }
}