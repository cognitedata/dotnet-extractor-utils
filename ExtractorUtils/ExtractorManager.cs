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
    public class ExtractorManager : IExtractorManager
    {   
        /// <summary>
        /// True to require a CogniteDestination to be set.
        /// </summary>
        public ExtractorManager(CogniteDestination destination)
        {
            Destination = destination;
        }

        private CogniteDestination Destination {get;}
        async Task IExtractorManager.WaitToBecomeActive(int index, string databaseName, string tableName, int sleepTime, int inactivityThreshold, CancellationTokenSource source)
        {
            Console.WriteLine("This is extractor " + index);
            while (!source.IsCancellationRequested)
            {
                var allRows = await Destination.CogniteClient.Raw.ListRowsAsync<LogData>(databaseName, tableName).ConfigureAwait(false);
                
                bool active = index == 0 ? true : false;
                foreach (var extractor in allRows.Items)
                {
                    DateTime currentTime = DateTime.Now;
                    LogData extractorData = extractor.Columns;
                    double timeDifference = currentTime.Subtract(extractorData.TimeStamp).TotalSeconds;
                    
                    Console.WriteLine("Extractor key: " + extractor.Key);
                    Console.WriteLine(Math.Floor(timeDifference) + " sec since last activity");
                    Console.WriteLine("Active: " + extractorData.Active);
                    Console.WriteLine();

                    if (extractorData.Active == true && timeDifference > inactivityThreshold && Int32.Parse(extractor.Key) != index)
                    {
                        Console.WriteLine("Starting extractor with index " + index);
                        return;
                    }
                }
                Console.WriteLine();

                await UploadLogToState(active, index, databaseName, tableName);
                await Task.Delay(sleepTime).ConfigureAwait(false);
            }
        }

        async Task UploadLogToState(bool active, int index, string databaseName, string tableName)
        {
            LogData log = new LogData();
            log.TimeStamp = DateTime.Now;
            log.Active = active;

            RawRowCreate<LogData> row = new RawRowCreate<LogData>() { Key = index.ToString(), Columns = log };

            List<RawRowCreate<LogData>> rows = new List<RawRowCreate<LogData>>(){row};
            var insert = await Destination.CogniteClient.Raw.CreateRowsAsync<LogData>(databaseName, tableName, rows).ConfigureAwait(false);
        }
    }
}