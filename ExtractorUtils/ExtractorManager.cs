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
        Task WaitToBecomeActive(int index,  string databaseName, string tableName, int sleepTime); 
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
        async Task IExtractorManager.WaitToBecomeActive(int index, string databaseName, string tableName, int sleepTime)
        {
            bool active = false;
            Console.WriteLine("This is extractor " +index);
            while (!active)
            {
                var allRows = await Destination.CogniteClient.Raw.ListRowsAsync<JsonElement>(databaseName, tableName).ConfigureAwait(false);
                Console.WriteLine(allRows);

                await UploadLogToState(index, databaseName, tableName, sleepTime);
                await Task.Delay(sleepTime).ConfigureAwait(false);
            }
        }

        async Task UploadLogToState(int index, string databaseName, string tableName, int sleepTime)
        {
            LogData log = new LogData();
            log.TimeStamp = DateTime.Now;
            log.Active = index == 0 ? true : false;

            RawRowCreate<LogData> row = new RawRowCreate<LogData>() { Key = index.ToString(), Columns = log };

            List<RawRowCreate<LogData>> rows = new List<RawRowCreate<LogData>>(){row};
            var insert = await Destination.CogniteClient.Raw.CreateRowsAsync<LogData>(databaseName, tableName, rows).ConfigureAwait(false);
        }
    }
}