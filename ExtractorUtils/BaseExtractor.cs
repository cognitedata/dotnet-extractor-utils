using Cognite.Extensions;
using Cognite.Extractor.Common;
using Cognite.Extractor.Utils;
using CogniteSdk;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.ExtractorUtils
{
    /// <summary>
    /// Base class for extractors writing timeseries, events or datapoints.
    /// </summary>
    public abstract class BaseExtractor : IDisposable
    {
        private bool disposedValue;

        /// <summary>
        /// Configuration object
        /// </summary>
        protected BaseConfig Config { get; }
        /// <summary>
        /// CDF destination
        /// </summary>
        protected CogniteDestination Destination { get; }
        /// <summary>
        /// Timeseries upload queue
        /// </summary>
        protected TimeSeriesUploadQueue TSUploadQueue { get; private set; }
        /// <summary>
        /// Event upload queue
        /// </summary>
        protected EventUploadQueue EventUploadQueue { get; private set; }
        /// <summary>
        /// Raw upload queues, by dbName-tableName and type.
        /// </summary>
        protected Dictionary<(string name, Type type), IUploadQueue> RawUploadQueues { get; private set; }
            = new Dictionary<(string name, Type type), IUploadQueue>();
        /// <summary>
        /// Scheduler for running various periodic tasks
        /// </summary>
        protected PeriodicScheduler Scheduler { get; }
        /// <summary>
        /// Cancellation token source
        /// </summary>
        protected CancellationTokenSource Source { get; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="config">Configuration object</param>
        /// <param name="destination">Cognite destination</param>
        /// <param name="token">Cancellation token</param>
        public BaseExtractor(BaseConfig config, CogniteDestination destination, CancellationToken token)
        {
            Config = config;
            Destination = destination;
            Source = CancellationTokenSource.CreateLinkedTokenSource(token);
            Scheduler = new PeriodicScheduler(Source.Token);
        }

        /// <summary>
        /// Verify that the extractor is configured correctly.
        /// </summary>
        /// <returns>Task</returns>
        public virtual async Task TestConfig()
        {
            await Destination.TestCogniteConfig(Source.Token).ConfigureAwait(false);
        }

        /// <summary>
        /// Method called to start the extractor.
        /// </summary>
        /// <returns></returns>
        public abstract Task Start();

        /// <summary>
        /// Create a raw queue with the given type and name
        /// </summary>
        /// <typeparam name="T">Type of columns in raw queue</typeparam>
        /// <param name="dbName">Name of database in Raw</param>
        /// <param name="tableName">Name of table in Raw</param>
        /// <param name="maxSize">Max size of queue before triggering push, 0 for no limit</param>
        /// <param name="uploadInterval">Interval between each push to CDF</param>
        /// <param name="callback">Callback after each push</param>
        protected void CreateRawQueue<T>(
            string dbName,
            string tableName,
            int maxSize,
            TimeSpan uploadInterval,
            Func<QueueUploadResult<(string key, T columns)>, Task> callback)
        {
            string name = $"{dbName}-{tableName}";
            if (RawUploadQueues.ContainsKey(($"{name}", typeof(T))))
                throw new InvalidOperationException($"Upload queue with type {typeof(T)}" +
                    $" and name {name} already exists");
            var queue = Destination.CreateRawUploadQueue(
                dbName,
                tableName,
                uploadInterval,
                maxSize,
                callback);
            RawUploadQueues[($"{name}", typeof(T))] = queue;
            _ = queue.Start(Source.Token);
        }

        /// <summary>
        /// Schedule a periodic run with the given type and name.
        /// Requires a raw queue with matching type and db/table to be created first.
        /// </summary>
        /// <typeparam name="T">Type of columns</typeparam>
        /// <param name="scheduleName">Name of schedule in Scheduler, must be unique</param>
        /// <param name="dbName">Database in Raw</param>
        /// <param name="tableName">Table in Raw</param>
        /// <param name="readInterval">Interval between each execution of <paramref name="readRawRows"/></param>
        /// <param name="readRawRows">Function asynchronously returning a new batch of raw rows from the source system</param>
        protected void ScheduleRawRun<T>(
            string scheduleName,
            string dbName,
            string tableName,
            TimeSpan readInterval,
            Func<CancellationToken, Task<IEnumerable<(string key, T columns)>>> readRawRows)
        {
            string name = $"{dbName}-{tableName}";
            if (!RawUploadQueues.TryGetValue((name, typeof(T)), out var queue))
                throw new InvalidOperationException($"Upload queue with type {typeof(T)} and name {name} has not been created");

            var rawQueue = (RawUploadQueue<T>)queue;

            Scheduler.SchedulePeriodicTask(scheduleName, readInterval, async (token) =>
            {
                var rows = await readRawRows(token).ConfigureAwait(false);
                rawQueue.Enqueue(rows);
            });
        }

        /// <summary>
        /// Set the TSUploadQueue value to a new timeseries queue.
        /// </summary>
        /// <param name="maxSize">Maximum size of queue before pushing to CDF, 0 for no limit</param>
        /// <param name="uploadInterval">Interval between each push to CDF</param>
        /// <param name="callback">Callback after each push to CDF</param>
        /// <param name="bufferPath">Optional path to buffer file</param>
        protected void CreateTimeseriesQueue(
            int maxSize,
            TimeSpan uploadInterval,
            Func<QueueUploadResult<(Identity id, Datapoint dp)>, Task> callback,
            string bufferPath = null)
        {
            if (TSUploadQueue != null) throw new InvalidOperationException("Timeseries upload queue already created");
            TSUploadQueue = Destination.CreateTimeSeriesUploadQueue(
                uploadInterval,
                maxSize,
                callback,
                bufferPath);
            _ = TSUploadQueue.Start(Source.Token);
        }

        /// <summary>
        /// Schedule a periodic run retrieving datapoints from source systems.
        /// </summary>
        /// <param name="scheduleName">Name of task in Scheduler, must be unique</param>
        /// <param name="readInterval">Interval between each read</param>
        /// <param name="readDatapoints">Function reading datapoints from the source system</param>
        protected void ScheduleDatapointsRun(
            string scheduleName,
            TimeSpan readInterval,
            Func<CancellationToken, Task<IEnumerable<(Identity id, Datapoint dp)>>> readDatapoints)
        {
            if (TSUploadQueue == null) throw new InvalidOperationException("Timeseries queue has not been created");
            Scheduler.SchedulePeriodicTask(scheduleName, readInterval, async (token) =>
            {
                var dps = await readDatapoints(token).ConfigureAwait(false);
                TSUploadQueue.Enqueue(dps);
            });
        }

        /// <summary>
        /// Set the EventUploadQueue value to a new event queue.
        /// </summary>
        /// <param name="maxSize">Maximum size of queue before pushing to CDF, 0 for no limit</param>
        /// <param name="uploadInterval">Interval between each push to CDF</param>
        /// <param name="callback">Callback after each push to CDF</param>
        /// <param name="bufferPath">Optional path to buffer file</param>
        protected void CreateEventQueue(
            int maxSize,
            TimeSpan uploadInterval,
            Func<QueueUploadResult<EventCreate>, Task> callback,
            string bufferPath = null)
        {
            if (EventUploadQueue != null) throw new InvalidOperationException("Event upload queue already created");
            EventUploadQueue = Destination.CreateEventUploadQueue(
                uploadInterval,
                maxSize,
                callback,
                bufferPath);
            _ = EventUploadQueue.Start(Source.Token);
        }

        /// <summary>
        /// Schedule a periodic run retrieving events from source systems.
        /// </summary>
        /// <param name="scheduleName">Name of task in Scheduler, must be unique</param>
        /// <param name="readInterval">Interval between each read</param>
        /// <param name="readEvents">Function reading events from the source system</param>
        protected void ScheduleEventsRun(
            string scheduleName,
            TimeSpan readInterval,
            Func<CancellationToken, Task<IEnumerable<EventCreate>>> readEvents)
        {
            if (EventUploadQueue == null) throw new InvalidOperationException("Event queue has not been created");
            Scheduler.SchedulePeriodicTask(scheduleName, readInterval, async (token) =>
            {
                var events = await readEvents(token).ConfigureAwait(false);
                EventUploadQueue.Enqueue(events);
            });
        }

        /// <summary>
        /// Dispose of extractor, waiting for running tasks and pushing everything pending to CDF.
        /// </summary>
        /// <param name="disposing">True if disposing</param>
        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    Scheduler.ExitAllAndWait().Wait();
                    Scheduler.Dispose();
                    EventUploadQueue?.Dispose();
                    TSUploadQueue?.Dispose();
                    foreach (var queue in RawUploadQueues.Values)
                    {
                        queue.Dispose();
                    }
                    RawUploadQueues.Clear();
                    Source.Cancel();
                    Source.Dispose();
                }

                disposedValue = true;
            }
        }

        /// <summary>
        /// Dispose extractor.
        /// </summary>
        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }
    }
}
