using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extensions;
using Cognite.Extractor.Common;
using Cognite.Extractor.Configuration;
using Cognite.Extractor.Utils;
using CogniteSdk;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Cognite.Extractor.Utils
{
    /// <summary>
    /// Base class for extractors writing timeseries, events or datapoints.
    /// </summary>
    public abstract class BaseExtractor<TConfig> : IDisposable, IAsyncDisposable
        where TConfig : VersionedConfig
    {
        /// <summary>
        /// Delegate for OnConfigUpdate event.
        /// </summary>
        /// <param name="sender">Extractor as sender</param>
        /// <param name="newConfig">New config object</param>
        /// <param name="revision">Revision of the new config object.</param>
        protected delegate void OnConfigUpdateHandler(object sender, TConfig newConfig, int revision);

        /// <summary>
        /// If a RemoteConfigManager is provided, it will periodically look for config updates.
        /// </summary>
        protected event OnConfigUpdateHandler? OnConfigUpdate;

        /// <summary>
        /// Configuration object
        /// </summary>
        protected TConfig Config { get; }
        /// <summary>
        /// CDF destination
        /// </summary>
        protected CogniteDestination? Destination { get; }
        /// <summary>
        /// Timeseries upload queue
        /// </summary>
        protected TimeSeriesUploadQueue? TSUploadQueue { get; private set; }
        /// <summary>
        /// Event upload queue
        /// </summary>
        protected EventUploadQueue? EventUploadQueue { get; private set; }
        /// <summary>
        /// Raw upload queues, by dbName-tableName and type.
        /// </summary>
        protected Dictionary<(string name, Type type), IUploadQueue> RawUploadQueues { get; private set; }
            = new Dictionary<(string name, Type type), IUploadQueue>();
        /// <summary>
        /// Scheduler for running various periodic tasks
        /// </summary>
        protected PeriodicScheduler Scheduler { get; set; } = null!;
        /// <summary>
        /// Cancellation token source
        /// </summary>
        protected CancellationTokenSource Source { get; set; } = null!;

        /// <summary>
        /// Access to the service provider this extractor was built from
        /// </summary>
        protected IServiceProvider Provider { get; private set; }

        /// <summary>
        /// Extraction run for reporting to an extraction pipeline in CDF.
        /// </summary>
        protected ExtractionRun? Run { get; }

        /// <summary>
        /// Config manager for fetching remote configs, will contain the newest config version and its revision.
        /// </summary>
        protected RemoteConfigManager<TConfig>? ConfigManager { get; }

        private readonly ILogger<BaseExtractor<TConfig>> _logger;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="config">Configuration object</param>
        /// <param name="destination">Cognite destination</param>
        /// <param name="provider">Service provider</param>
        /// <param name="run">Optional extraction run</param>
        /// <param name="configManager">Optional remote config manager</param>
        public BaseExtractor(
            TConfig config,
            IServiceProvider provider,
            CogniteDestination? destination = null,
            ExtractionRun? run = null,
            RemoteConfigManager<TConfig>? configManager = null)
        {
            Config = config;
            Destination = destination;
            Provider = provider;
            Run = run;
            _logger = provider.GetService<ILogger<BaseExtractor<TConfig>>>() ?? new NullLogger<BaseExtractor<TConfig>>();
            ConfigManager = configManager;
        }

        private async Task CheckForConfigUpdates(CancellationToken token)
        {
            var newConfig = await ConfigManager!.FetchLatest(token).ConfigureAwait(false);
            if (newConfig != null)
            {
                OnConfigUpdate?.Invoke(this, newConfig, ConfigManager.Revision);
            }
        }

        /// <summary>
        /// Verify that the extractor is configured correctly.
        /// </summary>
        /// <returns>Task</returns>
        protected virtual async Task TestConfig()
        {
            if (Destination != null)
            {
                await Destination.TestCogniteConfig(Source.Token).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Called before Start() and TestConfig(), by default initializes just
        /// Source and Scheduler.
        /// </summary>
        /// <param name="token"></param>
        protected virtual void Init(CancellationToken token)
        {
            Source?.Dispose();
            Scheduler?.Dispose();
            Source = CancellationTokenSource.CreateLinkedTokenSource(token);
            Scheduler = new PeriodicScheduler(Source.Token);
        }

        /// <summary>
        /// Method called to start the extractor.
        /// </summary>
        /// <param name="token">Cancellation token</param>
        /// <returns></returns>
        public virtual async Task Start(CancellationToken token)
        {
            Init(token);
            await TestConfig().ConfigureAwait(false);

            if (ConfigManager != null && ConfigManager.UpdatePeriod.Value != Timeout.InfiniteTimeSpan)
            {
                Scheduler.SchedulePeriodicTask(null, ConfigManager.UpdatePeriod, CheckForConfigUpdates, false);
            }

            try
            {
                await Start().ConfigureAwait(false);
                if (Run != null)
                {
                    Run.Start();
                }
                await Scheduler.WaitForAll().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                if (token.IsCancellationRequested) return;
                if (Run != null)
                {
                    await Run.Report(ExtPipeRunStatus.failure, true,
                        $"Error: {ex.Message}\n{ex.StackTrace}", token).ConfigureAwait(false);
                }
                throw;
            }
            finally
            {
                await OnStop().ConfigureAwait(false);
                if (Run != null)
                {
                    await Run.DisposeAsync().ConfigureAwait(false);
                }
            }
        }

        /// <summary>
        /// Method called to add high availability to an extractor.
        /// </summary>
        /// <param name="config">Configuration object</param>
        /// <param name="interval">Optional update state interval.</param>
        /// <param name="inactivityThreshold">Optional threshold for extractor being inactive.</param>
        /// <returns></returns>
        public async Task RunWithHighAvailabilityAndWait(
            HighAvailabilityConfig config,
            TimeSpan? interval = null,
            TimeSpan? inactivityThreshold = null)
        {
            var highAvailabilityManager = HighAvailabilityUtils
                .CreateHighAvailabilityManager(config, Provider, Scheduler, Source, interval, inactivityThreshold);

            if (highAvailabilityManager != null)
            {
                await highAvailabilityManager.WaitToBecomeActive().ConfigureAwait(false);
            }
            else
            {
                _logger.LogWarning("Add manager config to add high availability.");
            }
        }

        /// <summary>
        /// Internal method starting the extractor. Should handle any creation of timeseries,
        /// setup of source systems, and calls to the various Schedule and Create protected methods.
        /// Should not be the actual extraction. Start should return once the extractor has successfully started.
        /// Other tasks can be scheduled in the PeriodicScheduler.
        /// </summary>
        /// <returns></returns>
        protected abstract Task Start();


        /// <summary>
        /// Called when the extractor is stopping.
        /// </summary>
        /// <returns></returns>
        protected virtual Task OnStop()
        {
            return Task.CompletedTask;
        }

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
            if (Destination == null) throw new InvalidOperationException("Creating queues requires Destination");
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
            Func<QueueUploadResult<(Identity id, Datapoint dp)>, Task>? callback,
            string? bufferPath = null)
        {
            if (Destination == null) throw new InvalidOperationException("Creating queues requires Destination");
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
            Func<QueueUploadResult<EventCreate>, Task>? callback,
            string? bufferPath = null)
        {
            if (Destination == null) throw new InvalidOperationException("Creating queues requires Destination");
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
        /// Use DisposeAsync instead if possible.
        /// </summary>
        /// <param name="disposing">True if disposing</param>
        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                if (Scheduler != null)
                {
                    try
                    {
                        // Cannot be allowed to fail here
                        Scheduler.ExitAllAndWait().Wait();
                    }
                    catch { }
                    Scheduler.Dispose();
                    Scheduler = null!;
                }
                EventUploadQueue?.Dispose();
                EventUploadQueue = null;
                TSUploadQueue?.Dispose();
                TSUploadQueue = null;
                foreach (var queue in RawUploadQueues.Values)
                {
                    queue.Dispose();
                }
                RawUploadQueues.Clear();

                if (Source != null)
                {
                    Source.Cancel();
                    Source.Dispose();
                    Source = null!;
                }
            }
        }

        /// <summary>
        /// Internal method to dispose asynchronously.
        /// </summary>
        protected virtual async ValueTask DisposeAsyncCore()
        {
            if (Scheduler != null)
            {
                try
                {
                    await Scheduler.ExitAllAndWait().ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    _logger?.LogError(ex, "Error terminating scheduler: {msg}", ex.Message);
                }
                Scheduler.Dispose();
                Scheduler = null!;
            }
            if (EventUploadQueue != null) await EventUploadQueue.DisposeAsync().ConfigureAwait(false);
            EventUploadQueue = null;
            if (TSUploadQueue != null) await TSUploadQueue.DisposeAsync().ConfigureAwait(false);
            TSUploadQueue = null;
            foreach (var queue in RawUploadQueues.Values)
            {
                if (queue != null) await queue.DisposeAsync().ConfigureAwait(false);
            }
            RawUploadQueues.Clear();
            if (Source != null)
            {
                Source.Cancel();
                Source.Dispose();
                Source = null!;
            }
        }

        /// <summary>
        /// Dispose extractor. Use DisposeAsync instead if possible.
        /// </summary>
        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Dispose extractor asynchronously. Preferred over synchronous dispose.
        /// </summary>
        public async ValueTask DisposeAsync()
        {
            try
            {
                await DisposeAsyncCore().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Failed to dispose of extractor: {}", ex.Message);
            }
            Dispose(false);
#pragma warning disable CA1816 // Dispose methods should call SuppressFinalize
            GC.SuppressFinalize(this);
#pragma warning restore CA1816 // Dispose methods should call SuppressFinalize
        }
    }
}
