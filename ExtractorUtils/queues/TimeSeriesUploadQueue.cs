using Cognite.Extensions;
using Cognite.Extractor.Common;
using Cognite.Extractor.StateStorage;
using CogniteSdk;
using Microsoft.Extensions.Logging;
using Prometheus;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.Extractor.Utils
{
    /// <summary>
    /// Upload queue for timeseries datapoints
    /// </summary>
    public class TimeSeriesUploadQueue : BaseUploadQueue<(Identity id, Datapoint dp)>
    {
        private IExtractionStateStore _store;
        private IDictionary<Identity, BaseExtractionState> _states;
        private string _collection;


        private static readonly Counter _numberPoints = Prometheus.Metrics.CreateCounter("extractor_utils_queue_datapoints",
            "Number of datapoints uploaded to CDF from the queue");
        private static readonly Gauge _queueSize = Prometheus.Metrics.CreateGauge("extractor_utils_datapoints_queue_size",
            "Number of datapoints in the upload queue to CDF");

        private readonly string _bufferPath;
        private bool _bufferEnabled;
        private bool _bufferAny;
        /// <summary>
        /// Upload queue for timeseries datapoints
        /// </summary>
        /// <param name="destination">CogniteDestination to use for uploading</param>
        /// <param name="interval">Interval between each automated push, leave at zero to disable timed pushing</param>
        /// <param name="maxSize">Max size of queue before pushing, set to zero to disable max size</param>
        /// <param name="logger">Logger to use</param>
        /// <param name="callback">Callback after pushing</param>
        /// <param name="bufferPath">Path to local buffer file for binary buffering of datapoints</param>
        public TimeSeriesUploadQueue(
            CogniteDestination destination,
            TimeSpan interval,
            int maxSize,
            ILogger<CogniteDestination> logger,
            Func<QueueUploadResult<(Identity id, Datapoint dp)>, Task> callback,
            string bufferPath) : base(destination, interval, maxSize, logger, callback)
        {
            _bufferPath = bufferPath;
            if (!string.IsNullOrWhiteSpace(_bufferPath))
            {
                _bufferEnabled = true;
                if (!System.IO.File.Exists(_bufferPath))
                {
                    System.IO.File.Create(_bufferPath).Close();
                }
                _bufferAny = new FileInfo(_bufferPath).Length > 0;
                _bufferEnabled = true;
            }
        }

        /// <summary>
        /// Enqueue a datapoint by externalId
        /// </summary>
        /// <param name="id">Timeseries externalId</param>
        /// <param name="dp">Datapoint to enqueue</param>
        public void Enqueue(string id, Datapoint dp)
        {
            Enqueue(Identity.Create(id), dp);
        }
        /// <summary>
        /// Enqueue a datapoint by CogniteSdk Identity
        /// </summary>
        /// <param name="id">Timeseries identity</param>
        /// <param name="dp">Datapoint to enqueue</param>
        public void Enqueue(Identity id, Datapoint dp)
        {
            Enqueue((id, dp));
            _queueSize.Inc();
        }
        /// <summary>
        /// Enqueue a datapoint by internalId
        /// </summary>
        /// <param name="id">Timeseries internalId</param>
        /// <param name="dp">Datapoint to enqueue</param>
        public void Enqueue(long id, Datapoint dp)
        {
            Enqueue(Identity.Create(id), dp);
        }

        /// <summary>
        /// Add state storage to the queue. States are stored at after each upload.
        /// </summary>
        /// <param name="states">Map from timeseries identity to extraction state. Missing states are ignored. Required.</param>
        /// <param name="stateStore">Store to store states in. Optional.</param>
        /// <param name="collection">Collection in state store to use for extraction states</param>
        public void AddStateStorage(
            IDictionary<Identity, BaseExtractionState> states,
            IExtractionStateStore stateStore,
            string collection)
        {
            _store = stateStore;
            _states = states;
            _collection = collection;
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Reliability", "CA2007: Do not directly await a Task", Justification = "Awaiter configured by the caller")]
        private async Task WriteToBuffer(Dictionary<Identity, IEnumerable<Datapoint>> dps, CancellationToken token)
        {
            try
            {
                using (var stream = new FileStream(_bufferPath, FileMode.Append, FileAccess.Write, FileShare.None))
                {
                    await CogniteUtils.WriteDatapointsAsync(dps, stream, token);
                }
                _bufferAny = true;
            }
            catch (Exception ex)
            {
                DestLogger.LogWarning("Failed to write to buffer: {msg}", ex.Message);
            }
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Reliability", "CA2007: Do not directly await a Task", Justification = "Awaiter configured by the caller")]
        private async Task ReadFromBuffer(CancellationToken token)
        {
            IDictionary<Identity, IEnumerable<Datapoint>> dps;
            try
            {
                using (var stream = new FileStream(_bufferPath, FileMode.OpenOrCreate, FileAccess.Read, FileShare.None))
                {

                    do
                    {
                        dps = await CogniteUtils.ReadDatapointsAsync(stream, token, 1_000_000);
                        if (dps.Any())
                        {
                            var result = await Destination.InsertDataPointsAsync(dps, SanitationMode.Clean, RetryMode.OnError, token);
                            var fatal = result.Errors.FirstOrDefault(err => err.Type == ErrorType.FatalFailure);
                            if (fatal != null) throw fatal.Exception ?? new ResponseException(fatal.Message)
                            {
                                Code = fatal.Status
                            };
                            await HandleUploadResult(dps, token);
                            if (Callback != null) await Callback(new QueueUploadResult<(Identity id, Datapoint dp)>(
                                dps.SelectMany(kvp => kvp.Value.Select(dp => (kvp.Key, dp))).ToList()));
                        }
                    } while (dps.Any());
                }
            }
            catch (Exception ex)
            {
                DestLogger.LogWarning("Failed to read from buffer: {msg}", ex.Message);
                return;
            }
            System.IO.File.Create(_bufferPath).Close();
            _bufferAny = false;
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Reliability", "CA2007: Do not directly await a Task", Justification = "Awaiter configured by the caller")]
        private async Task HandleUploadResult(IDictionary<Identity, IEnumerable<Datapoint>> dps, CancellationToken token)
        {
            if (_states == null || !_states.Any()) return;
            foreach (var kvp in dps)
            {
                var states = new List<BaseExtractionState>();
                if (kvp.Value.Any() && _states.TryGetValue(kvp.Key, out var state))
                {
                    var (min, max) = kvp.Value.MinMax(dp => dp.Timestamp);
                    state.UpdateDestinationRange(CogniteTime.FromUnixTimeMilliseconds(min), CogniteTime.FromUnixTimeMilliseconds(max));
                    states.Add(state);
                }
                if (_store != null && !string.IsNullOrWhiteSpace(_collection) && states.Any())
                {
                    await _store.StoreExtractionState(states, _collection, token);
                }
            }
        }


        /// <summary>
        /// Upload datapoints to CDF.
        /// </summary>
        /// <param name="dps">Datapoints to upload</param>
        /// <param name="token"></param>
        /// <returns>Uploaded points or an error</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Reliability", "CA2007: Do not directly await a Task", Justification = "Awaiter configured by the caller")]
        protected override async Task<QueueUploadResult<(Identity id, Datapoint dp)>> UploadEntries(
            IEnumerable<(Identity id, Datapoint dp)> dps,
            CancellationToken token)
        {
            _queueSize.Dec(dps.Count());

            if(!dps.Any())
            {
                if (_bufferAny)
                {
                    bool connected;
                    try
                    {
                        await Destination.TestCogniteConfig(token);
                        connected = true;
                    }
                    catch (Exception ex)
                    {
                        DestLogger.LogTrace("Failed to connect to CDF for inserting datapoints: {msg}", ex.Message);
                        connected = false;
                    }
                    if (connected)
                    {
                        DestLogger.LogTrace("Reconnected to CDF, reading datapoints from buffer");
                        await ReadFromBuffer(token);
                    }
                }
                return new QueueUploadResult<(Identity id, Datapoint dp)>(Enumerable.Empty<(Identity id, Datapoint dp)>());
            }

            if (!dps.Any()) return new QueueUploadResult<(Identity, Datapoint)>(Enumerable.Empty<(Identity, Datapoint)>());
            DestLogger.LogTrace("Dequeued {Number} datapoints to upload to CDF", dps.Count());

            var comparer = new IdentityComparer();

            var dpMap = dps.GroupBy(pair => pair.id, pair => pair.dp, comparer).ToDictionary(group => group.Key,
                group => (IEnumerable<Datapoint>)group, comparer);

            try
            {
                var result = await Destination.InsertDataPointsAsync(dpMap, SanitationMode.Clean, RetryMode.OnError, token);
                
                if (result.Errors != null)
                {
                    var fatal = result.Errors.FirstOrDefault(err => err.Type == ErrorType.FatalFailure);
                    if (fatal != null) throw fatal.Exception ?? new ResponseException(fatal.Message)
                    {
                        Code = fatal.Status
                    };
                    foreach (var err in result.Errors)
                    {
                        if (err.Skipped != null && err.Skipped.Any())
                        {
                            foreach (var dpErr in err.Skipped.OfType<DataPointInsertError>())
                            {
                                dpMap.Remove(dpErr.Id);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                if (_bufferEnabled && (!(ex is ResponseException rex) || rex.Code >= 500))
                {
                    await WriteToBuffer(dpMap, token);
                }
                return new QueueUploadResult<(Identity id, Datapoint dp)>(ex);
            }

            if (_bufferAny)
            {
                await ReadFromBuffer(token);
            }

            try
            {
                await HandleUploadResult(dpMap, token);
            }
            catch (Exception ex)
            {
                DestLogger.LogWarning(ex, "Failed to handle upload results: {msg}", ex.Message);
            }

            var uploaded = dpMap.SelectMany(kvp => kvp.Value.Select(dp => (kvp.Key, dp))).ToList();
            _numberPoints.Inc(uploaded.Count);
            return new QueueUploadResult<(Identity, Datapoint)>(uploaded);
        }
    }
}
