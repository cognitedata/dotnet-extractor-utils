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
    /// Upload queue for timeseries datapoints.
    /// When an <see cref="ICharonClient"/> is provided the queue routes datapoints
    /// through the Charon REST-poll pipeline instead of writing directly to the CDF
    /// time series API. Pass <c>null</c> to use the direct CDF write path.
    /// </summary>
    public class TimeSeriesUploadQueue : BaseUploadQueue<(Identity id, Datapoint dp)>
    {
        private IExtractionStateStore? _store;
        private IDictionary<Identity, BaseExtractionState>? _states;
        private string? _collection;

        private static readonly Counter _numberPoints = Prometheus.Metrics.CreateCounter("extractor_utils_queue_datapoints",
            "Number of datapoints uploaded to CDF from the queue");
        private static readonly Gauge _queueSize = Prometheus.Metrics.CreateGauge("extractor_utils_datapoints_queue_size",
            "Number of datapoints in the upload queue to CDF");

        private readonly string? _bufferPath;
        private bool _bufferEnabled;
        private bool _bufferAny;
        private bool _createMissingTimeseries;
        private long? _dataSetId;

        // Charon fields
        private readonly ICharonClient? _charon;
        private bool _charonSetupDone;

        /// <summary>
        /// Upload queue for timeseries datapoints.
        /// </summary>
        /// <param name="destination">CogniteDestination to use for uploading</param>
        /// <param name="interval">Interval between each automated push, leave at zero to disable timed pushing</param>
        /// <param name="maxSize">Max size of queue before pushing, set to zero to disable max size</param>
        /// <param name="logger">Logger to use</param>
        /// <param name="callback">Callback after pushing</param>
        /// <param name="bufferPath">Path to local buffer file for binary buffering of datapoints</param>
        /// <param name="createMissingTimeseries">Create missing timeseries when insert fails, only works if datapoints
        /// are inserted by external id.</param>
        /// <param name="dataSetId">DataSetId to use if creating missing timeseries.</param>
        /// <param name="charon">
        /// Optional Charon client. When non-null, datapoints are routed through the Charon
        /// REST-poll pipeline instead of the CDF time series API. Fails hard on Charon errors —
        /// there is no silent fallback to the direct CDF write path.
        /// </param>
        public TimeSeriesUploadQueue(
            CogniteDestination destination,
            TimeSpan interval,
            int maxSize,
            ILogger<CogniteDestination> logger,
            Func<QueueUploadResult<(Identity id, Datapoint dp)>, Task>? callback,
            string? bufferPath,
            bool createMissingTimeseries = false,
            long? dataSetId = null,
            ICharonClient? charon = null) : base(destination, interval, maxSize, logger, callback)
        {
            _createMissingTimeseries = createMissingTimeseries;
            _bufferPath = bufferPath;
            _dataSetId = dataSetId;
            _charon = charon;
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
                using (var stream = new FileStream(_bufferPath!, FileMode.Append, FileAccess.Write, FileShare.None))
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
        private async Task<List<(Identity id, Datapoint dp)>> InsertDataPoints(IDictionary<Identity, IEnumerable<Datapoint>> dps, CancellationToken token)
        {
            CogniteResult<DataPointInsertError> result;
            if (_createMissingTimeseries)
            {
                var (dpResult, tsResult) = await Destination.InsertDataPointsCreateMissingAsync(dps, SanitationMode.Clean, RetryMode.OnError, _dataSetId, token);
                if (tsResult != null) DestLogger.LogResult(tsResult, RequestType.CreateDatapoints, true);
                result = dpResult;
            }
            else
            {
                result = await Destination.InsertDataPointsAsync(dps, SanitationMode.Clean, RetryMode.OnError, token);
            }

            DestLogger.LogResult(result, RequestType.CreateDatapoints, false, LogLevel.Debug);

            var skipped = new List<(Identity id, Datapoint dp)>();
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
                        foreach (var dpErr in err.Skipped)
                        {
                            if (dps.TryGetValue(dpErr.Id, out var byDp))
                            {
                                var uploaded = byDp.Except(dpErr.DataPoints);
                                if (uploaded.Any())
                                {
                                    dps[dpErr.Id] = uploaded;
                                    skipped.AddRange(dpErr.DataPoints.Select(dp => (dpErr.Id, dp)));
                                }
                                else
                                {
                                    dps.Remove(dpErr.Id);
                                }
                            }
                        }
                    }
                }
            }
            return skipped;
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Reliability", "CA2007: Do not directly await a Task", Justification = "Awaiter configured by the caller")]
        private async Task ReadFromBuffer(CancellationToken token)
        {
            IDictionary<Identity, IEnumerable<Datapoint>> dps;
            try
            {
                using (var stream = new FileStream(_bufferPath!, FileMode.OpenOrCreate, FileAccess.Read, FileShare.None))
                {

                    do
                    {
                        dps = await CogniteUtils.ReadDatapointsAsync(stream, token, 1_000_000);
                        if (dps.Any())
                        {
                            var skipped = await InsertDataPoints(dps, token);
                            await HandleUploadResult(dps, token);
                            if (Callback != null) await Callback(new QueueUploadResult<(Identity id, Datapoint dp)>(
                                dps.SelectMany(kvp => kvp.Value.Select(dp => (kvp.Key, dp))).ToList(), skipped));
                        }
                    } while (dps.Any());
                }
            }
            catch (Exception ex)
            {
                DestLogger.LogWarning("Failed to read from buffer: {msg}", ex.Message);
                return;
            }
            System.IO.File.Create(_bufferPath!).Close();
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
                    await _store.StoreExtractionState(states, _collection!, token);
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

            // --- Charon path ---
            if (_charon != null)
            {
                return await UploadEntriesCharon(dps, token).ConfigureAwait(false);
            }

            // --- Direct CDF path ---
            if (!dps.Any())
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
                return new QueueUploadResult<(Identity id, Datapoint dp)>(Enumerable.Empty<(Identity id, Datapoint dp)>(), Enumerable.Empty<(Identity id, Datapoint dp)>());
            }

            if (!dps.Any()) return new QueueUploadResult<(Identity, Datapoint)>(Enumerable.Empty<(Identity, Datapoint)>(), Enumerable.Empty<(Identity id, Datapoint dp)>());
            DestLogger.LogTrace("Dequeued {Number} datapoints to upload to CDF", dps.Count());

            var dpMap = dps.GroupBy(pair => pair.id, pair => pair.dp).ToDictionary(group => group.Key,
                group => (IEnumerable<Datapoint>)group);

            IEnumerable<(Identity id, Datapoint dp)> skipped;
            try
            {
                skipped = await InsertDataPoints(dpMap, token);
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
            return new QueueUploadResult<(Identity, Datapoint)>(uploaded, skipped);
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Reliability", "CA2007: Do not directly await a Task", Justification = "Awaiter configured by the caller")]
        private async Task<QueueUploadResult<(Identity id, Datapoint dp)>> UploadEntriesCharon(
            IEnumerable<(Identity id, Datapoint dp)> dps,
            CancellationToken token)
        {
            var dpList = dps.ToList();
            if (dpList.Count == 0)
            {
                return new QueueUploadResult<(Identity id, Datapoint dp)>(
                    Enumerable.Empty<(Identity id, Datapoint dp)>(),
                    Enumerable.Empty<(Identity id, Datapoint dp)>());
            }

            // Lazy /setup — called once before the first real upload.
            if (!_charonSetupDone)
            {
                await _charon!.SetupAsync(token);
                _charonSetupDone = true;
            }

            // Charon requires externalId. Items with only internalId are logged and skipped,
            // matching the behaviour of python-extractor-utils _upload_charon().
            var skipped = dpList.Where(p => p.id.ExternalId == null).ToList();
            if (skipped.Count > 0)
            {
                DestLogger.LogWarning(
                    "{Count} datapoints skipped: Charon requires externalId, internalId-only items cannot be sent",
                    skipped.Count);
            }

            var items = dpList
                .Where(p => p.id.ExternalId != null)
                .Select(p => new CharonItem
                {
                    ExternalId = p.id.ExternalId!,
                    Timestamp = p.dp.Timestamp,
                    Value = p.dp.NumericValue ?? 0,
                })
                .ToList();

            if (items.Count > 0)
            {
                // Fails hard on error — no silent fallback to direct CDF writes.
                await _charon!.InsertPayloadAsync(items, token);
                _numberPoints.Inc(items.Count);
            }

            var uploadedPairs = dpList
                .Where(p => p.id.ExternalId != null)
                .ToList();

            return new QueueUploadResult<(Identity id, Datapoint dp)>(uploadedPairs, skipped);
        }
    }
}
