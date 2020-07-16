using Cognite.Extractor.Common;
using Cognite.Extractor.StateStorage;
using CogniteSdk;
using Microsoft.Extensions.Logging;
using Prometheus;
using System;
using System.Collections.Generic;
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

        /// <summary>
        /// Upload queue for timeseries datapoints
        /// </summary>
        /// <param name="destination">CogniteDestination to use for uploading</param>
        /// <param name="interval">Interval between each automated push, leave at zero to disable timed pushing</param>
        /// <param name="maxSize">Max size of queue before pushing, set to zero to disable max size</param>
        /// <param name="logger">Logger to use</param>
        /// <param name="callback">Callback after pushing</param>
        public TimeSeriesUploadQueue(
            CogniteDestination destination,
            TimeSpan interval,
            int maxSize,
            ILogger<CogniteDestination> logger,
            Func<QueueUploadResult<(Identity id, Datapoint dp)>, Task> callback) : base(destination, interval, maxSize, logger, callback)
        { }

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

        private async Task HandleUploadResult(Dictionary<Identity, IEnumerable<Datapoint>> dps, CancellationToken token)
        {
            foreach (var kvp in dps)
            {
                var states = new List<BaseExtractionState>();
                if (kvp.Value.Any() && _states.TryGetValue(kvp.Key, out var state))
                {
                    var (min, max) = kvp.Value.MinMax(dp => CogniteTime.FromUnixTimeMilliseconds(dp.Timestamp));
                    state.UpdateDestinationRange(min, max);
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
        protected override async Task<QueueUploadResult<(Identity id, Datapoint dp)>> UploadEntries(
            IEnumerable<(Identity id, Datapoint dp)> dps,
            CancellationToken token)
        {
            _queueSize.Dec(dps.Count());

            if (!dps.Any()) return new QueueUploadResult<(Identity, Datapoint)>(Enumerable.Empty<(Identity, Datapoint)>());
            _logger.LogTrace("Dequeued {Number} datapoints to upload to CDF", dps.Count());

            var comparer = new IdentityComparer();

            var dpMap = dps.GroupBy(pair => pair.id, pair => pair.dp, comparer).ToDictionary(group => group.Key,
                group => (IEnumerable<Datapoint>)group, comparer);

            try
            {
                var err = await _destination.InsertDataPointsIgnoreErrorsAsync(dpMap, token);
                if (err.IdsNotFound?.Any() ?? false)
                {
                    foreach (var id in err.IdsNotFound) dpMap.Remove(id);
                }
                if (err.IdsWithMismatchedData?.Any() ?? false)
                {
                    foreach (var id in err.IdsWithMismatchedData) dpMap.Remove(id);
                }
            }
            catch (Exception ex)
            {
                return new QueueUploadResult<(Identity id, Datapoint dp)>(ex);
            }

            try
            {
                await HandleUploadResult(dpMap, token);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to handle upload results: {msg}", ex.Message);
            }


            var uploaded = dpMap.SelectMany(kvp => kvp.Value.Select(dp => (kvp.Key, dp))).ToList();
            _numberPoints.Inc(uploaded.Count);
            return new QueueUploadResult<(Identity, Datapoint)>(uploaded);
        }
    }
}
