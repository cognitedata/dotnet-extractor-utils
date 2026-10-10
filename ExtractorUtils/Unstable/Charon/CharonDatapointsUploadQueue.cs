using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading.Tasks;
using Cognite.Extensions;
using CogniteSdk;
using Microsoft.Extensions.Logging;

namespace Cognite.Extractor.Utils.Unstable.Charon
{
    /// <summary>
    /// Charon upload queue for datapoints. One enqueued reading becomes one payload item
    /// (<c>{externalId, timestamp, value}</c>); Charon does not pivot. Chunks to both the CDF
    /// datapoints-per-request and timeseries-per-request limits.
    /// </summary>
    public class CharonDatapointsUploadQueue : CharonUploadQueueBase<(Identity id, Datapoint dp)>
    {
        private readonly int _maxDatapoints;
        private readonly int _maxSeries;

        /// <summary>
        /// Create a datapoints upload queue.
        /// </summary>
        /// <param name="client">Charon client.</param>
        /// <param name="integrationId">Integration external id.</param>
        /// <param name="taskName">Task name to write to.</param>
        /// <param name="interval">Max interval between uploads.</param>
        /// <param name="maxSize">Max queue size before an upload is forced.</param>
        /// <param name="maxDatapoints">Max datapoints per Charon call (CDF limit, typically 100000).</param>
        /// <param name="maxSeries">Max distinct timeseries per Charon call (CDF limit, typically 10000).</param>
        /// <param name="logger">Logger.</param>
        /// <param name="callback">Optional callback after each upload.</param>
        public CharonDatapointsUploadQueue(
            ICharonClient client,
            string integrationId,
            string taskName,
            TimeSpan interval,
            int maxSize,
            int maxDatapoints,
            int maxSeries,
            ILogger logger,
            Func<QueueUploadResult<(Identity id, Datapoint dp)>, Task>? callback = null)
            : base(client, integrationId, taskName, interval, maxSize, logger, callback)
        {
            _maxDatapoints = maxDatapoints > 0 ? maxDatapoints : 100_000;
            _maxSeries = maxSeries > 0 ? maxSeries : 10_000;
        }

        /// <summary>Enqueue a datapoint by externalId.</summary>
        /// <param name="externalId">Timeseries external id.</param>
        /// <param name="dp">Datapoint.</param>
        public void Enqueue(string externalId, Datapoint dp) => Enqueue((Identity.Create(externalId), dp));

        /// <inheritdoc />
        protected override JsonElement ToPayloadItem((Identity id, Datapoint dp) item)
        {
            var externalId = item.id.ExternalId ?? item.id.Id?.ToString(System.Globalization.CultureInfo.InvariantCulture);
            object? value = item.dp.IsString || item.dp.NumericValue == null
                ? (object?)item.dp.StringValue
                : item.dp.NumericValue;
            var obj = new Dictionary<string, object?>
            {
                ["externalId"] = externalId,
                ["timestamp"] = item.dp.Timestamp,
                ["value"] = value,
            };
            var json = JsonSerializer.Serialize(obj, CharonJson.Options);
            return JsonSerializer.Deserialize<JsonElement>(json, CharonJson.Options);
        }

        /// <inheritdoc />
        protected override IEnumerable<IReadOnlyList<(Identity id, Datapoint dp)>> Chunk(
            IEnumerable<(Identity id, Datapoint dp)> items)
        {
            if (items == null) throw new ArgumentNullException(nameof(items));
            return ChunkInner(items);
        }

        /// <summary>Iterator backing <see cref="Chunk"/>.</summary>
        /// <param name="items">Non-null items to chunk.</param>
        /// <returns>Chunks sized to the datapoint and series limits.</returns>
        private IEnumerable<IReadOnlyList<(Identity id, Datapoint dp)>> ChunkInner(
            IEnumerable<(Identity id, Datapoint dp)> items)
        {
            var current = new List<(Identity id, Datapoint dp)>();
            var series = new HashSet<Identity>();
            foreach (var item in items)
            {
                // Flush before adding if adding would exceed either limit.
                if (current.Count >= _maxDatapoints
                    || (!series.Contains(item.id) && series.Count >= _maxSeries))
                {
                    yield return current;
                    current = new List<(Identity id, Datapoint dp)>();
                    series = new HashSet<Identity>();
                }
                current.Add(item);
                series.Add(item.id);
            }
            if (current.Count > 0) yield return current;
        }
    }
}
