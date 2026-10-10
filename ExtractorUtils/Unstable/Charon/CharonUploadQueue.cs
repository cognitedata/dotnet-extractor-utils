using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Cognite.Extractor.Utils.Unstable.Charon
{
    /// <summary>
    /// Charon upload queue for timeseries create/update tasks. Items are raw JSON bodies:
    /// for a <c>space_routing</c> task, already-shaped flat data-modeling bodies without a
    /// <c>space</c>; for a <c>custom</c> task, raw source-shaped items the mapping consumes.
    /// </summary>
    public class CharonUploadQueue : CharonUploadQueueBase<JsonElement>
    {
        private readonly int _chunkSize;

        /// <summary>
        /// Create a timeseries create/update upload queue.
        /// </summary>
        /// <param name="client">Charon client.</param>
        /// <param name="integrationId">Integration external id.</param>
        /// <param name="taskName">Task name to write to.</param>
        /// <param name="interval">Max interval between uploads.</param>
        /// <param name="maxSize">Max queue size before an upload is forced.</param>
        /// <param name="chunkSize">Max items per Charon call (CDF timeseries limit, typically 1000).</param>
        /// <param name="logger">Logger.</param>
        /// <param name="callback">Optional callback after each upload.</param>
        public CharonUploadQueue(
            ICharonClient client,
            string integrationId,
            string taskName,
            TimeSpan interval,
            int maxSize,
            int chunkSize,
            ILogger logger,
            Func<QueueUploadResult<JsonElement>, Task>? callback = null)
            : base(client, integrationId, taskName, interval, maxSize, logger, callback)
        {
            _chunkSize = chunkSize > 0 ? chunkSize : 1000;
        }

        /// <inheritdoc />
        protected override JsonElement ToPayloadItem(JsonElement item) => item;

        /// <inheritdoc />
        protected override IEnumerable<IReadOnlyList<JsonElement>> Chunk(IEnumerable<JsonElement> items)
        {
            if (items == null) throw new ArgumentNullException(nameof(items));
            return ChunkInner(items);
        }

        /// <summary>Iterator backing <see cref="Chunk"/>.</summary>
        /// <param name="items">Non-null items to chunk.</param>
        /// <returns>Chunks sized to the timeseries limit.</returns>
        private IEnumerable<IReadOnlyList<JsonElement>> ChunkInner(IEnumerable<JsonElement> items)
        {
            var current = new List<JsonElement>(_chunkSize);
            foreach (var item in items)
            {
                current.Add(item);
                if (current.Count >= _chunkSize)
                {
                    yield return current;
                    current = new List<JsonElement>(_chunkSize);
                }
            }
            if (current.Count > 0) yield return current;
        }
    }
}
