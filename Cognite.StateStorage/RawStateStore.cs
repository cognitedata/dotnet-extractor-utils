using Cognite.Common;
using Cognite.Extractor.Common;
using LiteDB;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.Extractor.StateStorage
{
    class RawStateStore : IExtractionStateStore
    {
        private readonly ILogger _logger;
        private readonly IRawDestination _destination;
        private readonly StateStoreConfig _config;
        private readonly string _dbName;
        private ConcurrentDictionary<string, DateTime> _lastTimeStored = new ConcurrentDictionary<string, DateTime>();
        public BsonMapper Mapper { get; }

        public RawStateStore(StateStoreConfig config, IRawDestination destination, ILogger logger)
        {
            _destination = destination;
            _config = config;
            _logger = logger;
            _dbName = config.Location;
            Mapper = StateStoreUtils.BuildMapper();
        }

        /// <summary>
        /// Restore states from raw table using <paramref name="restoreStorableState"/> method to write values into states.
        /// </summary>
        /// <typeparam name="T">Subtype of <see cref="BaseStorableState"/> inserted into state store</typeparam>
        /// <typeparam name="K">Implementation of <see cref="IExtractionState"/> used as state</typeparam>
        /// <param name="extractionStates">States to store</param>
        /// <param name="tableName">Raw table to store into</param>
        /// <param name="restoreStorableState">Action for pair of stored object and state, to restore the state with information from the poco</param>
        /// <param name="token"></param>
        /// <returns></returns>
        public async Task RestoreExtractionState<T, K>(
            IDictionary<string, K> extractionStates,
            string tableName,
            Action<K, T> restoreStorableState,
            CancellationToken token)
            where T : BaseStorableState
            where K : IExtractionState
        {
            try
            {
                _logger.LogDebug("Attempting to restore {TotalNum} extration states from raw table {table}", extractionStates.Count(), tableName);
                int count = 0;
                var raw = await _destination.GetRowsAsync(_dbName, tableName, token);
                foreach (var kvp in raw)
                {
                    var id = kvp.Key;
                    if (extractionStates.TryGetValue(id, out var state))
                    {
                        var poco = StateStoreUtils.DeserializeViaBson<T>(kvp.Value, Mapper);
                        poco.Id = kvp.Key;
                        restoreStorableState(state, poco);
                        count++;
                    }
                }
                StateStoreMetrics.StateRestoreCount.Inc();
                StateStoreMetrics.StateRestoreStates.Inc(count);
                _logger.LogDebug("Restored {Restored} out of {TotalNum} extration states from raw table {store}",
                        count,
                        extractionStates.Count(),
                        tableName);
            }
            catch (Exception e)
            {
                _logger.LogWarning(e, "Failed to restore extraction state from raw table {store}: {Message}", tableName, e.Message);
            }
        }

        /// <summary>
        /// Restores state from raw table given by <paramref name="tableName"/>
        /// </summary>
        /// <typeparam name="K">Subtype of <see cref="BaseExtractionState"/> used as state</typeparam>
        /// <param name="extractionStates">States to restore</param>
        /// <param name="tableName">Raw table to restore from</param>
        /// <param name="initializeMissing">If true, initialize states missing from store to empty.</param>
        /// <param name="token"></param>
        /// <returns></returns>
        public async Task RestoreExtractionState<K>(
            IDictionary<string, K> extractionStates,
            string tableName,
            bool initializeMissing,
            CancellationToken token) where K : BaseExtractionState
        {
            var mapped = new HashSet<string>();

            await RestoreExtractionState<BaseExtractionStatePoco, K>(extractionStates, tableName, (state, poco) =>
            {
                if (!(poco is BaseExtractionStatePoco statePoco)) return;
                state.InitExtractedRange(statePoco.FirstTimestamp, statePoco.LastTimestamp);
                mapped.Add(state.Id);
            }, token);

            if (initializeMissing)
            {
                foreach (var state in extractionStates.Where(state => !mapped.Contains(state.Key)))
                {
                    state.Value.InitExtractedRange(TimeRange.Empty.First, TimeRange.Empty.Last);
                }
            }
        }

        /// <summary>
        /// Store information from states into raw state store.
        /// </summary>
        /// <typeparam name="T">Subtype of <see cref="BaseStorableState"/> extracted from state store</typeparam>
        /// <typeparam name="K">Implementation of <see cref="IExtractionState"/> used as state</typeparam>
        /// <param name="extractionStates">States to store</param>
        /// <param name="tableName">Raw tabke to store into</param>
        /// <param name="buildStorableState">Method to create a storable state from extraction state</param>
        /// <param name="token"></param>
        /// <returns></returns>
        public async Task StoreExtractionState<T, K>(IEnumerable<K> extractionStates, string tableName,
            Func<K, T> buildStorableState, CancellationToken token)
            where T : BaseStorableState
            where K : IExtractionState
        {
            if (!_lastTimeStored.ContainsKey(tableName)) _lastTimeStored[tableName] = CogniteTime.DateTimeEpoch;
            var lastTimeStored = _lastTimeStored[tableName];
            var storageTime = DateTime.UtcNow;

            var statesToStore = extractionStates.Where(state =>
                state.LastTimeModified.HasValue && state.LastTimeModified > lastTimeStored && state.LastTimeModified < storageTime).ToList();

            var pocosToStore = statesToStore.Select(buildStorableState).ToList();

            if (!pocosToStore.Any()) return;

            try
            {

                var dicts = pocosToStore.Select(poco => StateStoreUtils.BsonToDict(Mapper.ToDocument(poco)))
                    .ToDictionary(raw => (string)raw["_id"], raw => raw);
                // No reason to store the row key.
                foreach (var dict in dicts.Values)
                {
                    dict.Remove("_id");
                }
                await _destination.InsertRawRowsAsync(_dbName, tableName, dicts, token);
                StateStoreMetrics.StateStoreCount.Inc();
                StateStoreMetrics.StateStoreStates.Inc(pocosToStore.Count);

                _logger.LogDebug("Saved {Stored} out of {TotalNumber} extraction states to raw table {store}.",
                    pocosToStore.Count, extractionStates.Count(), tableName);
                _lastTimeStored[tableName] = storageTime;

            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to insert raw rows into table {table}: {message}", tableName, ex.Message);
            }
        }

        /// <summary>
        /// Store first and last timestamp to raw state store table given by <paramref name="tableName"/>
        /// </summary>
        /// <typeparam name="K">Subtype of <see cref="BaseExtractionState"/> used as state</typeparam>
        /// <param name="extractionStates">States to store</param>
        /// <param name="tableName">Raw table to store to</param>
        /// <param name="token"></param>
        /// <returns></returns>
        public Task StoreExtractionState<K>(IEnumerable<K> extractionStates, string tableName, CancellationToken token) where K : BaseExtractionState
        {
            return StoreExtractionState(extractionStates, tableName, state =>
                new BaseExtractionStatePoco
                {
                    Id = state.Id,
                    FirstTimestamp = state.DestinationExtractedRange.First,
                    LastTimestamp = state.DestinationExtractedRange.Last
                }, token);
        }

        public async Task DeleteExtractionState(IEnumerable<IExtractionState> extractionStates, string tableName, CancellationToken token)
        {
            HashSet<string> idsToDelete = new HashSet<string>(extractionStates.Select(s => s.Id));
            if (!idsToDelete.Any()) return;

            try
            {
                _logger.LogInformation("Attempting to delete {Num} entries from raw table {store}", idsToDelete.Count, tableName);
                await _destination.DeleteRowsAsync(_dbName, tableName, idsToDelete, token);
                _logger.LogDebug("Removed {NumDeleted} entries from store {store}", idsToDelete.Count, tableName);
            }
            catch (Exception e)
            {
                _logger.LogWarning(e, "Failed to delete extraction state from store {store}: {Message}", e.Message, tableName);
            }
        }
    }
}
