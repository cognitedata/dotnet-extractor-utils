using Cognite.Common;
using LiteDB;
using Microsoft.Extensions.Logging;
using System;
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
        private DateTime _lastTimeStored;
        public BsonMapper Mapper { get; }

        public RawStateStore(StateStoreConfig config, IRawDestination destination, ILogger logger)
        {
            _lastTimeStored = DateTime.UtcNow;
            _destination = destination;
            _config = config;
            _logger = logger;
            _dbName = config.Location;
            Mapper = StateStoreUtils.BuildMapper();
        }

        public async Task RestoreExtractionState<T, K>(
            IDictionary<string, K> extractionStates,
            string tableName,
            Action<K, T> restoreStorableState,
            CancellationToken token)
            where T : BaseStorableState
            where K : IExtractionState
        {
            if (string.IsNullOrEmpty(_config?.Location)) return;

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

        public Task RestoreExtractionState<K>(
            IDictionary<string, K> extractionStates,
            string tableName,
            CancellationToken token) where K : BaseExtractionState
        {
            return RestoreExtractionState<BaseExtractionStatePoco, K>(extractionStates, tableName, (state, poco) =>
            {
                if (!(poco is BaseExtractionStatePoco statePoco)) return;
                state.InitExtractedRange(statePoco.FirstTimestamp, statePoco.LastTimestamp);
            }, token);
        }

        public async Task StoreExtractionState<T, K>(IEnumerable<K> extractionStates, string tableName, Func<K, T> buildStorableState, CancellationToken token)
            where T : BaseStorableState
            where K : IExtractionState
        {
            if (string.IsNullOrEmpty(_config?.Location)) return;

            var storageTime = DateTime.UtcNow;

            var statesToStore = extractionStates.Where(state =>
                state.LastTimeModified.HasValue && state.LastTimeModified > _lastTimeStored && state.LastTimeModified < storageTime).ToList();

            var pocosToStore = statesToStore.Select(buildStorableState).ToList();

            if (!pocosToStore.Any()) return;

            try
            {

                var dicts = pocosToStore.Select(poco => StateStoreUtils.BsonToDict(Mapper.ToDocument(poco)))
                    .ToDictionary(raw => (string)raw["id"], raw => raw);
                // No reason to store the row key.
                foreach (var dict in dicts.Values)
                {
                    dict.Remove("id");
                }
                await _destination.InsertRawRowsAsync(_dbName, tableName, dicts, token);
                StateStoreMetrics.StateStoreCount.Inc();
                StateStoreMetrics.StateStoreStates.Inc(pocosToStore.Count);

                _logger.LogDebug("Saved {Stored} out of {TotalNumber} extraction states to litedb store {store}.",
                    pocosToStore.Count, extractionStates.Count(), tableName);
                _lastTimeStored = storageTime;

            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to insert raw rows into table {table}: {message}", tableName, ex.Message);
            }
        }

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
            if (string.IsNullOrEmpty(_config.Location)) return;

            HashSet<string> idsToDelete = new HashSet<string>(extractionStates.Select(s => s.Id));
            if (!idsToDelete.Any()) return;

            try
            {
                _logger.LogInformation("Attempting to delete {Num} entries from raw table {store}", idsToDelete.Count, tableName);
                await _destination.DeleteRowsAsync(_dbName, tableName, idsToDelete, token);
                _logger.LogDebug("Removed {NumDeleted} entries from store {store}", idsToDelete.Count, tableName);
            }
            catch (LiteException e)
            {
                _logger.LogWarning(e, "Failed to delete extraction state from store {store}: {Message}", e.Message, tableName);
            }
        }
    }
}
