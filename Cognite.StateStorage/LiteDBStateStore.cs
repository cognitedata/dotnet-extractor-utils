using System;
using LiteDB;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Threading;
using System.Linq;
using Prometheus;
using Cognite.Extractor.Common;
using System.Collections.Concurrent;
using System.IO;

namespace Cognite.Extractor.StateStorage
{
    /// <summary>
    /// Local database to store state between each run of the extractor.
    /// Used when the extractor needs to know the first/last points extracted from the source system
    /// and pushed to the destination.
    /// </summary>
    public class LiteDBStateStore : IExtractionStateStore
    {
        private readonly StateStoreConfig _config;
        private readonly ILogger _logger;
        private ConcurrentDictionary<string, DateTime> _lastTimeStored = new ConcurrentDictionary<string, DateTime>();

        /// <summary>
        /// BsonMapper used to convert objects into bson. Can be modified to add functionality.
        /// </summary>
        public BsonMapper Mapper { get; }
        private string ConnectionString { get => $"filename={_config.Location};upgrade=true"; }

        /// <summary>
        /// Create StateStore wrapper.
        /// </summary>
        /// <param name="config">Configuration of StateStore</param>
        /// <param name="logger">Logger to use with StateStore</param>
        public LiteDBStateStore(StateStoreConfig config, ILogger logger)
        {
            _config = config;
            _logger = logger;
            Mapper = StateStoreUtils.BuildMapper();
        }

        /// <summary>
        /// Return a connection to the database. Must be disposed of after use.
        /// This uses the custom DateTime mapper.
        /// </summary>
        /// <param name="readOnly">If true, the database connection is read-only</param>
        /// <returns></returns>
        public LiteDatabase GetDatabase(bool readOnly = false)
        {
            return new LiteDatabase(readOnly ? $"{ConnectionString};ReadOnly=true" : ConnectionString, Mapper);
        }

        /// <summary>
        /// Store information from states into state store
        /// </summary>
        /// <typeparam name="T">Subtype of <see cref="BaseStorableState"/> extracted from state store</typeparam>
        /// <typeparam name="K">Implementation of <see cref="IExtractionState"/> used as state</typeparam>
        /// <param name="extractionStates">States to store</param>
        /// <param name="tableName">Collection to store into</param>
        /// <param name="buildStorableState">Method to create a storable state from extraction state</param>
        /// <param name="token"></param>
        /// <returns></returns>
        public async Task StoreExtractionState<T, K>(
            IEnumerable<K> extractionStates,
            string tableName,
            Func<K, T> buildStorableState,
            CancellationToken token)
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
                using (var db = GetDatabase())
                {
                    var col = db.GetCollection<T>(tableName);
                    await Task.Run(() => col.Upsert(pocosToStore), token);
                    db.Checkpoint();
                    StateStoreMetrics.StateStoreCount.Inc();
                    StateStoreMetrics.StateStoreStates.Inc(pocosToStore.Count);
                }
                _logger.LogDebug("Saved {Stored} out of {TotalNumber} extraction states to litedb store {store}.",
                    pocosToStore.Count, extractionStates.Count(), tableName);
                _lastTimeStored[tableName] = storageTime;
            }
            catch (LiteException e)
            {
                _logger.LogWarning("Failed to store extraction state to litedb store {store}: {Message}", tableName, e.Message);
            }
        }

        /// <summary>
        /// Store first and last timestamp to litedb state store collection given by <paramref name="tableName"/>
        /// </summary>
        /// <typeparam name="K">Subtype of <see cref="BaseExtractionState"/> used as state</typeparam>
        /// <param name="extractionStates">States to store</param>
        /// <param name="tableName">Collection to store to</param>
        /// <param name="token"></param>
        /// <returns></returns>
        public Task StoreExtractionState<K>(IEnumerable<K> extractionStates, string tableName, CancellationToken token)
            where K : BaseExtractionState
        {
            return StoreExtractionState(extractionStates, tableName, state =>
                new BaseExtractionStatePoco
                {
                    Id = state.Id,
                    FirstTimestamp = state.DestinationExtractedRange.First,
                    LastTimestamp = state.DestinationExtractedRange.Last
                }, token);
        }

        /// <summary>
        /// Generic method to restore state with a custom type.
        /// </summary>
        /// <typeparam name="T">Subtype of <see cref="BaseStorableState"/> inserted into state store</typeparam>
        /// <typeparam name="K">Subtype of <see cref="IExtractionState"/> used as state</typeparam>
        /// <param name="extractionStates">States to store</param>
        /// <param name="tableName">Collection to store into</param>
        /// <param name="restoreStorableState">Action for pair of stored object and state, to restore the state with information from the poco</param>
        /// <param name="token"></param>
        /// <returns></returns>
        public async Task RestoreExtractionState<T, K>(
            IDictionary<string, K> extractionStates,
            string tableName,
            Action<K, T> restoreStorableState,
            CancellationToken token) where T : BaseStorableState where K : IExtractionState
        {
            try
            {
                _logger.LogDebug("Attempting to restore {TotalNum} extraction states from litedb store {store}", extractionStates.Count(), tableName);
                using (var db = GetDatabase(true))
                {
                    var col = db.GetCollection<T>(tableName);
                    var pocos = await Task.Run(() => col.FindAll(), token);
                    int count = 0;
                    foreach (var poco in pocos)
                    {
                        if (extractionStates.TryGetValue(poco.Id, out var state))
                        {
                            restoreStorableState(state, poco);
                            count++;
                        }
                    }
                    StateStoreMetrics.StateRestoreCount.Inc();
                    StateStoreMetrics.StateRestoreStates.Inc(count);
                    _logger.LogDebug("Restored {Restored} out of {TotalNum} extraction states from litedb store {store}",
                        count,
                        extractionStates.Count(),
                        tableName);
                }
            }
            catch (LiteException e)
            {
                _logger.LogWarning("Failed to restore extraction state from litedb store {store}: {Message}", tableName, e.Message);
            }
            catch (FileNotFoundException e)
            {
                _logger.LogWarning("Failed to restore extraction state, store {store} does not exist: {Message}", tableName, e.Message);
            }
        }
        /// <summary>
        /// Restore first and last timestamp from state store.
        /// </summary>
        /// <typeparam name="K">Subtype of <see cref="BaseExtractionState"/> used as state</typeparam>
        /// <param name="extractionStates">States to restore</param>
        /// <param name="tableName">Table to restore from</param>
        /// <param name="initializeMissing">Initialize states missing from store to empty</param>
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
        /// Delete states from state store
        /// </summary>
        /// <param name="extractionStates">States to delete</param>
        /// <param name="tableName">Table to delete from</param>
        /// <param name="token"></param>
        /// <returns></returns>
        public async Task DeleteExtractionState(IEnumerable<IExtractionState> extractionStates, string tableName, CancellationToken token)
        {
            HashSet<string> idsToDelete = new HashSet<string>(extractionStates.Select(s => s.Id));
            if (!idsToDelete.Any()) return;

            try
            {
                using (var db = GetDatabase())
                {
                    _logger.LogInformation("Attempting to delete {Num} entries from litedb state store {store}", idsToDelete.Count, tableName);
                    var col = db.GetCollection<BaseStorableState>(tableName);
                    int numDeleted = await Task.Run(() => col.DeleteMany(state => idsToDelete.Contains(state.Id)));
                    _logger.LogDebug("Removed {NumDeleted} entries from store {store}", numDeleted++, tableName);
                }
            }
            catch (LiteException e)
            {
                _logger.LogWarning("Failed to delete extraction state from store {store}: {Message}", e.Message, tableName);
            }
        }
    }
}
