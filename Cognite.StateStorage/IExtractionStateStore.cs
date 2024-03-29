﻿using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.Extractor.StateStorage
{
    /// <summary>
    /// Represents a general extractor state store, for storing first/last timestamps.
    /// </summary>
    public interface IExtractionStateStore : IDisposable
    {
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
        Task StoreExtractionState<T, K>(
            IEnumerable<K> extractionStates,
            string tableName,
            Func<K, T> buildStorableState,
            CancellationToken token) where T : BaseStorableState where K : IExtractionState;

        /// <summary>
        /// Store states into state store table given by <paramref name="tableName"/>
        /// </summary>
        /// <param name="extractionStates">States to store</param>
        /// <param name="tableName">Collection to store to</param>
        /// <param name="token"></param>
        /// <returns></returns>
        Task StoreExtractionState<K>(IEnumerable<K> extractionStates, string tableName, CancellationToken token)
            where K : BaseExtractionState;

        /// <summary>
        /// Generic method to restore state with a custom type.
        /// </summary>
        /// <typeparam name="T">Subtype of <see cref="BaseStorableState"/> inserted into state store</typeparam>
        /// <typeparam name="K">Implementation of <see cref="IExtractionState"/> used as state</typeparam>
        /// <param name="extractionStates">States to store</param>
        /// <param name="tableName">Collection to store into</param>
        /// <param name="restoreStorableState">Action for pair of stored object and state, to restore the state with information from the poco</param>
        /// <param name="token"></param>
        /// <returns></returns>
        Task RestoreExtractionState<T, K>(
            IDictionary<string, K> extractionStates,
            string tableName,
            Action<K, T> restoreStorableState,
            CancellationToken token) where T : BaseStorableState where K : IExtractionState;

        /// <summary>
        /// Restores state from state store table given by <paramref name="tableName"/>
        /// </summary>
        /// <typeparam name="K">Subtype of <see cref="BaseExtractionState"/> used as state</typeparam>
        /// <param name="extractionStates">States to restore</param>
        /// <param name="tableName">Collection to restore from</param>
        /// <param name="initializeMissing">If true, initialize states missing from store to empty.</param>
        /// <param name="token"></param>
        /// <returns></returns>
        Task RestoreExtractionState<K>(
            IDictionary<string, K> extractionStates,
            string tableName,
            bool initializeMissing,
            CancellationToken token)
            where K : BaseExtractionState;

        /// <summary>
        /// Deletes states from state store table given by <paramref name="tableName"/>
        /// </summary>
        /// <param name="extractionStates">States to delete</param>
        /// <param name="tableName">Collection to delete from</param>
        /// <param name="token"></param>
        /// <returns></returns>
        Task DeleteExtractionState(IEnumerable<IExtractionState> extractionStates, string tableName, CancellationToken token);

        /// <summary>
        /// Find all extraction states in the table.
        /// </summary>
        /// <typeparam name="T">Type of state to obtain</typeparam>
        /// <param name="tableName">Name of table to restore from</param>
        /// <param name="token"></param>
        /// <returns></returns>
        Task<IEnumerable<T>> GetAllExtractionStates<T>(string tableName, CancellationToken token) where T : BaseStorableState;
    }
}
