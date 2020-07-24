using System.Collections.Generic;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.Extractor.StateStorage
{
    /// <summary>
    /// A destination capable of pushing to raw
    /// </summary>
    public interface IRawDestination
    {
        /// <summary>
        /// Insert the provided <paramref name="rows"/> into CDF Raw. The rows are a dictionary of 
        /// keys and DTOs (data type objects). The DTOs  of type <typeparamref name="T"/> are serialized to JSON 
        /// before they are sent to Raw. If the <paramref name="database"/> or <paramref name="table"/> do not
        /// exist, they are created
        /// </summary>
        /// <param name="database">Raw database name</param>
        /// <param name="table">Raw table name</param>
        /// <param name="rows">Rows of keys and columns</param>
        /// <param name="token">Cancellation token</param>
        /// <typeparam name="T">DTO type</typeparam>
        /// <returns>Task</returns>
        Task InsertRawRowsAsync<T>(
            string database,
            string table,
            IDictionary<string, T> rows,
            CancellationToken token);

        /// <summary>
        /// Returns all rows from the given database and table
        /// </summary>
        /// <param name="dbName">Database to read from</param>
        /// <param name="tableName">Table to read from</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>All rows</returns>
        Task<IDictionary<string, IDictionary<string, JsonElement>>> GetRowsAsync(string dbName, string tableName, CancellationToken token);

        /// <summary>
        /// Delete the given rows from raw database
        /// </summary>
        /// <param name="dbName">Database to delete from</param>
        /// <param name="tableName">Table to delete from</param>
        /// <param name="rowKeys">Keys for rows to delete</param>
        /// <param name="token">Cancellation token</param>
        /// <returns></returns>
        Task DeleteRowsAsync(string dbName, string tableName, IEnumerable<string> rowKeys, CancellationToken token);
    }
}
