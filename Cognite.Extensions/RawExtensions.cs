using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extractor.Common;
using CogniteSdk;
using CogniteSdk.Resources;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Prometheus;

namespace Cognite.Extensions
{
    /// <summary>
    /// Extension utility methods for <see cref="Client.Raw"/>
    /// </summary>
    public static class RawExtensions
    {
        private static ILogger _logger = new NullLogger<Client>();

        internal static void SetLogger(ILogger logger)
        {
            _logger = logger;
        }
        /// <summary>
        /// Insert the provided <paramref name="rows"/> into CDF Raw. The rows are a dictionary of 
        /// keys and DTOs (data type objects). The DTOs  of type <typeparamref name="T"/> are serialized to JSON 
        /// before they are sent to Raw. If the <paramref name="database"/> or <paramref name="table"/> do not
        /// exist, they are created
        /// </summary>
        /// <param name="raw">Raw client</param>
        /// <param name="database">Raw database name</param>
        /// <param name="table">Raw table name</param>
        /// <param name="rows">Rows of keys and columns</param>
        /// <param name="chunkSize">Chunk size</param>
        /// <param name="throttleSize">Throttle size</param>
        /// <param name="token">Cancelation token</param>
        /// <param name="options">Optional JSON options parameter, to be used when converting dto to JsonElement</param>
        /// <typeparam name="T">DTO type</typeparam>
        public static async Task InsertRowsAsync<T>(
            this RawResource raw,
            string database, 
            string table, 
            IDictionary<string, T> rows, 
            int chunkSize, 
            int throttleSize,
            CancellationToken token,
            JsonSerializerOptions? options = null)
        {
            var chunks = rows
                .Select(kvp =>  new RawRowCreate<T>() { Key = kvp.Key, Columns = kvp.Value })
                .ChunkBy(chunkSize);

            var generators = chunks.
                Select<IEnumerable<RawRowCreate<T>>, Func<Task>>(
                    chunk => async () => {
                        using (CdfMetrics.Raw.WithLabels("create_rows"))
                        {
                            await raw.CreateRowsAsync<T>(database, table, chunk, true, options, token).ConfigureAwait(false);
                        }
                    }
                );
            int numTasks = 0;
            await generators
                .RunThrottled(throttleSize, (_) =>
                    _logger.LogDebug("{MethodName} completed {Num}/{Total} tasks", nameof(InsertRowsAsync), ++numTasks,
                        Math.Ceiling((double)rows.Count / chunkSize)),  token)
                .ConfigureAwait(false);
        }

        internal static JsonElement DtoToJson<T>(T dto, JsonSerializerOptions? options)
        {
            if (dto is JsonElement) return (JsonElement)(object)dto;
            var bytes = JsonSerializer.SerializeToUtf8Bytes(dto, options);
            var document = JsonDocument.Parse(bytes);
            return document.RootElement;
        }

        /// <summary>
        /// Returns all rows from the given database and table. <paramref name="chunkSize"/> items are fetched with each request.
        /// </summary>
        /// <param name="raw">Raw client</param>
        /// <param name="dbName">Database to read from</param>
        /// <param name="tableName">Table to read from</param>
        /// <param name="chunkSize">Max number of items per request</param>
        /// <param name="token">Cancellation token</param>
        /// <param name="options">Optional json serializer options</param>
        /// <returns>All rows</returns>
        public static async Task<IDictionary<string, T>> GetRowsAsync<T>(
            this RawResource raw,
            string dbName,
            string tableName,
            int chunkSize,
            CancellationToken token,
            JsonSerializerOptions? options = null)
        {
            // This might be able to be improved with the ability to pre-fetch cursors for parallel read. Missing from the SDK.
            var result = new Dictionary<string, T>();
            string? cursor = null;
            do
            {
                var query = new RawRowQuery
                {
                    Limit = chunkSize
                };
                if (cursor != null)
                {
                    query.Cursor = cursor;
                }
                ItemsWithCursor<RawRow<T>> rows;
                using (CdfMetrics.Raw.WithLabels("list_rows").NewTimer())
                {
                    rows = await raw.ListRowsAsync<T>(dbName, tableName, query, options, token).ConfigureAwait(false);
                }
                foreach (var row in rows.Items)
                {
                    result[row.Key] = row.Columns;
                }
                cursor = rows.NextCursor;
                _logger.LogDebug("Read: {count} rows from raw table: {raw}, database: {db}", rows.Items.Count(), tableName, dbName);
            } while (cursor != null);
            return result;
        }

        /// <summary>
        /// Delete the given rows from raw database and table.
        /// Will succeed even if database or table does not exist.
        /// </summary>
        /// <param name="raw">Raw client</param>
        /// <param name="dbName">Database to delete from</param>
        /// <param name="tableName">Table to delete from</param>
        /// <param name="rowKeys">Keys for rows to delete</param>
        /// <param name="chunkSize">Number of deleted rows per request</param>
        /// <param name="throttleSize">Nax number of parallel threads</param>
        /// <param name="token">Cancellation token</param>
        /// <returns></returns>
        public static async Task DeleteRowsAsync(
            this RawResource raw,
            string dbName,
            string tableName,
            IEnumerable<string> rowKeys,
            int chunkSize,
            int throttleSize,
            CancellationToken token)
        {
            var chunks = rowKeys
                .Select(key => new RawRowDelete { Key = key })
                .ChunkBy(chunkSize)
                .ToList();
            var generators = chunks
                .Select<IEnumerable<RawRowDelete>, Func<Task>>(
                    chunk => async () =>
                    {
                        using (CdfMetrics.Raw.WithLabels("delete").NewTimer())
                        {
                            await raw.DeleteRowsAsync(dbName, tableName, chunk, token).ConfigureAwait(false);
                        }
                    }
                );
            try
            {
                int numTasks = 0;
                await generators
                    .RunThrottled(throttleSize, (_) =>
                        _logger.LogDebug("{MethodName} completed {Num}/{Total} tasks", nameof(DeleteRowsAsync), ++numTasks, chunks.Count), token)
                    .ConfigureAwait(false);
            }
            catch (ResponseException ex)
            {
                // In order to ignore missing tables/databases
                if (ex.Code == 404)
                {
                    _logger.LogDebug(ex.Message);
                    return;
                }
                throw;
            }
        }
    }
}