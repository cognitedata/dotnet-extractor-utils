using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extractor.Common;
using CogniteSdk;
using Moq;
using Oryx.Cognite;

namespace Cognite.Extractor.Testing.Mock
{
    /// <summary>
    /// A wrapper around a mocked raw table, to simplify access to typed rows.
    /// </summary>
    public class RawTable
    {
        /// <summary>
        /// Raw reference to the rows in the table.
        /// </summary>
        public Dictionary<string, RawRow<JsonNode>> Rows { get; } = new Dictionary<string, RawRow<JsonNode>>();

        /// <summary>
        /// Get a row, deserialized to the given type.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key"></param>
        /// <returns></returns>
        public RawRow<T>? GetRow<T>(string key) where T : class
        {
            if (Rows.TryGetValue(key, out var row))
            {
                return new RawRow<T>
                {
                    Key = row.Key,
                    Columns = row.Columns.Deserialize<T>(Oryx.Cognite.Common.jsonOptions)!,
                    LastUpdatedTime = row.LastUpdatedTime,
                };
            }
            return null;
        }

        /// <summary>
        /// Add a row to the table, serializing the columns from the given type.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key"></param>
        /// <param name="columns"></param>
        public void Add<T>(string key, T columns) where T : class
        {
            Rows[key] = new RawRow<JsonNode>
            {
                Key = key,
                Columns = JsonSerializer.SerializeToNode(columns, Oryx.Cognite.Common.jsonOptions)!,
                LastUpdatedTime = DateTime.UtcNow.ToUnixTimeMilliseconds(),
            };
        }

        /// <summary>
        /// List all rows, deserialized to the given type.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public IEnumerable<RawRow<T>> GetRows<T>() where T : class
        {
            foreach (var row in Rows.Values)
            {
                yield return new RawRow<T>
                {
                    Key = row.Key,
                    Columns = row.Columns.Deserialize<T>(Oryx.Cognite.Common.jsonOptions)!,
                    LastUpdatedTime = row.LastUpdatedTime,
                };
            }
        }
    }

    /// <summary>
    /// A mock for the CDF Raw API.
    /// </summary>
    public class RawMock
    {
        /// <summary>
        /// Representation of databases in mocked raw, map from (dbName, tableName) to rows in the table.
        /// </summary>
        public Dictionary<(string, string), RawTable> Databases { get; }
            = new Dictionary<(string, string), RawTable>();

        /// <summary>
        /// Get the table if it exists.
        /// Returns null if it does not.
        /// </summary>
        /// <param name="dbName">Database name to get</param>
        /// <param name="tableName">Table name to get</param>
        /// <returns>Table, if it has been created.</returns>
        public RawTable? GetTable(string dbName, string tableName)
        {
            if (Databases.TryGetValue((dbName, tableName), out var table))
            {
                return table;
            }
            return null;
        }

        /// <summary>
        /// Get or create a raw table.
        /// </summary>
        /// <param name="dbName">Database name of table to get</param>
        /// <param name="tableName">Table name to get</param>
        /// <returns>Table, either new or already existing</returns>
        public RawTable GetOrCreateTable(string dbName, string tableName)
        {
            if (!Databases.TryGetValue((dbName, tableName), out var table))
            {
                table = new RawTable();
                Databases[(dbName, tableName)] = table;
            }
            return table;
        }

        /// <summary>
        /// Check if the table exists.
        /// </summary>
        /// <param name="dbName">Database name to check</param>
        /// <param name="tableName">Table name to check</param>
        /// <returns>True if the table exists, false otherwise.</returns>
        public bool HasTable(string dbName, string tableName)
        {
            return Databases.ContainsKey((dbName, tableName));
        }

        /// <summary>
        /// Get a matcher for the create raw rows endpoint.
        /// </summary>
        /// <param name="times">Expected number of times called.</param>
        /// <returns>The created matcher.</returns>
        public RequestMatcher CreateRawRowsMatcher(Times times)
        {
            return new SimpleMatcher("POST", _rawRowsRegexRaw, MockRawRows, times);
        }
        /// <summary>
        /// Get a matcher for the get raw rows endpoint.
        /// </summary>
        /// <param name="times"></param>
        /// <returns></returns>
        public RequestMatcher GetRawRowsMatcher(Times times)
        {
            return new SimpleMatcher("GET", _rawRowsRegexRaw, MockGetRawRows, times);
        }

        private static string _rawRowsRegexRaw = @"/raw/dbs/([^/]+)/tables/([^/]+)/rows$";
        private Regex _rawRowsRegex = new Regex(_rawRowsRegexRaw, RegexOptions.Compiled);
        private async Task<HttpResponseMessage> MockRawRows(RequestContext context, CancellationToken token)
        {
            var match = _rawRowsRegex.Match(context.RawRequest.RequestUri!.AbsolutePath!);
            var dbName = match.Groups[1].Value;
            var tableName = match.Groups[2].Value;
            if (!Databases.ContainsKey((dbName, tableName)))
            {
                if (context.RawRequest.RequestUri.Query.Contains("ensureParent=true"))
                {
                    Databases[(dbName, tableName)] = new RawTable();
                }
                else
                {
                    return context.CreateError(System.Net.HttpStatusCode.BadRequest, "Table does not exist");
                }
            }

            var table = Databases[(dbName, tableName)];

            var body = await context.ReadJsonBody<ItemsWithoutCursor<RawRowCreate<JsonNode>>>().ConfigureAwait(false);

            foreach (var row in body!.Items)
            {
                table.Rows[row.Key] = new RawRow<JsonNode>
                {
                    Key = row.Key,
                    Columns = row.Columns,
                    LastUpdatedTime = DateTime.UtcNow.ToUnixTimeMilliseconds(),
                };
            }

            return context.CreateJsonResponse(new EmptyResponse());
        }

        private HttpResponseMessage MockGetRawRows(RequestContext context, CancellationToken token)
        {
            var match = _rawRowsRegex.Match(context.RawRequest.RequestUri!.AbsolutePath!);
            var dbName = match.Groups[1].Value;
            var tableName = match.Groups[2].Value;
            if (!Databases.TryGetValue((dbName, tableName), out var table))
            {
                return context.CreateError(System.Net.HttpStatusCode.BadRequest, "Table does not exist");
            }

            var query = context.ParseQuery();
            var limit = int.Parse(query.ValueOrDefaultCompat("limit", "25")!);
            var cursor = int.Parse(query.ValueOrDefaultCompat("cursor", "0")!);

            var result = table.Rows.Values.Skip(cursor).Take(limit).ToList();
            if (limit + cursor < table.Rows.Count)
            {
                // More data available
                cursor += limit;
                return context.CreateJsonResponse(new ItemsWithCursor<RawRow<JsonNode>> { Items = result, NextCursor = cursor.ToString() });
            }
            return context.CreateJsonResponse(new ItemsWithCursor<RawRow<JsonNode>> { Items = result });
        }
    }
}