using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extractor.Common;
using CogniteSdk;
using Moq;

namespace Cognite.Extractor.Testing.Mock
{
    /// <summary>
    /// A mock for the CDF Raw API.
    /// </summary>
    public class RawMock
    {
        /// <summary>
        /// Representation of databases in mocked raw, map from (dbName, tableName) to rows in the table.
        /// </summary>
        public Dictionary<(string, string), Dictionary<string, RawRow<JsonNode>>> Databases { get; }
            = new Dictionary<(string, string), Dictionary<string, RawRow<JsonNode>>>();

        /// <summary>
        /// Get the table if it exists.
        /// Returns null if it does not.
        /// </summary>
        /// <param name="dbName">Database name to get</param>
        /// <param name="tableName">Table name to get</param>
        /// <returns>Table, if it has been created.</returns>
        public Dictionary<string, RawRow<JsonNode>>? GetTable(string dbName, string tableName)
        {
            if (Databases.TryGetValue((dbName, tableName), out var table))
            {
                return table;
            }
            return null;
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

        private static string _rawRowsRegexRaw = @"/raw/dbs/([^/]+)/tables/([^/]+)/rows";
        private Regex _rawRowsRegex = new Regex(_rawRowsRegexRaw, RegexOptions.Compiled);
        private async Task<HttpResponseMessage> MockRawRows(RequestContext context, CancellationToken token)
        {
            var match = _rawRowsRegex.Match(context.RawRequest.RequestUri.AbsolutePath);
            var dbName = match.Groups[1].Value;
            var tableName = match.Groups[2].Value;
            if (!Databases.ContainsKey((dbName, tableName)))
            {
                if (context.RawRequest.RequestUri.Query.Contains("ensureParent=true"))
                {
                    Databases[(dbName, tableName)] = new Dictionary<string, RawRow<JsonNode>>();
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
                table[row.Key] = new RawRow<JsonNode>
                {
                    Key = row.Key,
                    Columns = row.Columns,
                    LastUpdatedTime = DateTime.UtcNow.ToUnixTimeMilliseconds(),
                };
            }

            return context.CreateJsonResponse(new EmptyResponse());
        }
    }
}