using CogniteSdk;
using CogniteSdk.Resources;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.Extensions
{
    public static partial class ResultHandlers
    {
        private static void ParseSequencesException(ResponseException ex, CogniteError err)
        {
            if (ex.Missing?.Any() ?? false)
            {
                if (ex.Message.StartsWith("asset", StringComparison.InvariantCultureIgnoreCase))
                {
                    err.Type = ErrorType.ItemMissing;
                    err.Resource = ResourceType.AssetId;
                    err.Values = ex.Missing.Select(dict
                        => (dict["id"] as MultiValue.Long)?.Value)
                        .Where(id => id.HasValue)
                        .Select(id => Identity.Create(id.Value));
                }
                else if (ex.Message.StartsWith("dataset", StringComparison.InvariantCultureIgnoreCase)
                    || ex.Message.StartsWith("data set", StringComparison.InvariantCultureIgnoreCase))
                {
                    err.Type = ErrorType.ItemMissing;
                    err.Resource = ResourceType.DataSetId;
                    err.Values = ex.Missing.Select(dict
                        => (dict["id"] as MultiValue.Long)?.Value)
                        .Where(id => id.HasValue)
                        .Select(id => Identity.Create(id.Value));
                }
            }
            else if (ex.Duplicated?.Any() ?? false)
            {
                if (ex.Duplicated.First().ContainsKey("externalId"))
                {
                    err.Type = ErrorType.ItemExists;
                    err.Resource = ResourceType.ExternalId;
                    err.Values = ex.Duplicated.Select(dict
                        => (dict["externalId"] as MultiValue.String)?.Value)
                        .Where(id => id != null)
                        .Select(Identity.Create);
                }
            }
        }

        /// <summary>
        /// Clean list of SequenceCreate objects based on CogniteError
        /// </summary>
        /// <param name="error">Error that occured with a previous push</param>
        /// <param name="sequences">Sequences to clean</param>
        /// <returns>Sequences that are not affected by the error</returns>
        public static IEnumerable<SequenceCreate> CleanFromError(
            CogniteError error,
            IEnumerable<SequenceCreate> sequences)
        {
            if (sequences == null) throw new ArgumentNullException(nameof(sequences));
            if (error == null) return sequences;
            if (!error.Values?.Any() ?? true)
            {
                error.Values = sequences.Where(seq => seq.ExternalId != null).Select(seq => Identity.Create(seq.ExternalId));
                return Enumerable.Empty<SequenceCreate>();
            }

            var items = new HashSet<Identity>(error.Values, new IdentityComparer());

            var ret = new List<SequenceCreate>();
            var skipped = new List<object>();

            foreach (var seq in sequences)
            {
                bool added = false;
                switch (error.Resource)
                {
                    case ResourceType.DataSetId:
                        if (!seq.DataSetId.HasValue || !items.Contains(Identity.Create(seq.DataSetId.Value))) added = true;
                        break;
                    case ResourceType.ExternalId:
                        if (seq.ExternalId == null || !items.Contains(Identity.Create(seq.ExternalId))) added = true;
                        break;
                    case ResourceType.AssetId:
                        if (!seq.AssetId.HasValue || !items.Contains(Identity.Create(seq.AssetId.Value))) added = true;
                        break;
                }
                if (added)
                {
                    ret.Add(seq);
                }
                else
                {
                    CdfMetrics.SequencesSkipped.Inc();
                    skipped.Add(seq);
                }
            }
            if (skipped.Any())
            {
                error.Skipped = skipped;
            }
            else
            {
                error.Skipped = sequences;
                return Enumerable.Empty<SequenceCreate>();
            }
            return ret;
        }

        private static void ParseSequenceRowException(ResponseException ex, CogniteError err)
        {
            if (ex.Missing?.Any() ?? false)
            {
                err.Type = ErrorType.ItemMissing;
                err.Resource = ResourceType.Id;
                err.Values = ex.Missing.Select(dict =>
                {
                    if (dict.TryGetValue("id", out var idVal) && idVal is MultiValue.Long longVal)
                    {
                        return Identity.Create(longVal.Value);
                    }
                    else if (dict.TryGetValue("externalId", out var extIdVal) && extIdVal is MultiValue.String stringVal)
                    {
                        return Identity.Create(stringVal.Value);
                    }
                    return null;
                }).Where(id => id != null);
            }
            else if (ex.Message.StartsWith("error in sequence", StringComparison.InvariantCultureIgnoreCase))
            {
                err.Type = ErrorType.SanitationFailed;
                err.Resource = ResourceType.SequenceRow;
                err.Complete = false;
            }
            else if (ex.Code == 404)
            {
                err.Type = ErrorType.ItemMissing;
                err.Resource = ResourceType.ColumnExternalId;
                err.Complete = false;
            }
        }

        /// <summary>
        /// Clean list of SequenceDataCreates based on error.
        /// If errors concern column contents, this will fetch all sequences
        /// and identify the rows or sequences that caused the error.
        /// </summary>
        /// <param name="resource">CogniteSdk Sequences resource</param>
        /// <param name="error">Error to clean from</param>
        /// <param name="creates">Sequence data creates to clean</param>
        /// <param name="sequencesChunkSize">Chunk size for retrieving sequences</param>
        /// <param name="sequencesThrottleSize">Number of parallel requests for retrieving sequences</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>Sequences data creates that did not cause <paramref name="error"/></returns>
        public static async Task<IEnumerable<SequenceDataCreate>> CleanFromError(
            SequencesResource resource,
            CogniteError error,
            IEnumerable<SequenceDataCreate> creates,
            int sequencesChunkSize,
            int sequencesThrottleSize,
            CancellationToken token)
        {
            if (creates == null) throw new ArgumentNullException(nameof(creates));
            if (error == null) return creates;

            var ret = new List<SequenceDataCreate>();
            var skipped = new List<object>();
            var values = error.Values;

            if (!error.Complete)
            {
                await CompleteError(resource, error, creates, sequencesChunkSize, sequencesThrottleSize, token).ConfigureAwait(false);

                var emptySeqs = new List<Identity>();
                var createMap = creates.ToDictionary(
                    seq => seq.Id.HasValue ? Identity.Create(seq.Id.Value) : Identity.Create(seq.ExternalId), new IdentityComparer());

                // Handle bad rows
                foreach (var rowError in error.Data.OfType<SequenceRowError>())
                {
                    skipped.AddRange(rowError.BadRows);
                    var create = createMap[rowError.Id];

                    create.Rows = create.Rows.Except(rowError.BadRows).ToList();

                    CdfMetrics.SequenceRowsSkipped.Inc(rowError.BadRows.Count());

                    if (!create.Rows.Any()) emptySeqs.Add(rowError.Id);
                }
                values = emptySeqs;
            }

            if (!values?.Any() ?? true)
            {
                error.Values = creates.Select(seq => seq.Id.HasValue ? Identity.Create(seq.Id.Value) : Identity.Create(seq.ExternalId));
                return Enumerable.Empty<SequenceDataCreate>();
            }

            var items = new HashSet<Identity>(values, new IdentityComparer());

            foreach (var seq in creates)
            {
                bool added = false;
                var idt = seq.Id.HasValue ? Identity.Create(seq.Id.Value) : Identity.Create(seq.ExternalId);
                switch (error.Resource)
                {                        
                    case ResourceType.Id:
                        if (!items.Contains(idt)) added = true;
                        break;
                    case ResourceType.SequenceRow:
                        if (!items.Contains(idt)) added = true;
                        break;
                    case ResourceType.ColumnExternalId:
                        if (!items.Contains(idt)) added = true;
                        break;
                }
                if (added)
                {
                    ret.Add(seq);
                }
                else
                {
                    skipped.Add(seq);
                }
            }

            if (skipped.Any())
            {
                error.Skipped = skipped;
            }
            else
            {
                error.Skipped = creates;
                return Array.Empty<SequenceDataCreate>();
            }
            return ret;

        }

        private static async Task CompleteError(
            SequencesResource resource,
            CogniteError error,
            IEnumerable<SequenceDataCreate> creates,
            int sequencesChunkSize,
            int sequencesThrottleSize,
            CancellationToken token)
        {
            if (error.Complete) return;

            var comparer = new IdentityComparer();

            var createMap = creates
                .ToDictionary(seq => seq.Id.HasValue ? Identity.Create(seq.Id.Value) : Identity.Create(seq.ExternalId), comparer);

            var sequences = await resource
                .GetByIdsIgnoreErrors(createMap.Keys, sequencesChunkSize, sequencesThrottleSize, token)
                .ConfigureAwait(false);
            var sequenceMap = sequences.ToDictionary(seq =>
            {
                var idIdt = Identity.Create(seq.Id);
                if (createMap.ContainsKey(idIdt)) return idIdt;
                else return Identity.Create(seq.ExternalId);
            }, comparer);

            var errors = new List<SequenceRowError>();

            foreach (var kvp in createMap)
            {
                var foundSeq = sequenceMap[kvp.Key];
                var create = kvp.Value;
                if (error.Resource == ResourceType.SequenceRow)
                {
                    var colMap = foundSeq.Columns.ToDictionary(seq => seq.ExternalId);
                    var orderedColumns = create.Columns
                        .Select(col => colMap[col])
                        .ToArray();

                    var badRows = new List<SequenceRow>();

                    // Verify each row in the sequence
                    foreach (var row in create.Rows)
                    {
                        int idx = 0;
                        var fieldEnum = row.Values.GetEnumerator();

                        while (fieldEnum.MoveNext())
                        {
                            var column = orderedColumns[idx++];
                            if (fieldEnum.Current.Type != column.ValueType)
                            {
                                badRows.Add(row);
                                break;
                            }
                        }
                    }

                    if (badRows.Any())
                    {
                        errors.Add(new SequenceRowError
                        {
                            BadRows = badRows,
                            Id = kvp.Key
                        });
                    }
                }
                else if (error.Resource == ResourceType.ColumnExternalId)
                {
                    var colMap = foundSeq.Columns.ToDictionary(seq => seq.ExternalId);

                    foreach (var col in create.Columns)
                    {
                        if (!colMap.ContainsKey(col))
                        {
                            errors.Add(new SequenceRowError
                            {
                                BadRows = create.Rows,
                                Id = kvp.Key
                            });
                        }
                    }
                }
            }
            error.Complete = true;
            error.Data = errors;
        }
    }
    /// <summary>
    /// Contains information about skipped rows per sequence in a row insert request
    /// </summary>
    public class SequenceRowError
    {
        /// <summary>
        /// Id of skipped sequence
        /// </summary>
        public Identity Id { get; set; }
        /// <summary>
        /// Bad rows
        /// </summary>
        public IEnumerable<SequenceRow> BadRows { get; set; }
    }
}
