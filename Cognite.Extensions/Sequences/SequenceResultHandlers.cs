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
                        .Select(id => Identity.Create(id!.Value));
                }
                else if (ex.Message.StartsWith("dataset", StringComparison.InvariantCultureIgnoreCase)
                    || ex.Message.StartsWith("data set", StringComparison.InvariantCultureIgnoreCase))
                {
                    err.Type = ErrorType.ItemMissing;
                    err.Resource = ResourceType.DataSetId;
                    err.Values = ex.Missing.Select(dict
                        => (dict["id"] as MultiValue.Long)?.Value)
                        .Where(id => id.HasValue)
                        .Select(id => Identity.Create(id!.Value));
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
            CogniteError<SequenceCreate> error,
            IEnumerable<SequenceCreate> sequences)
        {
            if (sequences == null) throw new ArgumentNullException(nameof(sequences));
            if (error == null) return sequences;
            if (!error.Values?.Any() ?? true)
            {
                error.Values = sequences.Where(seq => seq.ExternalId != null).Select(seq => Identity.Create(seq.ExternalId));
                return Enumerable.Empty<SequenceCreate>();
            }

            var items = new HashSet<Identity>(error.Values);

            var ret = new List<SequenceCreate>();
            var skipped = new List<SequenceCreate>();

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
                    return null!;
                }).Where(id => id != null);
            }
            // Error messages are completely different in greenfield and bluefield
            else if (ex.Code == 400 && (ex.Message.StartsWith("error in sequence", StringComparison.InvariantCultureIgnoreCase)
                || ex.Message.StartsWith("expected", StringComparison.InvariantCultureIgnoreCase)))
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
        /// </summary>
        /// <param name="error">Error to clean from</param>
        /// <param name="creates">Sequence data creates to clean</param>
        /// <returns>Sequences data creates that did not cause <paramref name="error"/></returns>
        public static IEnumerable<SequenceDataCreate> CleanFromError(
            CogniteError<SequenceRowError> error,
            IEnumerable<SequenceDataCreate> creates)
        {
            if (creates == null) throw new ArgumentNullException(nameof(creates));
            if (error == null) return creates;

            var ret = new List<SequenceDataCreate>();
            var skipped = new List<SequenceRowError>();

            if (!error.Values?.Any() ?? true)
            {
                error.Values = creates.Select(seq => seq.Id.HasValue ? Identity.Create(seq.Id.Value) : Identity.Create(seq.ExternalId));
                return Enumerable.Empty<SequenceDataCreate>();
            }

            var items = new HashSet<Identity>(error.Values);

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
                    skipped.Add(new SequenceRowError(seq.Rows, idt));
                }
            }

            if (error.Skipped == null || !error.Skipped.Any())
            {
                if (skipped.Any())
                {
                    error.Skipped = skipped;
                }
                else
                {
                    error.Skipped = creates.Select(seq => new SequenceRowError(
                        seq.Rows,
                        seq.Id.HasValue ? Identity.Create(seq.Id.Value) : Identity.Create(seq.ExternalId)));
                    return Array.Empty<SequenceDataCreate>();
                }
            }
            
            return ret;

        }

        /// <summary>
        /// Ensure that the list of sequence row creates correctly match the corresponding sequences in CDF,
        /// checks both missing columns and mismatched data types.
        /// </summary>
        /// <param name="resource">CogniteSdk Sequences resource</param>
        /// <param name="creates">SequenceDataCreates to check</param>
        /// <param name="sequencesChunkSize">Chunk size for reading sequences from CDF</param>
        /// <param name="sequencesThrottleSize">Number of parallel requests to read sequences from CDF</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>Up to two <see cref="CogniteError"/> containing data about failed sequences</returns>
        public static async Task<IEnumerable<CogniteError<SequenceRowError>>> VerifySequencesFromCDF(
            SequencesResource resource,
            IEnumerable<SequenceDataCreate> creates,
            int sequencesChunkSize,
            int sequencesThrottleSize,
            CancellationToken token)
        {
            var createMap = creates
                .ToDictionary(seq => seq.Id.HasValue ? Identity.Create(seq.Id.Value) : Identity.Create(seq.ExternalId));

            IEnumerable<Sequence> sequences;
            try
            {
                sequences = await resource
                    .GetByIdsIgnoreErrors(createMap.Keys, sequencesChunkSize, sequencesThrottleSize, token)
                    .ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                var err = ParseSimpleError(
                    ex,
                    createMap.Keys,
                    createMap.Select(kvp => new SequenceRowError(kvp.Value.Rows, kvp.Key)));
                return new[] { err };
            }
            

            var sequenceMap = sequences.ToDictionary(seq =>
            {
                var idIdt = Identity.Create(seq.Id);
                if (createMap.ContainsKey(idIdt)) return idIdt;
                else return Identity.Create(seq.ExternalId);
            });

            var columnErrors = new List<SequenceRowError>();
            var rowErrors = new List<SequenceRowError>();

            foreach (var kvp in createMap)
            {
                var foundSeq = sequenceMap[kvp.Key];
                var create = kvp.Value;
                var colMap = foundSeq.Columns.ToDictionary(seq => seq.ExternalId);

                var badColumns = create.Columns.Where(col => !colMap.ContainsKey(col)).ToList();
                if (badColumns.Any())
                {
                    columnErrors.Add(new SequenceRowError(create.Rows, badColumns, kvp.Key));
                    create.Rows = Enumerable.Empty<SequenceRow>();
                    continue;
                }

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
                        if (fieldEnum.Current != null && fieldEnum.Current.Type != column.ValueType)
                        {
                            badRows.Add(row);
                            break;
                        }
                    }
                }

                if (badRows.Any())
                {
                    rowErrors.Add(new SequenceRowError(badRows, kvp.Key));
                    create.Rows = create.Rows.Except(badRows).ToList();
                }
            }

            var errors = new List<CogniteError<SequenceRowError>>();
            if (columnErrors.Any())
            {
                errors.Add(new CogniteError<SequenceRowError>
                {
                    Message = "Columns missing in sequences",
                    Status = 404,
                    Skipped = columnErrors,
                    Resource = ResourceType.ColumnExternalId,
                    Type = ErrorType.ItemMissing,
                    Values = columnErrors.Select(seq => seq.Id!)
                });
            }
            if (rowErrors.Any())
            {
                errors.Add(new CogniteError<SequenceRowError>
                {
                    Message = "Error in sequence rows",
                    Status = 400,
                    Skipped = rowErrors,
                    Resource = ResourceType.SequenceRowValues,
                    Type = ErrorType.MismatchedType,
                    Values = rowErrors
                        .Where(seq => !createMap[seq.Id].Rows.Any())
                        .Select(seq => seq.Id)
                        .ToList()
                });
            }
            return errors;
        }
    }
    /// <summary>
    /// Contains information about skipped rows per sequence in a row insert request
    /// </summary>
    public class SequenceRowError
    {
        /// <summary>
        /// Constructor for missing columns
        /// </summary>
        /// <param name="skippedRows">Rows skipped due to this error</param>
        /// <param name="badColumns">Bad columns</param>
        /// <param name="id">Id of offending sequence</param>
        public SequenceRowError(IEnumerable<SequenceRow> skippedRows, IEnumerable<string> badColumns, Identity id)
        {
            SkippedRows = skippedRows;
            BadColumns = badColumns;
            Id = id;
        }

        /// <summary>
        /// Constructor for skipped rows
        /// </summary>
        /// <param name="skippedRows">Rows skipped due to this error</param>
        /// <param name="id">Id of offending sequence</param>
        public SequenceRowError(IEnumerable<SequenceRow> skippedRows, Identity id)
        {
            SkippedRows = skippedRows;
            Id = id;
        }

        /// <summary>
        /// Missing columns, if any
        /// </summary>
        public IEnumerable<string>? BadColumns { get; }
        /// <summary>
        /// Id of sequence
        /// </summary>
        public Identity Id { get; }
        /// <summary>
        /// Bad rows
        /// </summary>
        public IEnumerable<SequenceRow> SkippedRows { get; }
    }
}
