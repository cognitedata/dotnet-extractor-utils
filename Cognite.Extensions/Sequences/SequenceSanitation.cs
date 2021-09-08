using CogniteSdk;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Cognite.Extensions
{
    public static partial class Sanitation
    {
        /// <summary>
        /// Maximum length of sequence name
        /// </summary>
        public const int SequenceNameMax = 255;

        /// <summary>
        /// Maximum length of sequence description
        /// </summary>
        public const int SequenceDescriptionMax = 1000;

        /// <summary>
        /// Maximum size of key in sequence metadata
        /// </summary>
        public const int SequenceMetadataMaxPerKey = 32;

        /// <summary>
        /// Maximum size of sequence metadata in bytes
        /// </summary>
        public const int SequenceMetadataMaxBytes = 10_000;

        /// <summary>
        /// Maximum total size of sequence and sequence column metadata.
        /// </summary>
        public const int SequenceMetadataMaxBytesTotal = 100_000;

        /// <summary>
        /// Maximum size of key in sequence column metadata
        /// </summary>
        public const int SequenceColumnMetadataMaxPerKey = 32;

        /// <summary>
        /// Maximum size of sequence column metadata in bytes
        /// </summary>
        public const int SequenceColumnMetadataMaxBytes = 10_000;

        /// <summary>
        /// Maximum length of sequence column description
        /// </summary>
        public const int SequenceColumnDescriptionMax = 1000;

        /// <summary>
        /// Maximum length of sequence column name.
        /// </summary>
        public const int SequenceColumnNameMax = 64;

        /// <summary>
        /// Sanitize a SequenceCreate object so that it can be safely sent to CDF.
        /// Requests may still fail due to conflicts or missing ids.
        /// </summary>
        /// <param name="seq">Sequence to sanitize</param>
        public static void Sanitize(this SequenceCreate seq)
        {
            if (seq == null) throw new ArgumentNullException(nameof(seq));
            seq.ExternalId = seq.ExternalId.Truncate(ExternalIdMax);
            seq.Name = seq.Name.Truncate(SequenceNameMax);
            if (seq.AssetId < 1) seq.AssetId = null;
            seq.Description = seq.Description.Truncate(SequenceDescriptionMax);
            if (seq.DataSetId < 1) seq.DataSetId = null;
            seq.Metadata = seq.Metadata.SanitizeMetadata(SequenceMetadataMaxPerKey, SequenceMetadataMaxBytes,
                SequenceMetadataMaxBytes, SequenceMetadataMaxBytes, out int totalBytes);

            foreach (var col in seq.Columns)
            {
                col.ExternalId = col.ExternalId.Truncate(ExternalIdMax);
                col.Name = col.Name.Truncate(SequenceColumnNameMax);
                col.Description = col.Description.Truncate(SequenceColumnDescriptionMax);
                col.Metadata = col.Metadata.SanitizeMetadata(SequenceColumnMetadataMaxPerKey, SequenceColumnMetadataMaxBytes,
                    SequenceColumnMetadataMaxBytes,
                    Math.Min(SequenceColumnMetadataMaxBytes, SequenceMetadataMaxBytesTotal - totalBytes), out int colBytes);
                totalBytes += colBytes;
            }
        }

        /// <summary>
        /// Sanitize a SequenceDataCreate object.
        /// </summary>
        /// <param name="seq"></param>
        public static void Sanitize(this SequenceDataCreate seq)
        {
            if (seq == null) throw new ArgumentNullException(nameof(seq));
            seq.ExternalId = seq.ExternalId.Truncate(ExternalIdMax);
            if (seq.Rows == null) return;
            foreach (var row in seq.Rows) row.Sanitize();
        }

        private static IEnumerable<MultiValue> Sanitize(this IEnumerable<MultiValue> values)
        {
            if (values == null) yield break;
            foreach (var val in values)
            {
                if (val == null) yield return null;
                if (val is MultiValue.String strVal)
                {
                    yield return new MultiValue.String(strVal.Value.Truncate(CogniteUtils.StringLengthMax));
                }
                else if (val is MultiValue.Double doubleVal)
                {
                    if (!double.IsNaN(doubleVal.Value) && !double.IsInfinity(doubleVal.Value))
                    {
                        double value = doubleVal.Value;
                        value = Math.Max(CogniteUtils.NumericValueMin, value);
                        value = Math.Min(CogniteUtils.NumericValueMax, value);
                        yield return value == doubleVal.Value ? doubleVal : new MultiValue.Double(value);
                    }
                    else
                    {
                        yield return null;
                    }
                }
                else yield return val;
            }
        }

        /// <summary>
        /// Ensure that all row values are valid. i.e. within -1E100 and 1E100, not infinity or NaN,
        /// all string values less than 256 characters.
        /// </summary>
        /// <param name="row"></param>
        public static void Sanitize(this SequenceRow row)
        {
            if (row == null) return;
            row.Values = row.Values.Sanitize();
        }


        /// <summary>
        /// Check that given SequenceCreate satisfies CDF limits.
        /// </summary>
        /// <param name="seq">Sequence to check</param>
        /// <returns>Null if sequence satisfies limits, otherwise the resource type that fails</returns>
        public static ResourceType? Verify(this SequenceCreate seq)
        {
            if (seq == null) throw new ArgumentNullException(nameof(seq));
            if (!seq.ExternalId.CheckLength(ExternalIdMax)) return ResourceType.ExternalId;
            if (!seq.Name.CheckLength(SequenceNameMax)) return ResourceType.Name;
            if (seq.AssetId != null && seq.AssetId < 1) return ResourceType.AssetId;
            if (!seq.Description.CheckLength(SequenceDescriptionMax)) return ResourceType.Description;
            if (seq.DataSetId != null && seq.DataSetId < 1) return ResourceType.DataSetId;
            if (!seq.Metadata.VerifyMetadata(SequenceMetadataMaxPerKey, SequenceMetadataMaxBytes,
                SequenceMetadataMaxBytes, SequenceMetadataMaxBytes, out int totalBytes)) return ResourceType.Metadata;

            if (seq.Columns == null || !seq.Columns.Any()) return ResourceType.SequenceColumns;

            foreach (var col in seq.Columns)
            {
                if (col.ExternalId == null || !col.ExternalId.CheckLength(ExternalIdMax)) return ResourceType.ColumnExternalId;
                if (!col.Name.CheckLength(SequenceColumnNameMax)) return ResourceType.ColumnName;
                if (!col.Description.CheckLength(SequenceColumnDescriptionMax)) return ResourceType.ColumnDescription;
                if (!col.Metadata.VerifyMetadata(SequenceColumnMetadataMaxPerKey, SequenceColumnMetadataMaxBytes, SequenceColumnMetadataMaxBytes,
                    Math.Min(SequenceColumnMetadataMaxBytes, SequenceMetadataMaxBytesTotal - totalBytes), out int colBytes)) return ResourceType.ColumnMetadata;
                totalBytes += colBytes;
            }

            if (totalBytes > SequenceMetadataMaxBytesTotal) return ResourceType.Metadata;

            return null;
        }

        /// <summary>
        /// Check that given SequenceDataCreate satisifes CDF limits and requirements.
        /// </summary>
        /// <param name="seq">Sequence data create to check</param>
        /// <returns>Null if create satisfies limits, otherwise the resource type that fails</returns>
        public static ResourceType? Verify(this SequenceDataCreate seq)
        {
            if (seq == null) throw new ArgumentNullException(nameof(seq));
            if (seq.ExternalId == null && seq.Id == null || !seq.ExternalId.CheckLength(ExternalIdMax)) return ResourceType.ExternalId;
            if (seq.Columns == null || !seq.Columns.Any()) return ResourceType.SequenceColumns;
            if (seq.Rows == null || !seq.Rows.Any()) return ResourceType.SequenceRows;
            return null;
        }

        /// <summary>
        /// Check that the given sequence row is valid
        /// </summary>
        /// <param name="row">Row to check</param>
        /// <param name="seq">Sequence this row belongs to, should be verified</param>
        /// <returns></returns>
        public static ResourceType? Verify(this SequenceRow row, SequenceDataCreate seq)
        {
            if (seq == null) throw new ArgumentNullException(nameof(seq));
            if (row == null) return ResourceType.SequenceRow;
            if (row.Values == null) return ResourceType.SequenceRowValues;
            if (row.Values.Count() != seq.Columns.Count()) return ResourceType.SequenceRowValues;
            foreach (var val in row.Values)
            {
                if (row.RowNumber < 0)
                {
                    return ResourceType.SequenceRowNumber;
                }
                if (val == null) continue;
                if (val is MultiValue.Double doubleVal)
                {
                    if (double.IsNaN(doubleVal.Value) || double.IsInfinity(doubleVal.Value)
                        || doubleVal.Value > CogniteUtils.NumericValueMax || doubleVal.Value < CogniteUtils.NumericValueMin)
                        return ResourceType.SequenceRowValues;
                }
                else if (val is MultiValue.String stringVal)
                {
                    if (!stringVal.Value.CheckLength(CogniteUtils.StringLengthMax)) return ResourceType.SequenceRowValues;
                }
            }
            return null;
        }

        /// <summary>
        /// Clean list of SequenceCreate objects, sanitizing each and removing any duplicates.
        /// The first encountered duplicate is kept.
        /// Invalid sequences due to duplicate column externalIds or other fatal issues are also removed.
        /// </summary>
        /// <param name="sequences">SequenceCreate request to clean</param>
        /// <param name="mode">The type of sanitation to apply</param>
        /// <returns>Cleaned create request and optional errors if any ids were duplicated</returns>
        public static (IEnumerable<SequenceCreate>, IEnumerable<CogniteError>) CleanSequenceRequest(
            IEnumerable<SequenceCreate> sequences,
            SanitationMode mode)
        {
            if (mode == SanitationMode.None) return (sequences, Enumerable.Empty<CogniteError>());
            if (sequences == null) throw new ArgumentNullException(nameof(sequences));

            var result = new List<SequenceCreate>();
            var errors = new List<CogniteError>();

            var ids = new HashSet<string>();
            var duplicated = new HashSet<string>();
            var bad = new List<(ResourceType, SequenceCreate)>();

            foreach (var seq in sequences)
            {
                var columns = new HashSet<string>();
                var duplicatedColumns = new HashSet<string>();
                bool toAdd = true;
                if (mode == SanitationMode.Remove)
                {
                    var failedField = seq.Verify();
                    if (failedField.HasValue)
                    {
                        bad.Add((failedField.Value, seq));
                        toAdd = false;
                    }
                }
                else if (mode == SanitationMode.Clean)
                {
                    if (seq.Columns == null || !seq.Columns.Any())
                    {
                        bad.Add((ResourceType.SequenceColumns, seq));
                        toAdd = false;
                    }
                    else
                    {
                        seq.Sanitize();
                    }
                }

                if (seq.ExternalId != null)
                {
                    if (!ids.Add(seq.ExternalId))
                    {
                        duplicated.Add(seq.ExternalId);
                        toAdd = false;
                    }
                }

                if (seq.Columns != null)
                {
                    foreach (var col in seq.Columns)
                    {
                        if (col.ExternalId == null)
                        {
                            bad.Add((ResourceType.ColumnExternalId, seq));
                            break;
                        }
                        if (!columns.Add(col.ExternalId))
                        {
                            duplicatedColumns.Add(col.ExternalId);
                            toAdd = false;
                        }
                    }
                }
                if (duplicatedColumns.Any())
                {
                    errors.Add(new CogniteError
                    {
                        Status = 409,
                        Message = "Duplicate column externalId",
                        Resource = ResourceType.ColumnExternalId,
                        Type = ErrorType.ItemDuplicated,
                        Values = duplicatedColumns.Select(col => Identity.Create(col)),
                        Skipped = new[] { seq }
                    });
                }

                if (toAdd)
                {
                    result.Add(seq);
                }
            }


            if (duplicated.Any())
            {
                errors.Add(new CogniteError
                {
                    Status = 409,
                    Message = "Duplicate external ids",
                    Resource = ResourceType.ExternalId,
                    Type = ErrorType.ItemDuplicated,
                    Values = duplicated.Select(item => Identity.Create(item)).ToArray()
                });
            }
            if (bad.Any())
            {
                errors.AddRange(bad.GroupBy(pair => pair.Item1).Select(group => new CogniteError
                {
                    Skipped = group.Select(pair => pair.Item2).ToList(),
                    Resource = group.Key,
                    Type = ErrorType.SanitationFailed,
                    Status = 400
                }));
            }
            return (result, errors);
        }

        /// <summary>
        /// Clean list of SequenceDataCreate objects, sanitizing each and removing any duplicates.
        /// The first encountered duplicate is kept.
        /// Invalid sequences due to duplicate column externalIds or other fatal issues are also removed.
        /// Invalid rows are removed individually.
        /// </summary>
        /// <param name="sequences">SequenceCreate request to clean</param>
        /// <param name="mode">The type of sanitation to apply</param>
        /// <returns>Cleaned create request and optional errors if any ids were duplicated</returns>
        public static (IEnumerable<SequenceDataCreate>, IEnumerable<CogniteError>) CleanSequenceDataRequest(
            IEnumerable<SequenceDataCreate> sequences,
            SanitationMode mode)
        {
            if (mode == SanitationMode.None) return (sequences, Enumerable.Empty<CogniteError>());
            if (sequences == null) throw new ArgumentNullException(nameof(sequences));

            var result = new List<SequenceDataCreate>();
            var errors = new List<CogniteError>();

            var comparer = new IdentityComparer();

            var ids = new HashSet<Identity>(comparer);
            var duplicated = new HashSet<Identity>(comparer);
            var bad = new List<(ResourceType, SequenceDataCreate)>();

            var badRowSequences = new List<(ResourceType, SequenceRowError)>();

            foreach (var seq in sequences)
            {
                var columns = new HashSet<string>();
                var duplicatedColumns = new HashSet<string>();
                bool toAdd = true;

                if (mode == SanitationMode.Clean)
                {
                    seq.Sanitize();
                }
                var failedField = seq.Verify();
                if (failedField.HasValue)
                {
                    bad.Add((failedField.Value, seq));
                    toAdd = false;
                }

                var idt = seq.Id.HasValue ? Identity.Create(seq.Id.Value) : Identity.Create(seq.ExternalId);

                if (!ids.Add(idt))
                {
                    duplicated.Add(idt);
                    toAdd = false;
                }

                var badRows = new List<(ResourceType, SequenceRow)>();

                var rowNums = new HashSet<long>();
                var duplicateRows = new List<SequenceRow>();

                if (seq.Columns != null && seq.Rows != null)
                {
                    var goodRows = new List<SequenceRow>(seq.Rows.Count());
                    foreach (var row in seq.Rows)
                    {
                        bool addRow = true;
                        failedField = row.Verify(seq);
                        if (failedField.HasValue)
                        {
                            badRows.Add((failedField.Value, row));
                            addRow = false;
                        }
                        
                        if (!rowNums.Add(row.RowNumber))
                        {
                            duplicateRows.Add(row);
                            addRow = false;
                        }

                        if (addRow)
                        {
                            goodRows.Add(row);
                        }
                        else
                        {
                            CdfMetrics.SequenceRowsSkipped.Inc();
                        }
                    }
                    seq.Rows = goodRows;
                    if (!seq.Rows.Any())
                    {
                        bad.Add((ResourceType.SequenceRows, seq));
                        toAdd = false;
                    }
                }

                if (seq.Columns != null)
                {
                    foreach (var col in seq.Columns)
                    {
                        if (col == null)
                        {
                            bad.Add((ResourceType.ColumnExternalId, seq));
                            break;
                        }
                        if (!columns.Add(col))
                        {
                            duplicatedColumns.Add(col);
                            toAdd = false;
                        }
                    }
                }

                if (duplicatedColumns.Any())
                {
                    errors.Add(new CogniteError
                    {
                        Status = 409,
                        Message = "Duplicate columns",
                        Resource = ResourceType.ColumnExternalId,
                        Type = ErrorType.ItemDuplicated,
                        Values = duplicatedColumns.Select(col => Identity.Create(col)),
                        Skipped = new[] { seq }
                    });
                }
                if (duplicateRows.Any())
                {
                    errors.Add(new CogniteError
                    {
                        Status = 409,
                        Message = "Duplicate row numbers",
                        Resource = ResourceType.SequenceRowNumber,
                        Type = ErrorType.ItemDuplicated,
                        Values = duplicateRows.Select(row => Identity.Create(row.RowNumber)),
                        Skipped = duplicateRows
                    });
                }

                if (badRows.Any())
                {
                    badRowSequences.AddRange(badRows.GroupBy(pair => pair.Item1).Select(group => (
                        group.Key,
                        new SequenceRowError
                        {
                            Id = idt,
                            BadRows = group.Select(pair => pair.Item2).ToList()
                        }
                    )));
                }

                if (toAdd)
                {
                    result.Add(seq);
                }

            }

            if (duplicated.Any())
            {
                errors.Add(new CogniteError
                {
                    Status = 409,
                    Message = "Duplicate internal or external ids",
                    Resource = ResourceType.Id,
                    Type = ErrorType.ItemDuplicated,
                    Values = duplicated.ToArray()
                });
            }
            if (bad.Any())
            {
                errors.AddRange(bad.GroupBy(pair => pair.Item1).Select(group => new CogniteError
                {
                    Skipped = group.Select(pair => pair.Item2).ToList(),
                    Resource = group.Key,
                    Type = ErrorType.SanitationFailed,
                    Status = 400
                }));
            }
            if (badRowSequences.Any())
            {
                errors.AddRange(badRowSequences.GroupBy(pair => pair.Item1).Select(group => new CogniteError
                {
                    Skipped = group.SelectMany(pair => pair.Item2.BadRows),
                    Resource = group.Key,
                    Type = ErrorType.SanitationFailed,
                    Status = 400,
                    Data = group.Select(pair => pair.Item2)
                }));
            }
            return (result, errors);
        }
    }
}
