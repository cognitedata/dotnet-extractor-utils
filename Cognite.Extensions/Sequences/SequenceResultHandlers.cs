using CogniteSdk;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

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
    }
}
