using CogniteSdk;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Cognite.Extensions
{
    public static partial class ResultHandlers
    {
        private static void ParseDatapointsException(ResponseException ex, CogniteError err)
        {
            if (ex.Missing?.Any() ?? false)
            {
                err.Type = ErrorType.ItemMissing;
                err.Resource = ResourceType.ExternalId;
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
            else if (ex.Message == "Expected string value for datapoint" || ex.Message == "Expected numeric value for datapoint")
            {
                err.Type = ErrorType.MismatchedType;
                err.Resource = ResourceType.DataPointValue;
                err.Complete = false;
            }
        }

        /// <summary>
        /// Clean a list of identity/datapoint pairs based on CogniteError
        /// </summary>
        /// <param name="error">Error to clean from</param>
        /// <param name="datapoints">Datapoints to remove</param>
        /// <returns>A modified version of <paramref name="datapoints"/> or an empty dictionary</returns>
        public static IDictionary<Identity, IEnumerable<Datapoint>> CleanFromError(
            CogniteError error,
            IDictionary<Identity, IEnumerable<Datapoint>> datapoints)
        {
            if (datapoints == null) throw new ArgumentNullException(nameof(datapoints));
            if (error == null) return datapoints;
            // In this case we've already finished the skipping.
            if (error.Skipped?.Any() ?? false) return datapoints;
            if (!error.Values.Any())
            {
                error.Skipped = datapoints.Select(kvp => new DataPointInsertError(kvp.Key, kvp.Value)).ToList();
                error.Values = error.Skipped.Select(pair => ((DataPointInsertError)pair).Id);
                return new Dictionary<Identity, IEnumerable<Datapoint>>(new IdentityComparer());
            }

            var skipped = new List<object>();

            foreach (var idt in error.Values)
            {
                if (!datapoints.TryGetValue(idt, out var dps)) continue;
                skipped.Add(new DataPointInsertError(idt, dps));
                CdfMetrics.DatapointsSkipped.Inc(dps.Count());
                datapoints.Remove(idt);
                CdfMetrics.DatapointTimeseriesSkipped.Inc();
            }

            if (skipped.Any())
            {
                error.Skipped = skipped;
            }
            else
            {
                error.Skipped = datapoints.Select(kvp => new DataPointInsertError(kvp.Key, kvp.Value)).ToList();
                return new Dictionary<Identity, IEnumerable<Datapoint>>(new IdentityComparer());
            }

            return datapoints;
        }
    }
}
