using CogniteSdk;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Cognite.Extensions
{
    public static partial class ResultHandlers
    {
        private static void ParseTimeSeriesException(ResponseException ex, CogniteError err)
        {
            if (ex.Missing?.Any() ?? false)
            {
                if (ex.Message.StartsWith("Asset ids not found", StringComparison.InvariantCultureIgnoreCase))
                {
                    err.Type = ErrorType.ItemMissing;
                    err.Resource = ResourceType.AssetId;
                    err.Values = ex.Missing.Select(dict
                        => (dict["id"] as MultiValue.Long)?.Value)
                        .Where(id => id.HasValue)
                        .Select(id => Identity.Create(id!.Value));
                }
                else if (ex.Message.StartsWith("Datasets ids not found", StringComparison.InvariantCultureIgnoreCase)
                        || ex.Message.StartsWith("Data set ids not found", StringComparison.InvariantCultureIgnoreCase))
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
                if (ex.Duplicated.First().ContainsKey("legacyName"))
                {
                    //TODO: legacyName will be ignored when the 0.5 api is removed. 
                    // Should remove this check then.
                    err.Type = ErrorType.ItemExists;
                    err.Resource = ResourceType.LegacyName;
                    err.Values = ex.Duplicated.Select(dict
                        => (dict["legacyName"] as MultiValue.String)?.Value)
                        .Where(id => id != null)
                        .Select(Identity.Create);
                }
                else if (ex.Duplicated.First().ContainsKey("externalId"))
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
        /// Clean list of TimeSeriesCreate objects based on CogniteError
        /// </summary>
        /// <param name="error">Error that occured with a previous push</param>
        /// <param name="timeseries">Timeseries to clean</param>
        /// <returns>TimeSeries that are not affected by the error</returns>
        public static IEnumerable<TimeSeriesCreate> CleanFromError(
            CogniteError<TimeSeriesCreate> error,
            IEnumerable<TimeSeriesCreate> timeseries)
        {
            if (timeseries == null)
            {
                throw new ArgumentNullException(nameof(timeseries));
            }
            if (error == null) return timeseries;
            if (!error.Values?.Any() ?? true)
            {
                error.Values = timeseries.Where(ts => ts.ExternalId != null).Select(ts => Identity.Create(ts.ExternalId));
                return Array.Empty<TimeSeriesCreate>();
            }

            var items = new HashSet<Identity>(error.Values);

            var ret = new List<TimeSeriesCreate>();
            var skipped = new List<TimeSeriesCreate>();

            foreach (var ts in timeseries)
            {
                bool added = false;
                switch (error.Resource)
                {
                    case ResourceType.DataSetId:
                        if (!ts.DataSetId.HasValue || !items.Contains(Identity.Create(ts.DataSetId.Value))) added = true;
                        break;
                    case ResourceType.ExternalId:
                        if (ts.ExternalId == null || !items.Contains(Identity.Create(ts.ExternalId))) added = true;
                        break;
                    case ResourceType.AssetId:
                        if (!ts.AssetId.HasValue || !items.Contains(Identity.Create(ts.AssetId.Value))) added = true;
                        break;
                    case ResourceType.LegacyName:
                        if (ts.LegacyName == null || !items.Contains(Identity.Create(ts.LegacyName))) added = true;
                        break;
                }
                if (added)
                {
                    ret.Add(ts);
                }
                else
                {
                    CdfMetrics.TimeSeriesSkipped.Inc();
                    skipped.Add(ts);
                }
            }
            if (skipped.Any())
            {
                error.Skipped = skipped;
            }
            else
            {
                error.Skipped = timeseries;
                return Array.Empty<TimeSeriesCreate>();
            }
            return ret;
        }
    }
}
