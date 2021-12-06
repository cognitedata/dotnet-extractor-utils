using CogniteSdk;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Cognite.Extensions
{
    public partial class ResultHandlers
    {
        private static void ParseTimeSeriesUpdateException(ResponseException ex, CogniteError err)
        {
            if (ex.Missing?.Any() ?? false)
            {
                if (ex.Message.StartsWith("Time series ids not found", StringComparison.InvariantCultureIgnoreCase))
                {
                    err.Type = ErrorType.ItemMissing;
                    err.Resource = ResourceType.Id;
                    err.Values = ex.Missing.Select(dict =>
                    {
                        if (dict.TryGetValue("id", out var idVal) && idVal is MultiValue.Long longIdVal)
                        {
                            return Identity.Create(longIdVal.Value);
                        }
                        else if (dict.TryGetValue("externalId", out var extIdVal)
                            && extIdVal is MultiValue.String strIdVal
                            && strIdVal.Value != null)
                        {
                            return Identity.Create(strIdVal.Value);
                        }
                        return null!;
                    }).Where(id => id != null);
                }
                else if (ex.Message.StartsWith("Asset ids not found", StringComparison.InvariantCultureIgnoreCase))
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
                err.Type = ErrorType.ItemExists;
                err.Resource = ResourceType.ExternalId;
                err.Values = ex.Duplicated.Select(dict
                    => (dict["externalId"] as MultiValue.String)?.Value)
                    .Where(id => id != null)
                    .Select(Identity.Create);
            }
        }

        /// <summary>
        /// Clean list of TimeSeriesUpdateItem objects based on CogniteError
        /// </summary>
        /// <param name="error">Error that occured with a previous push</param>
        /// <param name="items">Timeseries updates to clean</param>
        /// <returns>TimeSeries updates that are not affected by the error</returns>
        public static IEnumerable<TimeSeriesUpdateItem> CleanFromError(
            CogniteError<TimeSeriesUpdateItem> error,
            IEnumerable<TimeSeriesUpdateItem> items)
        {
            if (items == null)
            {
                throw new ArgumentNullException(nameof(items));
            }
            if (error == null) return items;
            if (!error.Values?.Any() ?? true)
            {
                error.Values = items.Where(ts => ts.ExternalId != null).Select(ts => Identity.Create(ts.ExternalId));
                return Array.Empty<TimeSeriesUpdateItem>();
            }

            var badValues = new HashSet<Identity>(error.Values);

            var ret = new List<TimeSeriesUpdateItem>();
            var skipped = new List<TimeSeriesUpdateItem>();

            foreach (var item in items)
            {
                bool added = false;
                var update = item.Update;

                switch (error.Resource)
                {
                    case ResourceType.DataSetId:
                        if (update.DataSetId?.Set == null
                            || !badValues.Contains(Identity.Create(update.DataSetId.Set.Value))) added = true;
                        break;
                    case ResourceType.ExternalId:
                        if (update.ExternalId?.Set == null
                            || !badValues.Contains(Identity.Create(update.ExternalId.Set))) added = true;
                        break;
                    case ResourceType.AssetId:
                        if (update.AssetId?.Set == null
                            || !badValues.Contains(Identity.Create(update.AssetId.Set.Value))) added = true;
                        break;
                    case ResourceType.Id:
                        var idt = item.Id.HasValue ? Identity.Create(item.Id.Value) : Identity.Create(item.ExternalId);

                        if (!badValues.Contains(idt)) added = true;
                        break;
                }
                if (added)
                {
                    ret.Add(item);
                }
                else
                {
                    CdfMetrics.TimeSeriesSkipped.Inc();
                    skipped.Add(item);
                }
            }
            if (skipped.Any())
            {
                error.Skipped = skipped;
            }
            else
            {
                error.Skipped = items;
                return Enumerable.Empty<TimeSeriesUpdateItem>();
            }
            return ret;
        }
    }
}
