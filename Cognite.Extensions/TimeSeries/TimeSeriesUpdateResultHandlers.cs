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
                if (ex.Message.StartsWith("Time series not found", StringComparison.InvariantCultureIgnoreCase))
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

        private static bool IsAffected(
            TimeSeriesUpdateItem item,
            HashSet<Identity> badValues,
            CogniteError<TimeSeriesUpdateItem> error)
        {
            var update = item.Update;
            return error.Resource switch
            {
                ResourceType.DataSetId => update.DataSetId?.Set != null && badValues.Contains(Identity.Create(update.DataSetId.Set.Value)),
                ResourceType.ExternalId => update.ExternalId?.Set != null && badValues.Contains(Identity.Create(update.ExternalId.Set)),
                ResourceType.AssetId => update.AssetId?.Set != null && badValues.Contains(Identity.Create(update.AssetId.Set.Value)),
                ResourceType.Id => badValues.Contains(item),
                _ => false
            };
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
            return CleanFromErrorCommon(error, items, IsAffected, item => item, CdfMetrics.TimeSeriesUpdatesSkipped);
        }
    }
}
