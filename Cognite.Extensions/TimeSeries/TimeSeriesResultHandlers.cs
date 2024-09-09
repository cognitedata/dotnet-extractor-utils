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

        private static bool IsAffected(TimeSeriesCreate ts, HashSet<IIdentity> badValues, CogniteError<TimeSeriesCreate> error)
        {
            return error.Resource switch
            {
                ResourceType.DataSetId => badValues.ContainsIdentity(ts.DataSetId),
                ResourceType.ExternalId => badValues.ContainsIdentity(ts.ExternalId),
                ResourceType.AssetId => badValues.ContainsIdentity(ts.AssetId),
                ResourceType.LegacyName => badValues.ContainsIdentity(ts.LegacyName),
                _ => false,
            };
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
            return CleanFromErrorCommon(error, timeseries, IsAffected,
                ts => ts.ExternalId == null ? null : Identity.Create(ts.ExternalId),
                CdfMetrics.TimeSeriesSkipped);
        }
    }
}
