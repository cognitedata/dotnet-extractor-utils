using CogniteSdk;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Cognite.Extensions
{
    public static partial class ResultHandlers
    {
        private static void ParseEventException(ResponseException ex, CogniteError err)
        {
            if (ex.Missing?.Any() ?? false)
            {
                if (ex.Message.StartsWith("Asset ids not found", StringComparison.InvariantCulture))
                {
                    err.Type = ErrorType.ItemMissing;
                    err.Resource = ResourceType.AssetId;
                    err.Values = ex.Missing.Select(dict =>
                        (dict["id"] as MultiValue.Long)?.Value)
                        .Where(id => id.HasValue)
                        .Select(id => Identity.Create(id!.Value));
                }
            }
            else if (ex.Duplicated?.Any() ?? false)
            {
                err.Type = ErrorType.ItemExists;
                err.Resource = ResourceType.ExternalId;
                err.Values = ex.Duplicated.Select(dict =>
                    (dict["externalId"] as MultiValue.String)?.Value)
                    .Where(id => id != null)
                    .Select(Identity.Create);
            }
            else if (ex.Code == 400)
            {
                if (ex.Message.StartsWith("Invalid dataSetIds", StringComparison.InvariantCulture))
                {
                    var idString = ex.Message.Replace("Invalid dataSetIds: ", "");
                    err.Type = ErrorType.ItemMissing;
                    err.Resource = ResourceType.DataSetId;
                    err.Values = ParseIdString(idString);
                }
            }
        }

        private static bool IsAffected(EventCreate evt, HashSet<Identity> badValues, CogniteError<EventCreate> error)
        {
            return error.Resource switch
            {
                ResourceType.DataSetId => badValues.ContainsIdentity(evt.DataSetId),
                ResourceType.ExternalId => badValues.ContainsIdentity(evt.ExternalId),
                ResourceType.AssetId => evt.AssetIds != null && evt.AssetIds.Any(id => badValues.ContainsIdentity(id)),
                _ => false
            };
        }

        /// <summary>
        /// Clean list of EventCreate objects based on CogniteError
        /// </summary>
        /// <param name="error">Error that occurred with a previous push</param>
        /// <param name="events">Events to clean</param>
        /// <returns>Events that are not affected by the error</returns>
        public static IEnumerable<EventCreate> CleanFromError(
            CogniteError<EventCreate> error,
            IEnumerable<EventCreate> events)
        {
            return CleanFromErrorCommon(error, events, IsAffected,
                evt => evt.ExternalId == null ? null : Identity.Create(evt.ExternalId),
                CdfMetrics.EventsSkipped);
        }
    }
}
