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
                        .Select(id => Identity.Create(id.Value));
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
            if (events == null)
            {
                throw new ArgumentNullException(nameof(events));
            }
            if (error == null) return events;
            if (!error.Values?.Any() ?? true)
            {
                error.Values = events.Where(evt => evt.ExternalId != null).Select(evt => Identity.Create(evt.ExternalId));
                return Array.Empty<EventCreate>();
            }

            var items = new HashSet<Identity>(error.Values);

            var ret = new List<EventCreate>();
            var skipped = new List<EventCreate>();

            foreach (var evt in events)
            {
                bool added = false;
                switch (error.Resource)
                {
                    case ResourceType.DataSetId:
                        if (!evt.DataSetId.HasValue || !items.Contains(Identity.Create(evt.DataSetId.Value))) added = true;
                        else CdfMetrics.EventsSkipped.Inc();
                        break;
                    case ResourceType.ExternalId:
                        if (evt.ExternalId == null || !items.Contains(Identity.Create(evt.ExternalId))) added = true;
                        else CdfMetrics.EventsSkipped.Inc();
                        break;
                    case ResourceType.AssetId:
                        if (evt.AssetIds == null || !evt.AssetIds.Any(id => items.Contains(Identity.Create(id)))) added = true;
                        break;
                }
                if (added)
                {
                    ret.Add(evt);
                }
                else
                {
                    CdfMetrics.EventsSkipped.Inc();
                    skipped.Add(evt);
                }
            }
            if (skipped.Any())
            {
                error.Skipped = skipped;
            }
            else
            {
                error.Skipped = events;
                return Array.Empty<EventCreate>();
            }
            return ret;
        }
    }
}
