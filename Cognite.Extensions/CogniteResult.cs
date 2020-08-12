using CogniteSdk;
using CogniteSdk.Resources;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.Extensions
{
    public static class ResultHandlers
    {
        private static CogniteError ParseCommonErrors(ResponseException ex)
        {
            // Handle any common behavior here
            if (ex.Code >= 500 || ex.Code != 400 && ex.Code != 422 && ex.Code != 409)
                return new CogniteError { Message = ex.Message, Status = ex.Code, Exception = ex };
            return null;
        }

        private static IEnumerable<Identity> ParseIdString(string idString)
        {
            var ids = new List<Identity>();
            foreach (var id in idString.Split(','))
            {
                if (long.TryParse(id.Trim(), out long internalId))
                {
                    ids.Add(Identity.Create(internalId));
                }
            }
            return ids;
        }

        private static void ParseAssetException(ResponseException ex, CogniteError err)
        {
            if (ex.Missing?.Any() ?? false)
            {
                // TODO add asset labels here once fixed in the API.
            }
            else if (ex.Duplicated?.Any() ?? false)
            {
                // Only externalIds may be duplicated when creating assets
                err.Type = ErrorType.ItemExists;
                err.Resource = ResourceType.ExternalId;
                err.Values = ex.Duplicated.Select(dict =>
                    (dict["externalId"] as MultiValue.String)?.Value)
                    .Where(id => id != null)
                    .Select(Identity.Create);
            }
            else if (ex.Code == 400)
            {
                if (ex.Message.StartsWith("Reference to unknown parent with externalId", StringComparison.InvariantCulture))
                {
                    // Missing parentExternalId only returns one value for some reason.
                    var missingId = ex.Message.Replace("Reference to unknown parent with externalId ", "");
                    err.Complete = false;
                    err.Type = ErrorType.ItemMissing;
                    err.Resource = ResourceType.ParentExternalId;
                    err.Values = new[] { Identity.Create(missingId) };
                }
                else if (ex.Message.StartsWith("The given parent ids do not exist", StringComparison.InvariantCulture))
                {
                    var idString = ex.Message.Replace("The given parent ids do not exist: ", "");
                    err.Type = ErrorType.ItemMissing;
                    err.Resource = ResourceType.ParentId;
                    err.Values = ParseIdString(idString);
                }
                else if (ex.Message.StartsWith("Invalid dataSetIds", StringComparison.InvariantCulture))
                {
                    var idString = ex.Message.Replace("Invalid dataSetIds: ", "");
                    err.Type = ErrorType.ItemMissing;
                    err.Resource = ResourceType.DataSetId;
                    err.Values = ParseIdString(idString);
                }
            }
        }

        private static void ParseTimeSeriesException(ResponseException ex, CogniteError err)
        {
            if (ex.Missing?.Any() ?? false)
            {
                if (ex.Message.StartsWith("Asset ids not found"))
                {
                    err.Type = ErrorType.ItemMissing;
                    err.Resource = ResourceType.AssetId;
                    err.Values = ex.Missing.Select(dict
                        => (dict["id"] as MultiValue.Long)?.Value)
                        .Where(id => id.HasValue)
                        .Select(id => Identity.Create(id.Value));
                }
                else if (ex.Message.StartsWith("datasets ids not found"))
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
                if (ex.Duplicated.First().ContainsKey("legacyName"))
                {
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

        private static void ParseEventException(ResponseException ex, CogniteError err)
        {
            if (ex.Missing?.Any() ?? false)
            {
                if (ex.Message.StartsWith("Asset ids not found"))
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

        public static CogniteError ParseException(Exception ex, RequestType type)
        {
            if (!(ex is ResponseException rex))
            {
                return new CogniteError { Message = ex.Message, Exception = ex };
            }
            var result = ParseCommonErrors(rex);
            if (result == null) result = new CogniteError
            {
                Status = rex.Code,
                Message = rex.Message,
                Exception = ex
            };
            else return result;
            if (type == RequestType.CreateAssets)
            {
                ParseAssetException(rex, result);
            }
            else if (type == RequestType.CreateTimeSeries)
            {
                ParseTimeSeriesException(rex, result);
            }
            else if (type == RequestType.CreateEvents)
            {
                ParseEventException(rex, result);
            }
            return result;
        }

        public static async Task<IEnumerable<AssetCreate>> CleanFromError(
            AssetsResource resource,
            CogniteError error,
            IEnumerable<AssetCreate> assets,
            int assetChunkSize,
            int assetThrottleSize,
            bool emptyOnError,
            CancellationToken token)
        {
            if (error == null) return assets;
            // This is mostly to avoid infinite loops. If there are no bad values then
            // there is no way to correctly clean the request, so there must be something
            // else wrong
            if (!error.Values?.Any() ?? true)
            {
                if (!emptyOnError) return assets;
                error.Values = assets.Where(asset => asset.ExternalId != null).Select(asset => Identity.Create(asset.ExternalId));
                return Array.Empty<AssetCreate>();
            }

            if (!error.Complete)
            {
                await CompleteError(resource, error, assets, assetChunkSize, assetThrottleSize, token);
            }

            var items = new HashSet<Identity>(error.Values, new IdentityComparer());

            var ret = new List<AssetCreate>();

            foreach (var asset in assets)
            {
                switch (error.Resource)
                {
                    case ResourceType.DataSetId:
                        if (!asset.DataSetId.HasValue || !items.Contains(Identity.Create(asset.DataSetId.Value))) ret.Add(asset);
                        break;
                    case ResourceType.ExternalId:
                        if (asset.ExternalId == null || !items.Contains(Identity.Create(asset.ExternalId))) ret.Add(asset);
                        break;
                    case ResourceType.ParentExternalId:
                        if (asset.ParentExternalId == null || !items.Contains(Identity.Create(asset.ParentExternalId))) ret.Add(asset);
                        break;
                    case ResourceType.ParentId:
                        if (!asset.ParentId.HasValue || !items.Contains(Identity.Create(asset.ParentId.Value))) ret.Add(asset);
                        break;
                }
            }
            return ret;
        }

        public static IEnumerable<TimeSeriesCreate> CleanFromError(
            CogniteError error,
            IEnumerable<TimeSeriesCreate> timeseries,
            bool emptyOnError)
        {
            if (error == null) return timeseries;
            if (!error.Values?.Any() ?? true)
            {
                if (!emptyOnError) return timeseries;
                error.Values = timeseries.Where(ts => ts.ExternalId != null).Select(ts => Identity.Create(ts.ExternalId));
                return Array.Empty<TimeSeriesCreate>();
            }

            var items = new HashSet<Identity>(error.Values, new IdentityComparer());

            var ret = new List<TimeSeriesCreate>();

            foreach (var ts in timeseries)
            {
                switch (error.Resource)
                {
                    case ResourceType.DataSetId:
                        if (!ts.DataSetId.HasValue || !items.Contains(Identity.Create(ts.DataSetId.Value))) ret.Add(ts);
                        break;
                    case ResourceType.ExternalId:
                        if (ts.ExternalId == null || !items.Contains(Identity.Create(ts.ExternalId))) ret.Add(ts);
                        break;
                    case ResourceType.AssetId:
                        if (!ts.AssetId.HasValue || !items.Contains(Identity.Create(ts.AssetId.Value))) ret.Add(ts);
                        break;
                    case ResourceType.LegacyName:
                        if (ts.LegacyName == null || !items.Contains(Identity.Create(ts.LegacyName))) ret.Add(ts);
                        break;
                }
            }
            return ret;
        }

        public static IEnumerable<EventCreate> CleanFromError(
            CogniteError error,
            IEnumerable<EventCreate> events,
            bool emptyOnError)
        {
            if (error == null) return events;
            if (!error.Values?.Any() ?? true)
            {
                if (!emptyOnError) return events;
                error.Values = events.Where(evt => evt.ExternalId != null).Select(evt => Identity.Create(evt.ExternalId));
                return Array.Empty<EventCreate>();
            }

            var items = new HashSet<Identity>(error.Values, new IdentityComparer());

            var ret = new List<EventCreate>();

            foreach (var evt in events)
            {
                switch (error.Resource)
                {
                    case ResourceType.DataSetId:
                        if (!evt.DataSetId.HasValue || !items.Contains(Identity.Create(evt.DataSetId.Value))) ret.Add(evt);
                        break;
                    case ResourceType.ExternalId:
                        if (evt.ExternalId == null || !items.Contains(Identity.Create(evt.ExternalId))) ret.Add(evt);
                        break;
                    case ResourceType.AssetId:
                        if (evt.AssetIds == null || !evt.AssetIds.Any(id => items.Contains(Identity.Create(id)))) ret.Add(evt);
                        break;
                }
            }
            return ret;
        }

        private static async Task CompleteError(
            AssetsResource resource,
            CogniteError error,
            IEnumerable<AssetCreate> assets,
            int assetChunkSize,
            int assetThrottleSize,
            CancellationToken token)
        {
            if (error.Complete) return;

            if (error.Resource == ResourceType.ParentExternalId)
            {
                var ids = assets.Select(asset => asset.ParentExternalId)
                    .Where(id => id != null)
                    .Distinct()
                    .Select(Identity.Create)
                    .Except(error.Values, new IdentityComparer());

                try
                {
                    var parents = await resource.GetAssetsByIdsIgnoreErrors(ids, assetChunkSize, assetThrottleSize, token);

                    error.Complete = true;
                    error.Values = ids
                        .Except(parents.Select(asset => Identity.Create(asset.ExternalId)))
                        .Concat(error.Values)
                        .Distinct(new IdentityComparer());
                }
                catch
                {
                    return;
                }
            }
        }
    }

    public class CogniteResult
    {
        public IEnumerable<CogniteError> Errors { get; set; }
        public bool IsAllGood => !Errors?.Any() ?? true;
        public CogniteResult(IEnumerable<CogniteError> errors)
        {
            Errors = errors;
        }
        public CogniteResult Merge(CogniteResult other)
        {
            if (other == null) return this;
            IEnumerable<CogniteError> errors;

            if (Errors == null) errors = other.Errors;
            else if (other.Errors == null) errors = Errors;
            else errors = Errors.Concat(other.Errors);

            return new CogniteResult(errors);
        }
    }

    public class CogniteResult<TResult> : CogniteResult
    {
        public IEnumerable<TResult> Results { get; set; }
        public CogniteResult(IEnumerable<CogniteError> errors, IEnumerable<TResult> results) : base(errors)
        {
            Results = results;
        }
        public CogniteResult<TResult> Merge(CogniteResult<TResult> other)
        {
            if (other == null) return this;
            IEnumerable<CogniteError> errors;
            IEnumerable<TResult> results;
            if (Results == null) results = other.Results;
            else if (other.Results == null) results = Results;
            else results = Results.Concat(other.Results);

            if (Errors == null) errors = other.Errors;
            else if (other.Errors == null) errors = Errors;
            else errors = Errors.Concat(other.Errors);

            return new CogniteResult<TResult>(errors, results);
        }
    }

    public class CogniteError
    {
        public ErrorType Type { get; set; } = ErrorType.FatalFailure;
        public ResourceType Resource { get; set; } = ResourceType.None;
        public IEnumerable<Identity> Values { get; set; } = null;
        public Exception Exception { get; set; }
        public string Message { get; set; }
        public int Status { get; set; }
        public bool Complete { get; set; } = true;
    }

    public enum ErrorType
    {
        ItemExists,
        ItemMissing,
        ItemDuplicated,
        FatalFailure = -1
    }
    public enum ResourceType
    {
        Id,
        ExternalId,
        AssetId,
        ParentId,
        ParentExternalId,
        DataSetId,
        LegacyName,
        None = -1
    }
    public enum RequestType
    {
        CreateAssets,
        CreateTimeSeries,
        CreateEvents
    }
}
