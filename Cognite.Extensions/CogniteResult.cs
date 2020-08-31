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

        /// <summary>
        /// Parse exception into CogniteError which describes the error in detail.
        /// </summary>
        /// <param name="ex">Exception to parse</param>
        /// <param name="type">Request type</param>
        /// <returns>CogniteError representation of the exception</returns>
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

        /// <summary>
        /// Clean list of AssetCreate objects based on CogniteError object
        /// </summary>
        /// <param name="resource">CogniteSdk assets resource</param>
        /// <param name="error">Error that occured with a previous push</param>
        /// <param name="assets">Assets to clean</param>
        /// <param name="assetChunkSize">Maximum number of ids per asset read</param>
        /// <param name="assetThrottleSize">Maximum number of parallel asset read requests</param>
        /// <param name="token"></param>
        /// <returns>Assets that are not affected by the error</returns>
        public static async Task<IEnumerable<AssetCreate>> CleanFromError(
            AssetsResource resource,
            CogniteError error,
            IEnumerable<AssetCreate> assets,
            int assetChunkSize,
            int assetThrottleSize,
            CancellationToken token)
        {
            if (error == null) return assets;
            // This is mostly to avoid infinite loops. If there are no bad values then
            // there is no way to correctly clean the request, so there must be something
            // else wrong
            if (!error.Values?.Any() ?? true)
            {
                error.Values = assets.Where(asset => asset.ExternalId != null).Select(asset => Identity.Create(asset.ExternalId));
                return Array.Empty<AssetCreate>();
            }

            if (!error.Complete)
            {
                await CompleteError(resource, error, assets, assetChunkSize, assetThrottleSize, token);
            }

            var items = new HashSet<Identity>(error.Values, new IdentityComparer());

            var ret = new List<AssetCreate>();
            var skipped = new List<object>();

            foreach (var asset in assets)
            {
                bool added = false;
                switch (error.Resource)
                {
                    case ResourceType.DataSetId:
                        if (!asset.DataSetId.HasValue || !items.Contains(Identity.Create(asset.DataSetId.Value))) added = true;
                        break;
                    case ResourceType.ExternalId:
                        if (asset.ExternalId == null || !items.Contains(Identity.Create(asset.ExternalId))) added = true;
                        break;
                    case ResourceType.ParentExternalId:
                        if (asset.ParentExternalId == null || !items.Contains(Identity.Create(asset.ParentExternalId))) added = true;
                        break;
                    case ResourceType.ParentId:
                        if (!asset.ParentId.HasValue || !items.Contains(Identity.Create(asset.ParentId.Value))) added = true;
                        break;
                }
                if (added)
                {
                    ret.Add(asset);
                }
                else
                {
                    CdfMetrics.AssetsSkipped.Inc();
                    skipped.Add(asset);
                }
            }
            if (skipped.Any())
            {
                error.Skipped = skipped;
            }
            return ret;
        }

        /// <summary>
        /// Clean list of TimeSeriesCreate objects based on CogniteError
        /// </summary>
        /// <param name="error">Error that occured with a previous push</param>
        /// <param name="timeseries">Timeseries to clean</param>
        /// <returns>TimeSeries that are not affected by the error</returns>
        public static IEnumerable<TimeSeriesCreate> CleanFromError(
            CogniteError error,
            IEnumerable<TimeSeriesCreate> timeseries)
        {
            if (error == null) return timeseries;
            if (!error.Values?.Any() ?? true)
            {
                error.Values = timeseries.Where(ts => ts.ExternalId != null).Select(ts => Identity.Create(ts.ExternalId));
                return Array.Empty<TimeSeriesCreate>();
            }

            var items = new HashSet<Identity>(error.Values, new IdentityComparer());

            var ret = new List<TimeSeriesCreate>();
            var skipped = new List<object>();

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
            return ret;
        }

        /// <summary>
        /// Clean list of EventCreate objects based on CogniteError
        /// </summary>
        /// <param name="error">Error that occured with a previous push</param>
        /// <param name="events">Events to clean</param>
        /// <param name="emptyOnError">True if a fatal error should remove all entries,
        /// if this is false, a broken connection or similar may cause very long loops.</param>
        /// <returns>Events that are not affected by the error</returns>
        public static IEnumerable<EventCreate> CleanFromError(
            CogniteError error,
            IEnumerable<EventCreate> events)
        {
            if (error == null) return events;
            if (!error.Values?.Any() ?? true)
            {
                error.Values = events.Where(evt => evt.ExternalId != null).Select(evt => Identity.Create(evt.ExternalId));
                return Array.Empty<EventCreate>();
            }

            var items = new HashSet<Identity>(error.Values, new IdentityComparer());

            var ret = new List<EventCreate>();
            var skipped = new List<object>();

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

    /// <summary>
    /// Represents the result of one or more pushes to CDF.
    /// Contains a list of errors, one for each failed push, and potentially pre-push santiation.
    /// </summary>
    public class CogniteResult
    {
        /// <summary>
        /// Errors that have occured in this series of requests
        /// </summary>
        public IEnumerable<CogniteError> Errors { get; set; }
        /// <summary>
        /// True if nothing went wrong
        /// </summary>
        public bool IsAllGood => !Errors?.Any() ?? true;
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="errors">Initial list of errors</param>
        public CogniteResult(IEnumerable<CogniteError> errors)
        {
            Errors = errors;
        }
        /// <summary>
        /// Return a new CogniteResult that contains errors from both
        /// </summary>
        /// <param name="other">CogniteResult to merge with</param>
        /// <returns>A new result containing the CogniteErrors from both results</returns>
        public CogniteResult Merge(CogniteResult other)
        {
            if (other == null) return this;
            IEnumerable<CogniteError> errors;

            if (Errors == null) errors = other.Errors;
            else if (other.Errors == null) errors = Errors;
            else errors = Errors.Concat(other.Errors);

            return new CogniteResult(errors);
        }

        public static CogniteResult Merge(params CogniteResult[] results)
        {
            var errors = new List<CogniteError>();
            foreach (var result in results)
            {
                if (result == null) continue;
                if (result.Errors != null) errors.AddRange(result.Errors);
            }
            return new CogniteResult(errors);
        }
    }

    /// <summary>
    /// Represents the result of one or more pushes to CDF.
    /// Contains a list of errors, one for each failed push, and potentially pre-push santiation,
    /// as well as a list of result objects.
    /// </summary>
    /// <typeparam name="TResult"></typeparam>
    public class CogniteResult<TResult> : CogniteResult
    {
        /// <summary>
        /// A list of results
        /// </summary>
        public IEnumerable<TResult> Results { get; set; }
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="errors">Initial list of errors</param>
        /// <param name="results">Initial list of results</param>
        public CogniteResult(IEnumerable<CogniteError> errors, IEnumerable<TResult> results) : base(errors)
        {
            Results = results;
        }
        /// <summary>
        /// Return a new CogniteResult that contains errors and results from both
        /// </summary>
        /// <param name="other">CogniteResult to merge with</param>
        /// <returns>A new result containing errors and results from both</returns>
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

        public static CogniteResult<TResult> Merge(params CogniteResult<TResult>[] results)
        {
            var items = new List<TResult>();
            var errors = new List<CogniteError>();
            foreach (var result in results)
            {
                if (result == null) continue;
                if (result.Results != null) items.AddRange(result.Results);
                if (result.Errors != null) errors.AddRange(result.Errors);
            }
            return new CogniteResult<TResult>(errors, items);
        }
    }

    /// <summary>
    /// Represents an error that occured on a push to CDF, or
    /// in pre-push sanitation.
    /// </summary>
    public class CogniteError
    {
        /// <summary>
        /// Type of error, either pre-existing in CDF, duplicated in request,
        /// missing from CDF, or a fatal error.
        /// </summary>
        public ErrorType Type { get; set; } = ErrorType.FatalFailure;
        /// <summary>
        /// Affected resource if pre-existing, missing or duplicated.
        /// </summary>
        public ResourceType Resource { get; set; } = ResourceType.None;
        /// <summary>
        /// Values of the affected resources as CogniteSdk identities.
        /// </summary>
        public IEnumerable<Identity> Values { get; set; } = null;
        /// <summary>
        /// Input items skipped if the request was cleaned using this error.
        /// </summary>
        public IEnumerable<object> Skipped { get; set; } = null;
        /// <summary>
        /// Exception that caused this error, if any.
        /// </summary>
        public Exception Exception { get; set; }
        /// <summary>
        /// Message describing this error.
        /// </summary>
        public string Message { get; set; }
        /// <summary>
        /// HTTP status code, if any.
        /// </summary>
        public int Status { get; set; }
        /// <summary>
        /// False if this needs further work in order to complete,
        /// because the error message from CDF does not contain sufficient
        /// information.
        /// </summary>
        public bool Complete { get; set; } = true;
    }

    /// <summary>
    /// General type of error
    /// </summary>
    public enum ErrorType
    {
        /// <summary>
        /// Items already exist in CDF
        /// </summary>
        ItemExists,
        /// <summary>
        /// Items are missing from CDF
        /// </summary>
        ItemMissing,
        /// <summary>
        /// Items were duplicated in request
        /// </summary>
        ItemDuplicated,
        /// <summary>
        /// Item does not satisfy CDF field limits
        /// </summary>
        SanitationFailed,
        /// <summary>
        /// Something else happened that caused the request to fail
        /// </summary>
        FatalFailure = -1
    }
    /// <summary>
    /// Type of CDF attribute that caused this error
    /// </summary>
    public enum ResourceType
    {
        /// <summary>
        /// Cognite internal id
        /// </summary>
        Id,
        /// <summary>
        /// Cognite external id
        /// </summary>
        ExternalId,
        /// <summary>
        /// Asset id on a timeseries or event
        /// </summary>
        AssetId,
        /// <summary>
        /// Parent internal id on an asset
        /// </summary>
        ParentId,
        /// <summary>
        /// Parent external id on an asset
        /// </summary>
        ParentExternalId,
        /// <summary>
        /// Data set id on an asset, event or timeseries
        /// </summary>
        DataSetId,
        /// <summary>
        /// LegacyName on a timeseries
        /// </summary>
        LegacyName,
        /// <summary>
        /// Name on an asset or timeseries
        /// </summary>
        Name,
        /// <summary>
        /// Type of event
        /// </summary>
        Type,
        /// <summary>
        /// SubType of event
        /// </summary>
        SubType,
        /// <summary>
        /// Source on event or asset
        /// </summary>
        Source,
        /// <summary>
        /// Metadata on event, asset or timeseries
        /// </summary>
        Metadata,
        /// <summary>
        /// Labels on an asset
        /// </summary>
        Labels,
        /// <summary>
        /// Description on event, asset or timeseries
        /// </summary>
        Description,
        /// <summary>
        /// Start and end time on an event
        /// </summary>
        TimeRange,
        /// <summary>
        /// Unit on a timeseries
        /// </summary>
        Unit,
        /// <summary>
        /// None or unknown
        /// </summary>
        None = -1
    }
    /// <summary>
    /// Type of request that caused an error
    /// </summary>
    public enum RequestType
    {
        /// <summary>
        /// Create assets
        /// </summary>
        CreateAssets,
        /// <summary>
        /// Create timeseries
        /// </summary>
        CreateTimeSeries,
        /// <summary>
        /// Create events
        /// </summary>
        CreateEvents
    }
    

    /// <summary>
    /// When to retry a request
    /// For convenience, bit 0 indicates keeping duplicates,
    /// bit 1 indicates retrying on errors,
    /// bit 2 indicates retrying on fatal errors
    /// </summary>
    public enum RetryMode
    {
        /// <summary>
        /// Never retry, always stop after the first failure,
        /// multiple errors may still occur due to chunking.
        /// </summary>
        None = 0,
        /// <summary>
        /// Retry after a 4xx error that can be handled by cleaning the
        /// request
        /// </summary>
        OnError = 2,
        /// <summary>
        /// Retry after a 4xx error that can be handled by cleaning the
        /// request, but keep retrying for duplicates until they are
        /// returned when reading
        /// </summary>
        OnErrorKeepDuplicates = 3,
        /// <summary>
        /// Same as OnError, but keep retrying if a fatal error occurs
        /// </summary>
        OnFatal = 6,
        /// <summary>
        /// Same as OnErrorKeepDuplicates, but keep retrying if a fatal error occurs
        /// </summary>
        OnFatalKeepDuplicates = 7
    }
}
