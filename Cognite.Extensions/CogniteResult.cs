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

            if (rex.Missing?.Any() ?? false)
            {
                // TODO add asset labels here once fixed in the API.
            }
            else if (rex.Duplicated?.Any() ?? false)
            {
                if (type == RequestType.CreateAssets)
                {
                    // Only externalIds may be duplicated when creating assets
                    result.Type = ErrorType.ItemExists;
                    result.Message = rex.Message;
                    result.Resource = ResourceType.ExternalId;
                    result.Values = rex.Duplicated.Select(dict =>
                        (dict["externalId"] as MultiValue.String)?.Value)
                        .Where(id => id != null)
                        .Select(Identity.Create);
                }
            }
            else if (rex.Code == 400)
            {
                if (type == RequestType.CreateAssets)
                {
                    if (rex.Message.StartsWith("Reference to unknown parent with externalId", StringComparison.InvariantCulture))
                    {
                        // Missing parentExternalId only returns one value for some reason.
                        var missingId = rex.Message.Replace("Reference to unknown parent with externalId ", "");
                        result.Complete = false;
                        result.Type = ErrorType.ItemMissing;
                        result.Resource = ResourceType.ParentExternalId;
                        result.Values = new[] { Identity.Create(missingId) };
                    }
                    else if (rex.Message.StartsWith("The given parent ids do not exist", StringComparison.InvariantCulture))
                    {
                        var idString = rex.Message.Replace("The given parent ids do not exist: ", "");
                        result.Type = ErrorType.ItemMissing;
                        result.Resource = ResourceType.ParentId;
                        result.Values = ParseIdString(idString);
                    }
                    else if (rex.Message.StartsWith("Invalid dataSetIds", StringComparison.InvariantCulture))
                    {
                        var idString = rex.Message.Replace("Invalid dataSetIds: ", "");
                        result.Type = ErrorType.ItemMissing;
                        result.Resource = ResourceType.DataSetId;
                        result.Values = ParseIdString(idString);
                    }
                }
            }
            return result;
        }

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
            if (!error.Values?.Any() ?? true) return Array.Empty<AssetCreate>();
            
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
                        if (!asset.DataSetId.HasValue || !items.Contains(Identity.Create(asset.DataSetId.Value)))
                        {
                            ret.Add(asset);
                        }
                        break;
                    case ResourceType.ExternalId:
                        if (asset.ExternalId == null || !items.Contains(Identity.Create(asset.ExternalId)))
                        {
                            ret.Add(asset);
                        }
                        break;
                    case ResourceType.ParentExternalId:
                        if (asset.ParentExternalId == null || !items.Contains(Identity.Create(asset.ParentExternalId)))
                        {
                            ret.Add(asset);
                        }
                        break;
                    case ResourceType.ParentId:
                        if (!asset.ParentId.HasValue || !items.Contains(Identity.Create(asset.ParentId.Value)))
                        {
                            ret.Add(asset);
                        }
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
        public IEnumerable<CogniteError> Errors { get; }
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
        None = -1
    }
    public enum RequestType
    {
        CreateAssets,
        CreateTimeSeries,
        CreateEvents
    }
}
