using CogniteSdk;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Cognite.Extensions
{
    public static class ResultHandlers
    {
        public static CogniteError ParseCommonErrors(ResponseException ex)
        {
            // Handle any common behavior here
            if (ex.Code >= 500 || ex.Code != 400 && ex.Code != 422 && ex.Code != 409) return new CogniteError { Message = ex.Message, Status = ex.Code };
            return null;
        }


        public IEnumerable<CogniteError> ParseException(Exception ex, IEnumerable<AssetCreate> assets)
        {
            if (!(ex is ResponseException rex))
            {
                return new[] { new CogniteError { Message = ex.Message } };
            }
            if (rex.Missing.Any())
            {

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
    }

    public class CogniteResult<TResult> : CogniteResult
    {
        public IEnumerable<TResult> Results { get; }
        public CogniteResult(IEnumerable<CogniteError> errors, IEnumerable<TResult> results) : base(errors)
        {
            Results = results;
        }
    }

    public class CogniteError
    {
        public ErrorType Type { get; set; } = ErrorType.FatalFailure;
        public ResourceType Resource { get; set; } = ResourceType.None;
        public Identity Value { get; set; } = null;
        public int Index { get; set; } = -1;
        public string Message { get; set; }
        public int Status { get; set; }
    }

    public enum ErrorType
    {
        ItemExists,
        ItemMissing,
        FatalFailure
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
}
