﻿using CogniteSdk;
using CogniteSdk.Resources;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.Extensions
{
    /// <summary>
    /// Class containing static methods to parse errors from the SDK and clean request objects
    /// </summary>
    public static partial class ResultHandlers
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

        /// <summary>
        /// Parse exception into CogniteError which describes the error in detail.
        /// </summary>
        /// <param name="ex">Exception to parse</param>
        /// <param name="type">Request type</param>
        /// <returns>CogniteError representation of the exception</returns>
        public static CogniteError ParseException(Exception ex, RequestType type)
        {
            if (ex == null)
            {
                throw new ArgumentNullException(nameof(ex));
            }
            if (ex is ResponseException rex)
            {
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
                else if (type == RequestType.CreateSequences)
                {
                    ParseSequencesException(rex, result);
                }
                return result;
            }
            else
            {
                return new CogniteError { Message = ex.Message, Exception = ex };
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

        /// <summary>
        /// Return a new CogniteResult that contains errors from all <paramref name="results"/> given as parameter
        /// </summary>
        /// <param name="results">List of CogniteResult to merge with</param>
        /// <returns>A new result containing the CogniteErrors from all results given as parameter</returns>
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

        /// <summary>
        /// Return a new CogniteResult of type <typeparamref name="TResult"/> that contains errors from all <paramref name="results"/> given as parameter
        /// </summary>
        /// <param name="results">List of CogniteResult to merge with</param>
        /// <returns>A new result containing the CogniteErrors from all results given as parameter</returns>
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
        public IEnumerable<Identity> Values { get; set; }
        /// <summary>
        /// Input items skipped if the request was cleaned using this error.
        /// </summary>
        public IEnumerable<object> Skipped { get; set; }
        /// <summary>
        /// Further information about the error, for some errors.
        /// </summary>
        public IEnumerable<object> Data { get; set; }
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
        /// The Columns field on a sequence
        /// </summary>
        SequenceColumns,
        /// <summary>
        /// Name of a sequence column
        /// </summary>
        ColumnName,
        /// <summary>
        /// Description of a sequence column
        /// </summary>
        ColumnDescription,
        /// <summary>
        /// ExternalId of a sequence column
        /// </summary>
        ColumnExternalId,
        /// <summary>
        /// Metadata of a sequence column
        /// </summary>
        ColumnMetadata,
        /// <summary>
        /// Row in a sequence
        /// </summary>
        SequenceRow,
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
        CreateEvents,
        /// <summary>
        /// Create sequences
        /// </summary>
        CreateSequences
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
    /// <summary>
    /// How to do sanitation of objects before creating the request
    /// </summary>
    public enum SanitationMode
    {
        /// <summary>
        /// Don't do any sanitation. If you use this, you should make sure that objects are sanitized
        /// some other way.
        /// </summary>
        None,
        /// <summary>
        /// Clean objects before requesting. This modifies the passed request.
        /// </summary>
        Clean,
        /// <summary>
        /// Remove any offending objects and report them in the result.
        /// </summary>
        Remove
    }
}