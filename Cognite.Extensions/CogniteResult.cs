using CogniteSdk;
using CogniteSdk.Resources;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.Extensions
{
    /// <summary>
    /// Class containing static methods to parse errors from the SDK and clean request objects
    /// </summary>
    public static partial class ResultHandlers
    {
        private static CogniteError<TError>? ParseCommonErrors<TError>(ResponseException ex)
        {
            // Handle any common behavior here
            if (ex.Code >= 500 || ex.Code != 400 && ex.Code != 422 && ex.Code != 409)
                return new CogniteError<TError> { Message = ex.Message, Status = ex.Code, Exception = ex };
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
        public static CogniteError<TError> ParseException<TError>(Exception ex, RequestType type)
        {
            if (ex == null)
            {
                throw new ArgumentNullException(nameof(ex));
            }
            if (ex is ResponseException rex)
            {
                var result = ParseCommonErrors<TError>(rex);
                if (result == null) result = new CogniteError<TError>
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
                else if (type == RequestType.UpdateAssets)
                {
                    ParseAssetUpdateException(rex, result);
                }
                else if (type == RequestType.CreateTimeSeries)
                {
                    ParseTimeSeriesException(rex, result);
                }
                else if (type == RequestType.UpdateTimeSeries)
                {
                    ParseTimeSeriesUpdateException(rex, result);
                }
                else if (type == RequestType.CreateEvents)
                {
                    ParseEventException(rex, result);
                }
                else if (type == RequestType.CreateSequences)
                {
                    ParseSequencesException(rex, result);
                }
                else if (type == RequestType.CreateSequenceRows)
                {
                    ParseSequenceRowException(rex, result);
                }
                else if (type == RequestType.CreateDatapoints)
                {
                    ParseDatapointsException(rex, result);
                }
                return result;
            }
            else
            {
                return new CogniteError<TError> { Message = ex.Message, Exception = ex };
            }
        }

        private static IEnumerable<T> CleanFromErrorCommon<T>(
            CogniteError<T> error,
            IEnumerable<T> items,
            Func<T, HashSet<Identity>, CogniteError<T>, bool> isAffected,
            Func<T, Identity?> getIdentity,
            Prometheus.Counter skippedCounter)
        {
            if (items == null) throw new ArgumentNullException(nameof(items));
            if (error == null) return items;

            if (!error.Values?.Any() ?? true)
            {
                error.Values = items.Select(item => getIdentity(item)!).Where(idt => idt != null).ToList();
                return Enumerable.Empty<T>();
            }

            var badValues = new HashSet<Identity>(error.Values);

            var ret = new List<T>();
            var skipped = new List<T>();

            foreach (var item in items)
            {
                if (isAffected(item, badValues, error))
                {
                    skipped.Add(item);
                    skippedCounter.Inc();
                }
                else
                {
                    ret.Add(item);
                }
            }

            if (skipped.Any())
            {
                error.Skipped = skipped;
            }
            else
            {
                error.Skipped = items;
                return Enumerable.Empty<T>();
            }
            return ret;
        }
    }


    /// <summary>
    /// Represents the result of one or more pushes to CDF.
    /// Contains a list of errors, one for each failed push, and potentially pre-push santiation.
    /// </summary>
    public class CogniteResult<TError>
    {
        /// <summary>
        /// Errors that have occured in this series of requests
        /// </summary>
        public IEnumerable<CogniteError<TError>>? Errors { get; set; }
        /// <summary>
        /// True if nothing went wrong
        /// </summary>
        public bool IsAllGood => !Errors?.Any() ?? true;
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="errors">Initial list of errors</param>
        public CogniteResult(IEnumerable<CogniteError<TError>>? errors)
        {
            Errors = errors;
        }

        /// <summary>
        /// Throw exception if there are any fatal errors
        /// </summary>
        public void ThrowOnFatal()
        {
            if (Errors == null || !Errors.Any()) return;
            var fatal = Errors.Where(err => err.Type == ErrorType.FatalFailure);
            if (fatal.Count() > 1)
            {
                throw new AggregateException(fatal.Select(err => new CogniteErrorException(err)).ToList());
            }
            else if (fatal.Count() == 1)
            {
                throw new CogniteErrorException(fatal.Single());
            }
        }

        /// <summary>
        /// Throw exception if there are any errors at all.
        /// </summary>
        public void Throw()
        {
            if (Errors == null || !Errors.Any()) return;
            if (Errors.Count() == 1)
            {
                throw new CogniteErrorException(Errors.Single());
            }
            else
            {
                throw new AggregateException(Errors.Select(err => new CogniteErrorException(err)).ToList());
            }
        }

        /// <summary>
        /// Combine all non-fatal errors with the same resource and type
        /// </summary>
        public void MergeErrors()
        {
            if (Errors == null || !Errors.Any()) return;
            var result = new List<CogniteError<TError>>();
            var groups = Errors.GroupBy(err => (err.Type, err.Resource));
            foreach (var group in groups)
            {
                if (group.Key.Type == ErrorType.FatalFailure)
                {
                    result.AddRange(group);
                    continue;
                }

                if (group.Count() == 1)
                {
                    result.Add(group.Single());
                }
                else
                {
                    result.Add(CogniteError<TError>.Merge(group));
                }
            }
            Errors = result;
        }

        /// <summary>
        /// Return a new CogniteResult that contains errors from both
        /// </summary>
        /// <param name="other">CogniteResult to merge with</param>
        /// <returns>A new result containing the CogniteErrors from both results</returns>
        public CogniteResult<TError> Merge(CogniteResult<TError>? other)
        {
            if (other == null) return this;
            IEnumerable<CogniteError<TError>>? errors;

            if (Errors == null) errors = other.Errors;
            else if (other.Errors == null) errors = Errors;
            else errors = Errors.Concat(other.Errors);

            var result = new CogniteResult<TError>(errors);
            result.MergeErrors();
            return result;
        }

        /// <summary>
        /// Return a new CogniteResult that contains errors from all <paramref name="results"/> given as parameter
        /// </summary>
        /// <param name="results">List of CogniteResult to merge with</param>
        /// <returns>A new result containing the CogniteErrors from all results given as parameter</returns>
        public static CogniteResult<TError> Merge(params CogniteResult<TError>[]? results)
        {
            if (results == null) return new CogniteResult<TError>(null);
            var errors = new List<CogniteError<TError>>();
            foreach (var result in results)
            {
                if (result == null) continue;
                if (result.Errors != null) errors.AddRange(result.Errors);
            }
            var res = new CogniteResult<TError>(errors);
            res.MergeErrors();
            return res;
        }

        /// <summary>
        /// Return a grouping of skipped error objects with a list of the errors that caused them to be skipped.
        /// Useful for reporting a list of individual errors per passed resource.
        /// </summary>
        /// <returns>Errors grouped by skipped objects</returns>
        public IEnumerable<(TError Skipped, IEnumerable<CogniteError<TError>> Errors)> ErrorsBySkipped()
        {
            if (Errors == null || !Errors.Any()) return Enumerable.Empty<(TError, IEnumerable<CogniteError<TError>>)>();

            return Errors
                .SelectMany(err => err.Skipped.Select(skipped => (skipped, err)))
                .GroupBy(pair => pair.skipped)
                .Select(group => (group.Key, group.Select(pair => pair.err)))
                .ToList();
        }

        /// <summary>
        /// Replace errors in this cognite result using <paramref name="replace"/>.
        /// Used when the return type of a method is different from type of internal method
        /// (like upsert).
        /// </summary>
        /// <typeparam name="TRep">New error type</typeparam>
        /// <param name="replace">Method to replace error type</param>
        /// <returns>Result with all errors replaced</returns>
        public CogniteResult<TRep> Replace<TRep>(Func<TError, TRep> replace)
        {
            return new CogniteResult<TRep>(Errors?.Select(e => e.ReplaceSkipped(replace)));
        }
    }

    /// <summary>
    /// Represents the result of one or more pushes to CDF.
    /// Contains a list of errors, one for each failed push, and potentially pre-push santiation,
    /// as well as a list of result objects.
    /// </summary>
    /// <typeparam name="TResult"></typeparam>
    /// <typeparam name="TError">Type of skipped data in error</typeparam>
    public class CogniteResult<TResult, TError> : CogniteResult<TError>
    {
        /// <summary>
        /// A list of results
        /// </summary>
        public IEnumerable<TResult>? Results { get; set; }
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="errors">Initial list of errors</param>
        /// <param name="results">Initial list of results</param>
        public CogniteResult(IEnumerable<CogniteError<TError>>? errors, IEnumerable<TResult>? results) : base(errors)
        {
            Results = results;
        }

        

        /// <summary>
        /// Return a new CogniteResult that contains errors and results from both
        /// </summary>
        /// <param name="other">CogniteResult to merge with</param>
        /// <returns>A new result containing errors and results from both</returns>
        public CogniteResult<TResult, TError> Merge(CogniteResult<TResult, TError>? other)
        {
            if (other == null) return this;
            IEnumerable<CogniteError<TError>>? errors;
            IEnumerable<TResult>? results;
            if (Results == null) results = other.Results;
            else if (other.Results == null) results = Results;
            else results = Results.Concat(other.Results);

            if (Errors == null) errors = other.Errors;
            else if (other.Errors == null) errors = Errors;
            else errors = Errors.Concat(other.Errors);

            var result = new CogniteResult<TResult, TError>(errors, results);
            result.MergeErrors();
            return result;
        }

        /// <summary>
        /// Return a new CogniteResult of type <typeparamref name="TResult"/> that contains errors from all <paramref name="results"/> given as parameter
        /// </summary>
        /// <param name="results">List of CogniteResult to merge with</param>
        /// <returns>A new result containing the CogniteErrors from all results given as parameter</returns>
        public static CogniteResult<TResult, TError> Merge(params CogniteResult<TResult, TError>[] results)
        {
            if (results == null) return new CogniteResult<TResult, TError>(null, null);
            var items = new List<TResult>();
            var errors = new List<CogniteError<TError>>();
            foreach (var result in results)
            {
                if (result == null) continue;
                if (result.Results != null) items.AddRange(result.Results);
                if (result.Errors != null) errors.AddRange(result.Errors);
            }

            var res = new CogniteResult<TResult, TError>(errors, items);
            res.MergeErrors();
            return res;
        }

        /// <summary>
        /// Replace errors in this cognite result using <paramref name="replace"/>.
        /// Used when the return type of a method is different from type of internal method
        /// (like upsert).
        /// </summary>
        /// <typeparam name="TRep">New error type</typeparam>
        /// <param name="replace">Method to replace error type</param>
        /// <returns>Result with all errors replaced</returns>
        public new CogniteResult<TResult, TRep> Replace<TRep>(Func<TError, TRep> replace)
        {
            return new CogniteResult<TResult, TRep>(Errors?.Select(e => e.ReplaceSkipped(replace)), Results);
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
        public IEnumerable<Identity>? Values { get; set; }
        /// <summary>
        /// Exception that caused this error, if any.
        /// </summary>
        public Exception? Exception { get; set; }
        /// <summary>
        /// Message describing this error.
        /// </summary>
        public string? Message { get; set; }
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
    /// Represents an error that occured on a push to CDF, or
    /// in pre-push sanitation.
    /// </summary>
    public class CogniteError<TError> : CogniteError
    {
        /// <summary>
        /// Input items skipped if the request was cleaned using this error.
        /// </summary>
        public IEnumerable<TError>? Skipped { get; set; }
        /// <summary>
        /// Merge a list of errors. Note that the other errors should have the same
        /// ResourceType and ErrorType as this one, for meaningful results.
        /// </summary>
        /// <param name="errs"></param>
        /// <returns>Merged error</returns>
        public static CogniteError<TError> Merge(IEnumerable<CogniteError<TError>> errs)
        {
            if (!errs.Any()) throw new InvalidOperationException("List of errors is empty");
            var initial = errs.First();

            var skipped = initial.Skipped?.ToList() ?? new List<TError>();
            var values = initial.Values?.ToList() ?? new List<Identity>();

            foreach (var err in errs.Skip(1))
            {
                if (err.Exception != null && initial.Exception == null)
                {
                    initial.Exception = err.Exception;
                    initial.Message = err.Message;
                }
                if (err.Skipped != null) skipped.AddRange(err.Skipped);
                if (err.Values != null) values.AddRange(err.Values);
            }

            initial.Skipped = skipped;
            initial.Values = values;

            return initial;
        }

        /// <summary>
        /// Return a new cognite error with error type replaced according to
        /// <paramref name="replace"/>. Everything else will be the same.
        /// </summary>
        /// <typeparam name="TRep">Type of new element</typeparam>
        /// <param name="replace">Method to replace old error type with new</param>
        /// <returns>New cognite error with same contents except for replaced members of Skipped</returns>
        public CogniteError<TRep> ReplaceSkipped<TRep>(Func<TError, TRep> replace)
        {
            return new CogniteError<TRep>
            {
                Complete = Complete,
                Exception = Exception,
                Message = Message,
                Resource = Resource,
                Skipped = Skipped?.Select(s => replace(s)),
                Status = Status,
                Type = Type,
                Values = Values
            };
        }
    }

    /// <summary>
    /// Exception triggered by a <see cref="CogniteError"/>
    /// </summary>
    public class CogniteErrorException : Exception
    {
        private static string GetErrorString(CogniteError err)
        {
            var builder = new StringBuilder();
            builder.AppendFormat("CogniteError. Resource: {0}, Type: {1}: {2}",
                err?.Resource, err?.Type, err?.Message);
            if (err?.Exception is ResponseException rex)
            {
                builder.AppendFormat(". RequestId: {0}", rex.RequestId);
            }
            return builder.ToString();
        }


        /// <summary>
        /// Constructor taking a <see cref="CogniteError"/>
        /// </summary>
        /// <param name="err"></param>
        public CogniteErrorException(CogniteError err)
            : this(GetErrorString(err), err?.Exception)
        {
        }
        /// <summary>
        /// Constructor
        /// </summary>
        public CogniteErrorException()
        {
        }

        /// <summary>
        /// Constructor taking a message
        /// </summary>
        /// <param name="message">String message</param>
        public CogniteErrorException(string message) : base(message)
        {
        }

        /// <summary>
        /// Constructor taking a message and inner exception
        /// </summary>
        /// <param name="message">String message</param>
        /// <param name="innerException">Inner exception</param>
        public CogniteErrorException(string message, Exception? innerException) : base(message, innerException)
        {
        }
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
        /// Field type did not match
        /// </summary>
        MismatchedType,
        /// <summary>
        /// Item does not satisfy CDF field limits
        /// </summary>
        SanitationFailed,
        /// <summary>
        /// Item value is illegal for a different reason
        /// </summary>
        IllegalItem,
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
        /// Collection of rows when creating in sequence
        /// </summary>
        SequenceRows,
        /// <summary>
        /// Row in a sequence
        /// </summary>
        SequenceRow,
        /// <summary>
        /// Values of a sequence row
        /// </summary>
        SequenceRowValues,
        /// <summary>
        /// Row number of a sequence row
        /// </summary>
        SequenceRowNumber,
        /// <summary>
        /// Value of a datapoint
        /// </summary>
        DataPointValue,
        /// <summary>
        /// Timestamp of a datapoint
        /// </summary>
        DataPointTimestamp,
        /// <summary>
        /// The update object itself in some way
        /// </summary>
        Update,
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
        CreateSequences,
        /// <summary>
        /// Create sequence rows
        /// </summary>
        CreateSequenceRows,
        /// <summary>
        /// Create timeseries datapoints
        /// </summary>
        CreateDatapoints,
        /// <summary>
        /// Update assets
        /// </summary>
        UpdateAssets,
        /// <summary>
        /// Update timeseries
        /// </summary>
        UpdateTimeSeries
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
