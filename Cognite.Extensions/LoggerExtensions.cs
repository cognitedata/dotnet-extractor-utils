using CogniteSdk;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Cognite.Extensions
{
    /// <summary>
    /// Extensions class containing methods to log the results of extension methods
    /// </summary>
    public static class LoggerExtensions
    {
        /// <summary>
        /// Log a description of the error, with how the values that caused it
        /// as well as the number of objects that were affected by it.
        /// </summary>
        /// <param name="logger">Logger to log the error to</param>
        /// <param name="error">Error to log</param>
        /// <param name="requestType">Request that caused the error</param>
        /// <param name="ignoreExisting">True to not log errors caused by items already present in CDF</param>
        /// <param name="handledLevel">Log level of errors that were handled by the utils</param>
        /// <param name="fatalLevel">Log level of errors that could not be handled and caused the request to fail</param>
        public static void LogCogniteError<TError>(this ILogger logger,
            CogniteError<TError> error,
            RequestType requestType,
            bool ignoreExisting,
            LogLevel handledLevel = LogLevel.Debug,
            LogLevel fatalLevel = LogLevel.Error)
        {
            if (error == null)
            {
                throw new ArgumentNullException(nameof(error));
            }
            string? cogniteString = null;
            if (error.Exception != null && error.Exception is ResponseException rex)
            {
                cogniteString = $" RequestId: {rex.RequestId}, CDF Message: {rex.Message}";
            }
            else if (error.Exception != null)
            {
                cogniteString = $" Non-CDF Error of type: {error.Exception.GetType()}";
                if (!string.IsNullOrWhiteSpace(error.Exception.Message))
                {
                    cogniteString += $", Message: {error.Exception.Message}";
                }
            }

            string? valueString = null;
            if (error.Values != null && error.Values.Any())
            {
                valueString = string.Join(", ", error.Values.Select(idt => idt.ExternalId ?? idt.Id.ToString()));
            }
            string resourceName;
            switch (requestType)
            {
                case RequestType.CreateAssets:
                    resourceName = "assets";
                    break;
                case RequestType.CreateEvents:
                    resourceName = "events";
                    break;
                case RequestType.CreateTimeSeries:
                    resourceName = "timeseries";
                    break;
                case RequestType.CreateDatapoints:
                    resourceName = "datapoint timeseries";
                    break;
                case RequestType.CreateSequences:
                    resourceName = "sequences";
                    break;
                case RequestType.CreateSequenceRows:
                    resourceName = "sequence row sequences";
                    break;
                default:
                    resourceName = "unknown";
                    break;
            }
            switch (error.Type)
            {
                case ErrorType.FatalFailure:
                    logger.Log(fatalLevel, "Fatal error in request of type {type}: {msg}. {cdf}",
                        requestType, error.Message, cogniteString);
                    break;
                case ErrorType.ItemDuplicated:
                    logger.Log(handledLevel, "The following {resource}s were duplicated in the request: {values}, " +
                        "resulting in the full or partial removal of {cnt} {name} from the request.{cdf}",
                        error.Resource, valueString, error.Skipped?.Count() ?? 0, resourceName, cogniteString);
                    break;
                case ErrorType.ItemExists:
                    if (ignoreExisting) return;
                    logger.Log(handledLevel, "The following {resource}s already existed in CDF: {values}, " +
                        "resulting in the removal of {cnt} {name} from the request.{cdf}",
                        error.Resource, valueString, error.Skipped?.Count() ?? 0, resourceName, cogniteString);
                    break;
                case ErrorType.ItemMissing:
                    logger.Log(handledLevel, "The following {resource}s were missing in CDF: {values}, " +
                        "resulting in the removal of {cnt} {name} from the request.{cdf}",
                        error.Resource, valueString, error.Skipped?.Count() ?? 0, resourceName, cogniteString);
                    break;
                case ErrorType.MismatchedType:
                    logger.Log(handledLevel, "Values of {resource} were of mismatched type in " +
                        "{cnt} {name}, resulting in their full or partial removal from the request.{cdf}",
                        error.Resource, error.Skipped?.Count() ?? 0, resourceName, cogniteString);
                    break;
                case ErrorType.SanitationFailed:
                    logger.Log(handledLevel, "Sanitation of {resource} with values: {values} failed, " +
                        "resulting in the full or partial removal of {cnt} {name} from the request.{cdf}",
                        error.Resource, valueString, error.Skipped?.Count() ?? 0, resourceName, cogniteString);
                    break;
            }
        }

        /// <summary>
        /// Log the CogniteResult object and all its errors.
        /// </summary>
        /// <typeparam name="TResult">Type of result</typeparam>
        /// <typeparam name="TError">Type of reported error</typeparam>
        /// <param name="logger">Logger to write to</param>
        /// <param name="result">Result to log</param>
        /// <param name="requestType">Request type</param>
        /// <param name="ignoreExisting">True to not log errors caused by items already present in CDF</param>
        /// <param name="infoLevel">Level for summary information about the request</param>
        /// <param name="handledErrorLevel">Log level of errors that were handled by the utils</param>
        /// <param name="fatalLevel">Log level of errors that could not be handled and caused the request to fail</param>
        public static void LogResult<TResult, TError>(this ILogger logger,
            CogniteResult<TResult, TError> result,
            RequestType requestType,
            bool ignoreExisting,
            LogLevel infoLevel = LogLevel.Information,
            LogLevel handledErrorLevel = LogLevel.Debug,
            LogLevel fatalLevel = LogLevel.Error)
        {
            if (result == null) throw new ArgumentNullException(nameof(result));

            int successCount = result.Results?.Count() ?? 0;
            int errorCount = result.Errors?.Count() ?? 0;

            if (successCount > 0 || errorCount > 0)
            {
                logger.Log(infoLevel, "Request of type {type} had {cnt} results with {cnt2} errors",
                    requestType, successCount, errorCount);
            }
            

            if (result.Errors != null)
            {
                foreach (var err in result.Errors)
                {
                    logger.LogCogniteError(err, requestType, ignoreExisting, handledErrorLevel, fatalLevel);
                }
            }
        }

        /// <summary>
        /// Log the CogniteResult object and all its errors.
        /// </summary>
        /// <typeparam name="TError">Type of reported error</typeparam>
        /// <param name="logger">Logger to write to</param>
        /// <param name="result">Result to log</param>
        /// <param name="requestType">Request type</param>
        /// <param name="ignoreExisting">True to not log errors caused by items already present in CDF</param>
        /// <param name="infoLevel">Level for summary information about the request</param>
        /// <param name="handledErrorLevel">Log level of errors that were handled by the utils</param>
        /// <param name="fatalLevel">Log level of errors that could not be handled and caused the request to fail</param>
        public static void LogResult<TError>(this ILogger logger,
            CogniteResult<TError> result,
            RequestType requestType,
            bool ignoreExisting,
            LogLevel infoLevel = LogLevel.Information,
            LogLevel handledErrorLevel = LogLevel.Debug,
            LogLevel fatalLevel = LogLevel.Error)
        {
            if (result == null) throw new ArgumentNullException(nameof(result));

            int errorCount = result.Errors?.Count() ?? 0;

            if (errorCount > 0)
            {
                logger.Log(infoLevel, "Request of type {type} had {cnt} errors", requestType, errorCount);
            }

            if (result.Errors != null)
            {
                foreach (var err in result.Errors)
                {
                    logger.LogCogniteError(err, requestType, ignoreExisting, handledErrorLevel, fatalLevel);
                }
            }
        }
    }
}
