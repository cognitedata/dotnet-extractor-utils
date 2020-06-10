using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Threading;
using CogniteSdk;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Polly;
using Polly.Extensions.Http;
using Polly.Timeout;
using Cognite.Extractor.Common;
using System.Linq;

namespace Cognite.Extractor.Utils
{
    /// <summary>
    /// Utility class for configuring a <see href="https://github.com/cognitedata/cognite-sdk-dotnet">Cognite SDK</see> client
    /// </summary>
    public static class CogniteUtils
    {
        /// <summary>
        /// Cognite min double value
        /// </summary>
        public const double NumericValueMin = -1e+100;
        
        /// <summary>
        /// Cognite max double value
        /// </summary>
        public const double NumericValueMax = 1e+100;
        
        /// <summary>
        /// Cognite max string length
        /// </summary>
        public const int StringLengthMax = 255;

        /// <summary>
        /// Cognite min timestamp (1971)
        /// </summary>
        public const long TimestampMin = 31536000000L;

        /// <summary>
        /// Cognite max timestamp (2050)
        /// </summary>
        public const long TimestampMax = 2556144000000L;

        /// <summary>
        /// Configure a <see cref="Client.Builder"/> according to the <paramref name="config"/> object
        /// </summary>
        /// <param name="clientBuilder">This builder</param>
        /// <param name="config">A <see cref="CogniteConfig"/> configuration object</param>
        /// <param name="appId">Identifier of the application using the Cognite API</param>
        /// <param name="auth">A <see cref="Authenticator"/> authenticator used to obtain bearer access token. 
        /// If null, API keys are used for authentication</param>
        /// <param name="logger">A <see cref="ILogger"/> logger that the client can use to log calls to the 
        /// Cognite API (enabled in debug mode)</param>
        /// <param name="metrics">A <see cref="IMetrics"/> metrics collector, that the client can use
        /// to report metrics on the number and duration of API requests</param>
        /// <returns>A configured builder</returns>
        /// <exception cref="CogniteUtilsException">Thrown when <paramref name="config"/> is null or 
        /// the configured project is empty</exception>
        public static Client.Builder Configure(
            this Client.Builder clientBuilder, 
            CogniteConfig config,
            string appId,
            Authenticator auth = null, 
            ILogger<Client> logger = null,
            IMetrics metrics = null)
        {
            var builder = clientBuilder
                .SetAppId(appId);

            if (config?.Project?.TrimToNull() != null)
            {
                builder = builder.SetProject(config?.Project);
            }
            
            if (config?.Host?.TrimToNull() != null)
                builder = builder.SetBaseUrl(new Uri(config.Host));

            if (config?.ApiKey?.TrimToNull() != null)
            {
                builder = builder
                    .SetApiKey(config.ApiKey);
            }
            else if (auth != null)
            {
                builder = builder.SetTokenProvider(token => auth.GetToken(token));
            }

            if (config?.SdkLogging != null && !config.SdkLogging.Disable && logger != null) {
                builder = builder
                    .SetLogLevel(config.SdkLogging.Level)
                    .SetLogFormat(config.SdkLogging.Format)
                    .SetLogger(logger);
            }

            if (metrics != null) {
                builder = builder
                    .SetMetrics(metrics);
            }

            return builder;
        }

        /// <summary>
        /// Trim values to accepted CDF limits and filter out invalid double values (NaN and Infinity) 
        /// </summary>
        /// <param name="points">Data points</param>
        /// <returns></returns>
        public static IEnumerable<Datapoint> TrimValues(this IEnumerable<Datapoint> points)
        {
            foreach (var point in points)
            {
                // reduce GC pressure by re-using object if ok
                if (point.StringValue != null)
                {
                    yield return point.StringValue.Length < StringLengthMax ? point :
                        new Datapoint(CogniteTime.FromUnixTimeMilliseconds(point.Timestamp), point.StringValue.Substring(0, StringLengthMax));
                }
                else if (point.NumericValue.HasValue)
                {
                    double value = point.NumericValue.Value;
                    if (!double.IsNaN(value) && !double.IsInfinity(value))
                    {
                        value = Math.Max(NumericValueMin, value);
                        value = Math.Min(NumericValueMax, value);
                        yield return value == point.NumericValue.Value ? point : 
                            new Datapoint(CogniteTime.FromUnixTimeMilliseconds(point.Timestamp), value);
                    }
                }
            }
        }

        /// <summary>
        /// Remove data points from <paramref name="points"/> that contain timestamps outside the
        /// supported range in Cognite: from 1971 to 2050.
        /// </summary>
        /// <param name="points"></param>
        /// <returns></returns>
        public static IEnumerable<Datapoint> RemoveOutOfRangeTimestamps(this IEnumerable<Datapoint> points)
        {
            return points.Where(p => p.Timestamp >= TimestampMin && p.Timestamp <= TimestampMax);
        }
    }

    /// <summary>
    /// Exceptions produced by the Cognite utility classes
    /// </summary>
    public class CogniteUtilsException : Exception
    {
        /// <summary>
        /// Create a new Cognite utils exception with the given <paramref name="message"/>
        /// </summary>
        /// <param name="message">Message</param>
        /// <returns>A new <see cref="CogniteUtilsException"/> exception</returns>
        public CogniteUtilsException(string message) : base(message)
        {
        }

        /// <summary>
        /// Constructor
        /// </summary>
        public CogniteUtilsException()
        {
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="message"></param>
        /// <param name="innerException"></param>
        public CogniteUtilsException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }

    /// <summary>
    /// Comparer for <see cref="Identity"/> objects
    /// </summary>
    public class IdentityComparer : IEqualityComparer<Identity>
    {
        /// <summary>
        /// Determine if two <see cref="Identity"/> objects are equal:
        /// They have the same Id or ExternalId
        /// </summary>
        /// <param name="x">Identity</param>
        /// <param name="y">Identity</param>
        /// <returns></returns>
        public bool Equals(Identity x, Identity y)
        {
            if (Object.ReferenceEquals(x, y)) return true;

            if (Object.ReferenceEquals(x, null) || Object.ReferenceEquals(y, null)) return false;

            if (x.Id.HasValue && y.Id.HasValue && x.Id.Value == y.Id.Value)
            {
                return true;
            }
            if (x.ExternalId.TrimToNull() != null && y.ExternalId.TrimToNull() != null && x.ExternalId == y.ExternalId)
            {
                return true;
            }
            return false;
        }

        /// <summary>
        /// Get a hash code based on the identity Id or ExternalId
        /// </summary>
        /// <param name="obj">Identity</param>
        /// <returns></returns>
        public int GetHashCode(Identity obj)
        {
            if (Object.ReferenceEquals(obj, null)) return 0;

            if (obj.Id.HasValue)
            {
                return obj.Id.Value.GetHashCode();
            }
            if (obj.ExternalId.TrimToNull() == null)
            {
                return 0;
            }
            return obj.ExternalId.GetHashCode();
        }
    }

    /// <summary>
    /// Extension utilities for the Cognite client
    /// </summary>
    public static class CogniteExtensions
    {
        static IAsyncPolicy<HttpResponseMessage> GetRetryPolicy(ILogger logger)
        {
            int numRetries = 5;
            return Policy
                .HandleResult<HttpResponseMessage>(msg => 
                    msg.StatusCode == HttpStatusCode.Unauthorized
                    || (int)msg.StatusCode == 429) //  HttpStatusCode.TooManyRequests not in .Net Framework, is in .Net Core 3.0
                .OrTransientHttpError()
                .Or<TimeoutRejectedException>()
                .WaitAndRetryAsync(
                    // retry interval 0.125, 0.25, 0.5, 1, 2, ..., i.e. max 0.125 * 2^numRetries
                    numRetries,
                    retry => TimeSpan.FromMilliseconds(125 * Math.Pow(2, Math.Min(retry - 1, numRetries))),
                    (ex, ts) =>
                    {
                        if (ex.Result != null)
                        {
                            logger.LogDebug("Failed request with status code: {Code}. Retrying in {Time} ms. {Message}",
                                (int) ex.Result.StatusCode, ts.TotalMilliseconds, ex.Result.ReasonPhrase);
                            logger.LogDebug("Failed request: {Method} {Uri}",
                                ex.Result.RequestMessage.Method,
                                ex.Result.RequestMessage.RequestUri);
                        }
                        else if (ex.Exception != null)
                        {
                            logger.LogDebug("Request timed out or failed with message: {Message} Retrying in {Time} ms.",
                                ex.Exception.Message, ts.TotalMilliseconds);
                            var inner = ex.Exception.InnerException;
                            while (inner != null)
                            {
                                logger.LogDebug("Inner exception: {Message}", inner.Message);
                                inner = inner.InnerException;
                            }
                        }
                    });
        }

        static IAsyncPolicy<HttpResponseMessage> GetTimeoutPolicy()
        {
            return Policy.TimeoutAsync<HttpResponseMessage>(TimeSpan.FromSeconds(80)); // timeout for each individual try
        }

        /// <summary>
        /// Adds a configured Cognite client to the <paramref name="services"/> collection as a transient service
        /// </summary>
        /// <param name="services">The service collection</param>
        /// <param name="appId">Identifier of the application using the Cognite API</param>
        /// <param name="setLogger">If true, a <see cref="ILogger"/> logger is created and used by the client log calls to the 
        /// Cognite API (enabled in debug mode)</param>
        /// <param name="setMetrics">If true, a <see cref="IMetrics"/> metrics collector is created and used by the client
        /// to report metrics on the number and duration of API requests</param>
        /// <param name="setHttpClient">Default true. If false <see cref="Client.Builder"/> is not added to the
        /// <see cref="ServiceCollection"/>. If this is false it must be added before this method is called.</param>
        public static void AddCogniteClient(this IServiceCollection services,
                                            string appId,
                                            bool setLogger = false,
                                            bool setMetrics = false,
                                            bool setHttpClient = true)
        {
            if (setHttpClient)
            {
                services.AddHttpClient<Client.Builder>(c => c.Timeout = Timeout.InfiniteTimeSpan)
                    .AddPolicyHandler((provider, message) => { return GetRetryPolicy(provider.GetRequiredService<ILogger<Client>>()); })
                    .AddPolicyHandler(GetTimeoutPolicy());
            }

            services.AddHttpClient<Authenticator>();
            services.AddSingleton<IMetrics, CdfMetricCollector>();
            services.AddTransient(provider => {
                var cdfBuilder = provider.GetRequiredService<Client.Builder>();
                var conf = provider.GetService<CogniteConfig>();
                var auth = conf?.IdpAuthentication != null ? 
                    provider.GetRequiredService<Authenticator>() : null;
                var logger = setLogger ? 
                    provider.GetRequiredService<ILogger<Client>>() : null;
                var metrics = setMetrics ?
                    provider.GetRequiredService<IMetrics>() : null;
                var client = cdfBuilder.Configure(conf, appId, auth, logger, metrics).Build();
                return client;
            });
            services.AddTransient<CogniteDestination>();
        }
    }

    /// <summary>
    /// Data point abstraction. Consists of a timestamp and a double or string value
    /// </summary>
    public class Datapoint
    {
        private readonly long _timestamp;
        private readonly double? _numericValue;
        private readonly string _stringValue;
        
        /// <summary>
        /// Timestamp in Unix time milliseconds
        /// </summary>
        public long Timestamp => _timestamp;

        /// <summary>
        /// Optional string value
        /// </summary>
        public string StringValue => _stringValue;

        /// <summary>
        /// Optional double value
        /// </summary>
        public double? NumericValue => _numericValue;

        /// <summary>
        /// Creates a numeric data point
        /// </summary>
        /// <param name="timestamp">Timestamp</param>
        /// <param name="numericValue">double value</param>
        public Datapoint(DateTime timestamp, double numericValue)
        {
            _timestamp = timestamp.ToUnixTimeMilliseconds();
            _numericValue = numericValue;
            _stringValue = null;
        }

        /// <summary>
        /// Creates a string data point
        /// </summary>
        /// <param name="timestamp">Timestamp</param>
        /// <param name="stringValue">string value</param>
        public Datapoint(DateTime timestamp, string stringValue)
        {
            _timestamp = timestamp.ToUnixTimeMilliseconds();
            _numericValue = null;
            _stringValue = stringValue;
        }
    }
}