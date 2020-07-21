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
using Cognite.Common;
using System.IO;
using System.Threading.Tasks;

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
        /// Configure a CogntieSdk Client.Builder according to the <paramref name="config"/> object
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
        /// Write missing identities to the provided identity set.
        /// </summary>
        /// <param name="missing">Set to add missing ids to</param>
        /// <param name="e">Error containing missing ids</param>
        public static void ExtractMissingFromResponseException(HashSet<Identity> missing, ResponseException e)
        {
            foreach (var ts in e.Missing)
            {
                if (ts.TryGetValue("externalId", out MultiValue exIdValue))
                {
                    missing.Add(new Identity(exIdValue.ToString()));
                }
                else if (ts.TryGetValue("id", out MultiValue idValue))
                {
                    missing.Add(new Identity(((MultiValue.Long)idValue).Value));
                }
            }
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
        /// <summary>
        /// Turn string into an array of bytes on the format [unsigned short length][string].
        /// </summary>
        /// <param name="str">String to transform</param>
        /// <returns>Storable bytes</returns>
        public static byte[] StringToStorable(string str)
        {
            if (str == null)
            {
                return BitConverter.GetBytes((ushort)0);
            }
            var strBytes = System.Text.Encoding.UTF8.GetBytes(str);
            ushort size = (ushort)strBytes.Length;
            byte[] bytes = new byte[size + sizeof(ushort)];
            Buffer.BlockCopy(BitConverter.GetBytes(size), 0, bytes, 0, sizeof(ushort));
            if (size == 0) return bytes;
            Buffer.BlockCopy(strBytes, 0, bytes, sizeof(ushort), size);
            return bytes;
        }
        /// <summary>
        /// Read a string from given array of bytes, starting from given position.
        /// String being read is assumed to be on the format [unsigned short length][string]
        /// </summary>
        /// <param name="stream">Stream to read from</param>
        /// <param name="size">Optional size, if this is non-null, skip reading size from the stream</param>
        /// <returns>Resulting parsed string</returns>
        public static string StringFromStream(Stream stream, ushort? size = null)
        {
            if (!size.HasValue)
            {
                var sizeBytes = new byte[sizeof(ushort)];
                if (stream.Read(sizeBytes, 0, sizeof(ushort)) < sizeof(ushort)) return null;
                size = BitConverter.ToUInt16(sizeBytes, 0);
                if (size == 0) return null;
            }

            var bytes = new byte[size.Value];
            if (stream.Read(bytes, 0, size.Value) < size.Value) return null;

            return System.Text.Encoding.UTF8.GetString(bytes);
        }

        /// <summary>
        /// Write dictionary of datapoints to a stream.
        /// Encoding for one timeseries:
        /// [ushort id-length, 0 if internalId]{Either [long internalId] or [ushort size][string]}[uint count][dp 1]...[dp count]
        /// </summary>
        /// <param name="datapoints">Datapoints to store</param>
        /// <param name="stream">Stream to write to</param>
        /// <param name="token"></param>
        public static Task WriteDatapointsAsync(IDictionary<Identity, IEnumerable<Datapoint>> datapoints, Stream stream, CancellationToken token)
        {
            return Task.Run(() => WriteDatapoints(datapoints, stream), token);
        }

        /// <summary>
        /// Write dictionary of datapoints to a stream.
        /// Encoding for one timeseries:
        /// [ushort id-length, 0 if internalId]{Either [long internalId] or [ushort size][string]}[uint count][dp 1]...[dp count]
        /// </summary>
        /// <param name="datapoints">Datapoints to store</param>
        /// <param name="stream">Stream to write to</param>
        public static void WriteDatapoints(IDictionary<Identity, IEnumerable<Datapoint>> datapoints, Stream stream)
        {
            foreach (var kvp in datapoints)
            {
                var id = kvp.Key;
                var dps = kvp.Value;
                if (!dps.Any()) continue;
                byte[] idBytes;

                if (id.Id != null)
                {
                    idBytes = new byte[sizeof(ushort) + sizeof(long)];
                    idBytes[0] = idBytes[1] = 0;
                    Buffer.BlockCopy(BitConverter.GetBytes(id.Id.Value), 0, idBytes, sizeof(ushort), sizeof(long));
                }
                else
                {
                    idBytes = StringToStorable(id.ExternalId);
                }
                stream.Write(idBytes, 0, idBytes.Length);

                var count = dps.Count();
                stream.Write(BitConverter.GetBytes(count), 0, sizeof(int));

                foreach (var dp in dps)
                {
                    var bytes = dp.ToStorableBytes();
                    stream.Write(bytes, 0, bytes.Length);
                }
            }
        }

        /// <summary>
        /// Transforms binary encoded datapoints in a stream into a dictionary of datapoints.
        /// Encoding for one timeseries, ids may repeat:
        /// [ushort id-length, 0 if internalId]{Either [long internalId] or [ushort size][string]}[uint count][dp 1]...[dp count]
        /// </summary>
        /// <param name="stream">Stream to read from</param>
        /// <param name="token"></param>'
        /// <param name="chunkSize">Max number of points to read before returning, reuse the stream to continue reading.
        /// 0 means that there is no upper limit. If there are timeseries-chunks larger than the chunk size in the stream,
        /// it may be exceeded</param>
        /// <returns>Read datapoints grouped by identity</returns>
        public static Task<IDictionary<Identity, IEnumerable<Datapoint>>> ReadDatapointsAsync(Stream stream, CancellationToken token, int chunkSize = 0)
        {
            return Task.Run(() => ReadDatapoints(stream, chunkSize), token);
        }

        /// <summary>
        /// Transforms binary encoded datapoints in a stream into a dictionary of datapoints.
        /// Encoding for one timeseries, ids may repeat:
        /// [ushort id-length, 0 if internalId]{Either [long internalId] or [ushort size][string]}[uint count][dp 1]...[dp count]
        /// </summary>
        /// <param name="stream">Stream to read from</param>
        /// <param name="chunkSize">Max number of points to read before returning, reuse the stream to continue reading.
        /// 0 means that there is no upper limit. If there are timeseries-chunks larger than the chunk size in the stream,
        /// it may be exceeded.</param>
        /// <returns>Read datapoints grouped by identity</returns>
        public static IDictionary<Identity, IEnumerable<Datapoint>> ReadDatapoints(Stream stream, int chunkSize = 0)
        {
            var ret = new Dictionary<Identity, List<Datapoint>>(new IdentityComparer());

            var idSizeBuffer = new byte[sizeof(ushort)];

            int total = 0;

            while (true)
            {
                if (stream.Read(idSizeBuffer, 0, sizeof(ushort)) < sizeof(ushort)) break;
                ushort size = BitConverter.ToUInt16(idSizeBuffer, 0);

                Identity id;
                // A 0-length ID implies that the datapoint uses internalId
                if (size == 0)
                {
                    var idBuffer = new byte[sizeof(long)];
                    if (stream.Read(idBuffer, 0, sizeof(long)) < sizeof(long)) break;
                    id = Identity.Create(BitConverter.ToInt64(idBuffer, 0));
                }
                else
                {
                    string extId = StringFromStream(stream, size);
                    if (extId == null) break;
                    id = Identity.Create(extId);
                }

                var countBuffer = new byte[sizeof(uint)];
                if (stream.Read(countBuffer, 0, sizeof(uint)) < sizeof(uint)) break;
                uint count = BitConverter.ToUInt32(countBuffer, 0);
                if (count == 0) continue;

                if (chunkSize > 0 && count + total > chunkSize && total > 0) break;

                var dps = new List<Datapoint>();

                for (int i = 0; i < count; i++)
                {
                    var dp = Datapoint.FromStream(stream);
                    if (dp == null) break;

                    total++;
                    dps.Add(dp);
                }
                
                if (!ret.TryGetValue(id, out var datapoints))
                {
                    ret[id] = dps;
                }
                else
                {
                    datapoints.AddRange(dps);
                }
            }

            return ret.ToDictionary(kvp => kvp.Key, kvp => (IEnumerable<Datapoint>)kvp.Value, new IdentityComparer());
        }

        /// <summary>
        /// Transforms a single event into an array of bytes.
        /// Encoding:
        /// [Id][Start][End][Description][Type][SubType][Source][assetIdCount][dataSetId]{[assetId1]...}[MetaCount]{[key][value]...}
        /// </summary>
        /// <param name="evt">Event to write</param>
        /// <returns>Event serialized as bytes</returns>
        public static byte[] EventToStorable(EventCreate evt)
        {
            var bytes = new List<byte>();
            bytes.AddRange(StringToStorable(evt.ExternalId));
            bytes.AddRange(BitConverter.GetBytes(evt.StartTime ?? -1));
            bytes.AddRange(BitConverter.GetBytes(evt.EndTime ?? -1));
            bytes.AddRange(StringToStorable(evt.Description));
            bytes.AddRange(StringToStorable(evt.Type));
            bytes.AddRange(StringToStorable(evt.Subtype));
            bytes.AddRange(StringToStorable(evt.Source));
            bytes.AddRange(BitConverter.GetBytes(evt.DataSetId ?? 0));

            if (evt.AssetIds != null)
            {
                var assetIdBytes = new List<byte>();
                foreach (var id in evt.AssetIds)
                {
                    assetIdBytes.AddRange(BitConverter.GetBytes(id));
                }
                bytes.AddRange(BitConverter.GetBytes((ushort)evt.AssetIds.Count()));
                bytes.AddRange(assetIdBytes);
            }
            else
            {
                bytes.AddRange(new byte[] { 0, 0 });
            }


            if (evt.Metadata != null)
            {
                var metaDataBytes = new List<byte>();
                foreach (var kvp in evt.Metadata)
                {
                    metaDataBytes.AddRange(StringToStorable(kvp.Key));
                    metaDataBytes.AddRange(StringToStorable(kvp.Value));
                }
                bytes.AddRange(BitConverter.GetBytes((ushort)evt.Metadata.Count));
                bytes.AddRange(metaDataBytes);
            }
            else
            {
                bytes.AddRange(new byte[] { 0, 0 });
            }


            return bytes.ToArray();
        }
        /// <summary>
        /// Reads a single event from a stream.
        /// Encoding:
        /// [Id][Start][End][Description][Type][SubType][Source][assetIdCount][dataSetId]{[assetId1]...}[MetaCount]{[key][value]...}
        /// </summary>
        /// <param name="stream">Stream to read from</param>
        /// <returns></returns>
        public static EventCreate EventFromStream(Stream stream)
        {
            var evt = new EventCreate();
            evt.ExternalId = StringFromStream(stream);

            var timeBytes = new byte[sizeof(long) * 2];
            if (stream.Read(timeBytes, 0, sizeof(long) * 2) < sizeof(long) * 2) return null;
            var startTime = BitConverter.ToInt64(timeBytes, 0);
            var endTime = BitConverter.ToInt64(timeBytes, sizeof(long));
            if (startTime >= 0) evt.StartTime = startTime;
            if (endTime >= 0) evt.EndTime = endTime;

            evt.Description = StringFromStream(stream);
            evt.Type = StringFromStream(stream);
            evt.Subtype = StringFromStream(stream);
            evt.Source = StringFromStream(stream);

            var dataSetBytes = new byte[sizeof(long)];
            if (stream.Read(dataSetBytes, 0, sizeof(long)) < sizeof(long)) return null;
            long dataSetId = BitConverter.ToInt64(dataSetBytes, 0);
            if (dataSetId > 0) evt.DataSetId = dataSetId;

            var countBytes = new byte[sizeof(ushort)];
            if (stream.Read(countBytes, 0, sizeof(ushort)) < sizeof(ushort)) return null;
            ushort assetIdCount = BitConverter.ToUInt16(countBytes, 0);

            if (assetIdCount > 0)
            {
                var assetIds = new List<long>();
                var idBytes = new byte[sizeof(long)];
                for (int i = 0; i < assetIdCount; i++)
                {
                    if (stream.Read(idBytes, 0, sizeof(long)) < sizeof(long)) return null;
                    long id = BitConverter.ToInt64(idBytes, 0);
                    assetIds.Add(id);
                }
                evt.AssetIds = assetIds;
            }


            if (stream.Read(countBytes, 0, sizeof(ushort)) < sizeof(ushort)) return null;
            ushort metaDataCount = BitConverter.ToUInt16(countBytes, 0);

            if (metaDataCount > 0)
            {
                evt.Metadata = new Dictionary<string, string>();
                for (int i = 0; i < metaDataCount; i++)
                {
                    var key = StringFromStream(stream);
                    var value = StringFromStream(stream);
                    if (key == null) return null;
                    evt.Metadata[key] = value;
                }
            }


            return evt;
        }

        /// <summary>
        /// Read events from a stream.
        /// Encoding:
        /// [Id][Start][End][Description][Type][SubType][Source][assetIdCount][dataSetId]{[assetId1]...}[MetaCount]{[key][value]...}
        /// </summary>
        /// <param name="stream">Stream to read from</param>
        /// <param name="chunkSize">Maximum number of events to read at a time, reuse the stream to continue reading.
        /// If the list is empty, end of file is reached or the file is corrupt</param>
        /// <returns>A list of created events</returns>
        public static IEnumerable<EventCreate> ReadEvents(Stream stream, int chunkSize = 0)
        {
            var events = new List<EventCreate>();
            int total = 0;
            while (true)
            {
                var evt = EventFromStream(stream);
                if (evt == null) break;
                events.Add(evt);
                if (chunkSize > 0 && ++total >= chunkSize) break;
            }
            return events;
        }

        /// <summary>
        /// Read events from a stream.
        /// Encoding:
        /// [Id][Start][End][Description][Type][SubType][Source][assetIdCount][dataSetId]{[assetId1]...}[MetaCount]{[key][value]...}
        /// </summary>
        /// <param name="stream">Stream to read from</param>
        /// <param name="token"></param>
        /// <param name="chunkSize">Maximum number of events to read at a time, reuse the stream to continue reading.
        /// If the list is empty, end of file is reached or the file is corrupt</param>
        /// <returns>A list of created events</returns>
        public static Task<IEnumerable<EventCreate>> ReadEventsAsync(Stream stream, CancellationToken token, int chunkSize = 0)
        {
            return Task.Run(() => ReadEvents(stream, chunkSize), token);
        }

        /// <summary>
        /// Write events to a stream.
        /// Encoding:
        /// [Id][Start][End][Description][Type][SubType][Source][assetIdCount][dataSetId]{[assetId1]...}[MetaCount]{[key][value]...}
        /// </summary>
        /// <param name="events">Events to write</param>
        /// <param name="stream">Stream to write to</param>
        public static void WriteEvents(IEnumerable<EventCreate> events, Stream stream)
        {
            foreach (var evt in events)
            {
                var bytes = EventToStorable(evt);
                stream.Write(bytes, 0, bytes.Length);
            }
        }

        /// <summary>
        /// Write events to a stream.
        /// Encoding:
        /// [Id][Start][End][Description][Type][SubType][Source][assetIdCount][dataSetId]{[assetId1]...}[MetaCount]{[key][value]...}
        /// </summary>
        /// <param name="events">Events to write</param>
        /// <param name="stream">Stream to write to</param>
        /// <param name="token"></param>
        public static Task WriteEventsAsync(IEnumerable<EventCreate> events, Stream stream, CancellationToken token)
        {
            return Task.Run(() => WriteEvents(events, stream), token);
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
    /// Comparer for CogniteSdk Identity objects
    /// </summary>
    public class IdentityComparer : IEqualityComparer<Identity>
    {
        /// <summary>
        /// Determine if two CogniteSdk Identity objects are equal:
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
            if (string.IsNullOrWhiteSpace(obj.ExternalId))
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
        private static Action<DelegateResult<HttpResponseMessage>, TimeSpan> GetRetryHandler(ILogger logger)
        {
            return (ex, ts) =>
            {
                if (ex.Result != null)
                {
                    logger.LogDebug("Failed request with status code: {Code}. Retrying in {Time} ms. {Message}",
                        (int)ex.Result.StatusCode, ts.TotalMilliseconds, ex.Result.ReasonPhrase);
                    logger.LogDebug("Failed request: {Method} {Uri}",
                        ex.Result.RequestMessage?.Method,
                        ex.Result.RequestMessage?.RequestUri);
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
            };
        }
        static IAsyncPolicy<HttpResponseMessage> GetRetryPolicy(ILogger logger, RetryConfig config)
        {
            int numRetries = config == null ? 5 : config.MaxRetries;
            int maxDelay = config == null ? 5_000 : config.MaxDelay;
            if (maxDelay < 0) maxDelay = int.MaxValue;
            var builder = Policy
                .HandleResult<HttpResponseMessage>(msg =>
                    msg.StatusCode == HttpStatusCode.Unauthorized
                    || (int)msg.StatusCode == 429) //  HttpStatusCode.TooManyRequests not in .Net Framework, is in .Net Core 3.0
                .OrTransientHttpError()
                .Or<TimeoutRejectedException>();
            if (numRetries < 0)
            {
                return builder.WaitAndRetryForeverAsync(
                    retry => TimeSpan.FromMilliseconds(Math.Min(125 * Math.Pow(2, retry - 1), maxDelay)),
                    GetRetryHandler(logger));
            }
            else
            {
                return builder.WaitAndRetryAsync(
                    // retry interval 0.125, 0.25, 0.5, 1, 2, ..., i.e. max 0.125 * 2^numRetries
                    numRetries,
                    retry => TimeSpan.FromMilliseconds(Math.Min(125 * Math.Pow(2, Math.Min(retry - 1, numRetries)), maxDelay)),
                    GetRetryHandler(logger));
            }
                
        }

        static IAsyncPolicy<HttpResponseMessage> GetTimeoutPolicy(RetryConfig config)
        {
            TimeSpan timeout;
            if (config == null) timeout = TimeSpan.FromMilliseconds(80_000);
            else if (config.Timeout <= 0) timeout = TimeSpan.MaxValue;
            else timeout = TimeSpan.FromMilliseconds(config.Timeout);
            return Policy.TimeoutAsync<HttpResponseMessage>(timeout); // timeout for each individual try
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
        /// <param name="setHttpClient">Default true. If false CogniteSdk Client.Builder is not added to the
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
                    .AddPolicyHandler((provider, message) =>
                        GetRetryPolicy(provider.GetRequiredService<ILogger<Client>>(), provider.GetService<CogniteConfig>()?.CdfRetries))
                    .AddPolicyHandler((provider, message) => GetTimeoutPolicy(provider.GetService<CogniteConfig>()?.CdfRetries));
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
            services.AddTransient<IRawDestination, CogniteDestination>();
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
        /// True if datapoint is string
        /// </summary>
        public bool IsString => _numericValue == null;

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
        /// <summary>
        /// Creates a numeric data point
        /// </summary>
        /// <param name="timestamp">Timestamp</param>
        /// <param name="numericValue">double value</param>
        public Datapoint(long timestamp, double numericValue)
        {
            _timestamp = timestamp;
            _numericValue = numericValue;
            _stringValue = null;
        }

        /// <summary>
        /// Creates a string data point
        /// </summary>
        /// <param name="timestamp">Timestamp</param>
        /// <param name="stringValue">string value</param>
        public Datapoint(long timestamp, string stringValue)
        {
            _timestamp = timestamp;
            _numericValue = null;
            _stringValue = stringValue;
        }
        /// <summary>
        /// Convert datapoint into an array of bytes on the form
        /// [long timestamp][boolean isString]{Either [ushort length][string value] or [double value]}
        /// </summary>
        /// <returns></returns>
        public byte[] ToStorableBytes()
        {
            ushort size = sizeof(long) + sizeof(bool);

            byte[] valBytes;

            if (IsString)
            {
                valBytes = CogniteUtils.StringToStorable(_stringValue);
            }
            else
            {
                valBytes = BitConverter.GetBytes(_numericValue.Value);
            }
            size += (ushort)valBytes.Length;

            var bytes = new byte[size];
            int pos = 0;
            Buffer.BlockCopy(BitConverter.GetBytes(_timestamp), 0, bytes, pos, sizeof(long));
            pos += sizeof(long);
            Buffer.BlockCopy(BitConverter.GetBytes(IsString), 0, bytes, pos, sizeof(bool));
            pos += sizeof(bool);

            Buffer.BlockCopy(valBytes, 0, bytes, pos, valBytes.Length);

            return bytes;
        }
        /// <summary>
        /// Initializes Datapoint by reading from a stream. Requires that the next bytes in the stream represent a datapoint.
        /// </summary>
        /// <param name="stream">Stream to read from</param>
        public static Datapoint FromStream(Stream stream)
        {
            var baseBytes = new byte[sizeof(long) + sizeof(bool)];
            int read = stream.Read(baseBytes, 0, sizeof(long) + sizeof(bool));
            if (read < sizeof(long) + sizeof(bool)) return null;

            var timestamp = BitConverter.ToInt64(baseBytes, 0);
            var isString = BitConverter.ToBoolean(baseBytes, sizeof(long));

            if (isString)
            {
                string value = CogniteUtils.StringFromStream(stream);
                return new Datapoint(timestamp, value);
            }
            else
            {
                var valueBytes = new byte[sizeof(double)];
                if (stream.Read(valueBytes, 0, sizeof(double)) < sizeof(double)) return null;
                double value = BitConverter.ToDouble(valueBytes, 0);
                return new Datapoint(timestamp, value);
            }
        }
    }
}