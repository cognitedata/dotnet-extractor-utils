using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extractor.Common;
using CogniteSdk;
using CogniteSdk.Alpha;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Polly;
using Polly.Extensions.Http;
using Polly.Timeout;

namespace Cognite.Extensions
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
        /// Cognite min timestamp (1900)
        /// </summary>
        public const long TimestampMin = -2208988800000L;

        /// <summary>
        /// Cognite max timestamp (2099)
        /// </summary>
        public const long TimestampMax = 4102444799999L;

        /// <summary>
        /// Write missing identities to the provided identity set.
        /// </summary>
        /// <param name="missing">Set to add missing ids to</param>
        /// <param name="e">Error containing missing ids</param>
        public static void ExtractMissingFromResponseException(HashSet<Identity> missing, ResponseException e)
        {
            if (missing is null)
            {
                throw new ArgumentNullException(nameof(missing));
            }
            if (e is null)
            {
                throw new ArgumentNullException(nameof(e));
            }
            foreach (var ts in e.Missing)
            {
                if (ts.TryGetValue("externalId", out MultiValue? exIdValue) && exIdValue != null)
                {
                    missing.Add(new Identity(exIdValue.ToString()));
                }
                else if (ts.TryGetValue("id", out MultiValue? idValue) && idValue != null)
                {
                    missing.Add(new Identity(((MultiValue.Long)idValue).Value));
                }
                else if (ts.TryGetValue("instanceId", out MultiValue? instanceIdValue) && instanceIdValue != null)
                {
                    missing.Add(new Identity(((MultiValue.InstanceId)instanceIdValue).Value));
                }
            }
        }

        /// <summary>
        /// Turn string into an array of bytes on the format [unsigned short length][string].
        /// </summary>
        /// <param name="str">String to transform</param>
        /// <returns>Storable bytes</returns>
        public static byte[] StringToStorable(string? str)
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
        public static string? StringFromStream(Stream stream, ushort? size = null)
        {
            if (stream is null)
            {
                throw new ArgumentNullException(nameof(stream));
            }
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
            if (datapoints is null)
            {
                throw new ArgumentNullException(nameof(datapoints));
            }
            if (stream is null)
            {
                throw new ArgumentNullException(nameof(stream));
            }
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
            if (stream == null)
            {
                throw new ArgumentNullException(nameof(stream));
            }
            var ret = new Dictionary<Identity, List<Datapoint>>();

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
                    string? extId = StringFromStream(stream, size);
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

            return ret.ToDictionary(kvp => kvp.Key, kvp => (IEnumerable<Datapoint>)kvp.Value);
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
            if (evt == null)
            {
                throw new ArgumentNullException(nameof(evt));
            }
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
        public static EventCreate? EventFromStream(Stream stream)
        {
            if (stream == null)
            {
                throw new ArgumentNullException(nameof(stream));
            }
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
            if (events == null)
            {
                throw new ArgumentNullException(nameof(events));
            }
            if (stream == null)
            {
                throw new ArgumentNullException(nameof(stream));
            }
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
        /// <summary>
        /// Create a polly retry policy configured for use with CDF.
        /// </summary>
        /// <param name="logger">Logger to use on retry</param>
        /// <param name="maxRetries">Maximum number of retries</param>
        /// <param name="maxDelay">Maximum delay between each retry in milliseconds, negative for no upper limit</param>
        /// <returns></returns>
        public static IAsyncPolicy<HttpResponseMessage> GetRetryPolicy(ILogger? logger,
            int? maxRetries,
            int? maxDelay)
        {
            var random = new Random();
            logger = logger ?? new NullLogger<Client>();
            int numRetries = maxRetries ?? 5;
            int delay = maxDelay ?? 5 * 60 * 1000;
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
                    retry => TimeSpan.FromMilliseconds(Math.Min(125 * Math.Pow(2, retry - 1), delay)),
                    GetRetryHandler(logger));
            }
            else
            {
                return builder.WaitAndRetryAsync(
                    // retry interval 0.125, 0.25, 0.5, 1, 2, ..., i.e. max 0.125 * 2^numRetries
                    numRetries,
                    retry =>
                    {
                        var retryDelay = Math.Min(125 * Math.Pow(2, retry - 1), delay);
                        // Jitter so we land between initial * 2 ** attempt * 3/4 and initial * 2 ** attempt * 5/4
                        retryDelay = retryDelay / 4 * 3 + random.Next((int)(retryDelay / 2));

                        return TimeSpan.FromMilliseconds(retryDelay);
                    },
                    GetRetryHandler(logger));
            }

        }
        /// <summary>
        /// Get a polly timeout policy with a timeout set to <paramref name="timeout"/> milliseconds
        /// </summary>
        /// <param name="timeout">Timeout on each request in milliseconds</param>
        /// <returns></returns>
        public static IAsyncPolicy<HttpResponseMessage> GetTimeoutPolicy(int? timeout)
        {
            TimeSpan timeoutSpan;
            if (timeout == null) timeoutSpan = TimeSpan.FromMilliseconds(80_000);
            else if (timeout <= 0) timeoutSpan = Timeout.InfiniteTimeSpan;
            else timeoutSpan = TimeSpan.FromMilliseconds(timeout.Value);
            return Policy.TimeoutAsync<HttpResponseMessage>(timeoutSpan); // timeout for each individual try
        }

        /// <summary>
        /// Add logger to client extension methods.
        /// </summary>
        /// <param name="provider">Serviceprovider to use to get the loggers</param>
        public static void AddExtensionLoggers(this IServiceProvider provider)
        {
            var logger = provider.GetService<ILogger<Client>>();
            if (logger == null) logger = new NullLogger<Client>();
            AssetExtensions.SetLogger(logger);
            DataPointExtensions.SetLogger(logger);
            TimeSeriesExtensions.SetLogger(logger);
            RawExtensions.SetLogger(logger);
            EventExtensions.SetLogger(logger);
            SequenceExtensions.SetLogger(logger);
        }
    }

    /// <summary>
    /// Data point abstraction. Consists of a timestamp and a double or string value
    /// </summary>
    public class Datapoint
    {
        private readonly long _timestamp;
        private readonly double? _numericValue;
        private readonly string? _stringValue;
        private readonly StatusCode _statusCode;

        /// <summary>
        /// Timestamp in Unix time milliseconds
        /// </summary>
        public long Timestamp => _timestamp;

        /// <summary>
        /// Optional string value
        /// </summary>
        public string? StringValue => _stringValue;

        /// <summary>
        /// Optional double value
        /// </summary>
        public double? NumericValue => _numericValue;

        /// <summary>
        /// True if datapoint is string
        /// </summary>
        public bool IsString { get; }

        /// <summary>
        /// Datapoint status code.
        /// </summary>
        public StatusCode Status => _statusCode;

        /// <summary>
        /// Creates a numeric data point
        /// </summary>
        /// <param name="timestamp">Timestamp</param>
        /// <param name="numericValue">double value</param>
        /// <param name="statusCode">BETA: set the data point status code.
        /// This is only used if the beta datapoints endpoint is used.</param>
        public Datapoint(DateTime timestamp, double numericValue, StatusCode? statusCode = null) : this(timestamp.ToUnixTimeMilliseconds(), numericValue, statusCode)
        {
        }

        /// <summary>
        /// Creates a string data point
        /// </summary>
        /// <param name="timestamp">Timestamp</param>
        /// <param name="stringValue">string value</param>
        /// <param name="statusCode">BETA: set the data point status code.
        /// This is only used if the beta datapoints endpoint is used.</param>
        public Datapoint(DateTime timestamp, string? stringValue, StatusCode? statusCode = null) : this(timestamp.ToUnixTimeMilliseconds(), stringValue, statusCode)
        {
        }

        /// <summary>
        /// Creates a data point without a value. You still need to specify whether
        /// the time series it is being written to is a string or numeric time series.
        /// Default quality is bad.
        /// </summary>
        /// <param name="timestamp">Timestamp</param>
        /// <param name="isString">Whether the time series is string.</param>
        /// <param name="statusCode">BETA: set the data point status code.
        /// This is only used if the beta datapoints endpoint is used.</param>
        public Datapoint(DateTime timestamp, bool isString, StatusCode? statusCode = null) : this(timestamp.ToUnixTimeMilliseconds(), isString, statusCode)
        {
        }
        /// <summary>
        /// Creates a numeric data point
        /// </summary>
        /// <param name="timestamp">Timestamp</param>
        /// <param name="numericValue">double value</param>
        /// <param name="statusCode">BETA: set the data point status code.
        /// This is only used if the beta datapoints endpoint is used.</param>
        public Datapoint(long timestamp, double numericValue, StatusCode? statusCode = null)
        {
            _timestamp = timestamp;
            _numericValue = numericValue;
            IsString = false;
            _stringValue = null;
            _statusCode = statusCode ?? StatusCode.FromCategory(StatusCodeCategory.Good);
        }

        /// <summary>
        /// Creates a string data point
        /// </summary>
        /// <param name="timestamp">Timestamp</param>
        /// <param name="stringValue">string value</param>
        /// <param name="statusCode">BETA: set the data point status code.
        /// This is only used if the beta datapoints endpoint is used.</param>
        public Datapoint(long timestamp, string? stringValue, StatusCode? statusCode = null)
        {
            _timestamp = timestamp;
            _numericValue = null;
            _stringValue = stringValue;
            IsString = true;
            _statusCode = statusCode ?? StatusCode.FromCategory(StatusCodeCategory.Good);
        }

        /// <summary>
        /// Creates a data point without a value. You still need to specify whether
        /// the time series it is being written to is a string or numeric time series.
        /// </summary>
        /// <param name="timestamp">Timestamp</param>
        /// <param name="isString">Whether the time series is string.</param>
        /// <param name="statusCode">BETA: set the data point status code.
        /// This is only used if the beta datapoints endpoint is used.</param>
        public Datapoint(long timestamp, bool isString, StatusCode? statusCode = null)
        {
            _timestamp = timestamp;
            _numericValue = null;
            _stringValue = null;
            IsString = isString;
            _statusCode = statusCode ?? StatusCode.FromCategory(StatusCodeCategory.Bad);
        }
        /// <summary>
        /// Convert datapoint into an array of bytes on the form
        /// [long timestamp][boolean isString]{Either [ushort length][string value] or [double value]}
        /// </summary>
        /// <returns></returns>
        public byte[] ToStorableBytes()
        {
            ushort size = sizeof(long) + sizeof(bool) + sizeof(ulong);

            byte[] valBytes;

            if (IsString)
            {
                valBytes = CogniteUtils.StringToStorable(_stringValue);
            }
            else
            {
                valBytes = BitConverter.GetBytes(_numericValue!.Value);
            }
            size += (ushort)valBytes.Length;

            var bytes = new byte[size];
            int pos = 0;
            Buffer.BlockCopy(BitConverter.GetBytes(_timestamp), 0, bytes, pos, sizeof(long));
            pos += sizeof(long);
            Buffer.BlockCopy(BitConverter.GetBytes(IsString), 0, bytes, pos, sizeof(bool));
            pos += sizeof(bool);
            Buffer.BlockCopy(BitConverter.GetBytes(_statusCode.Code), 0, bytes, pos, sizeof(ulong));
            pos += sizeof(ulong);

            Buffer.BlockCopy(valBytes, 0, bytes, pos, valBytes.Length);

            return bytes;
        }
        /// <summary>
        /// Initializes Datapoint by reading from a stream. Requires that the next bytes in the stream represent a datapoint.
        /// </summary>
        /// <param name="stream">Stream to read from</param>
        public static Datapoint? FromStream(Stream stream)
        {
            if (stream is null)
            {
                throw new ArgumentNullException(nameof(stream));
            }
            var readLength = sizeof(long) + sizeof(bool) + sizeof(ulong);
            var baseBytes = new byte[readLength];
            int read = stream.Read(baseBytes, 0, readLength);
            if (read < readLength) return null;

            var timestamp = BitConverter.ToInt64(baseBytes, 0);
            var isString = BitConverter.ToBoolean(baseBytes, sizeof(long));
            var statusCode = BitConverter.ToUInt64(baseBytes, sizeof(long) + sizeof(bool));

            if (isString)
            {
                string? value = CogniteUtils.StringFromStream(stream);
                return new Datapoint(timestamp, value, StatusCode.Create(statusCode));
            }
            else
            {
                var valueBytes = new byte[sizeof(double)];
                if (stream.Read(valueBytes, 0, sizeof(double)) < sizeof(double)) return null;
                double value = BitConverter.ToDouble(valueBytes, 0);
                return new Datapoint(timestamp, value, StatusCode.Create(statusCode));
            }
        }
    }
}