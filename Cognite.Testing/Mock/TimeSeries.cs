using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extractor.Common;
using CogniteSdk;
using CogniteSdk.DataModels;
using Com.Cognite.V1.Timeseries.Proto;
using Moq;
using Xunit;

namespace Cognite.Extractor.Testing.Mock
{
    /// <summary>
    /// A wrapper around a mocked time series.
    /// </summary>
    public class TimeSeriesWrapper
    {
        /// <summary>
        /// The timeseries metadata instance.
        /// </summary>
        public TimeSeries Instance { get; }
        /// <summary>
        /// The numeric datapoints added to this timeseries.
        /// </summary>
        public List<NumericDatapoint> NumericDatapoints { get; } = new List<NumericDatapoint>();
        /// <summary>
        /// The string datapoints added to this timeseries.
        /// </summary>
        public List<StringDatapoint> StringDatapoints { get; } = new List<StringDatapoint>();

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="ts"></param>
        internal TimeSeriesWrapper(TimeSeries ts)
        {
            Instance = ts;
        }
    }

    /// <summary>
    /// Identity in the SDK cannot be deserialized, this is a workaround.
    /// </summary>
    class RawIdentity
    {
        public long? Id { get; set; }
        public string? ExternalId { get; set; }
        public InstanceIdentifier? InstanceId { get; set; }

        public Identity ToIdentity()
        {
            if (Id.HasValue)
            {
                return new Identity(Id.Value);
            }
            if (InstanceId != null)
            {
                return new Identity(InstanceId);
            }
            if (!string.IsNullOrEmpty(ExternalId))
            {
                return new Identity(ExternalId);
            }
            throw new InvalidOperationException("RawIdentity does not have any identifier set");
        }
    }

    /// <summary>
    /// Mock implementation of the timeseries API.
    /// </summary>
    public class TimeSeriesMock
    {
        private long _nextId = 10000;
        private readonly Dictionary<Identity, TimeSeriesWrapper> _timeSeries = new Dictionary<Identity, TimeSeriesWrapper>();

        /// <summary>
        /// Number of mocked timeseries.
        /// </summary>
        public int Count => _timeSeries.Count;

        /// <summary>
        /// All mocked timeseries.
        /// </summary>
        public IEnumerable<TimeSeriesWrapper> All => _timeSeries.Values;

        /// <summary>
        /// Create a mocked timeseries with the given identity and type.
        /// </summary>
        /// <param name="id">Timeseries ID.</param>
        /// <param name="isString">Indicates if the timeseries is of string type.</param>
        public TimeSeriesWrapper MockTimeSeries(Identity id, bool isString)
        {
            if (id == null) throw new ArgumentNullException(nameof(id));
            var ts = new TimeSeries
            {
                Id = id.Id ?? Interlocked.Increment(ref _nextId),
                ExternalId = id.ExternalId,
                InstanceId = id.InstanceId != null ? new InstanceIdentifier
                {
                    Space = id.InstanceId.Space,
                    ExternalId = id.InstanceId.ExternalId,
                } : null,
                IsString = isString,
                CreatedTime = DateTime.UtcNow.ToUnixTimeMilliseconds(),
                LastUpdatedTime = DateTime.UtcNow.ToUnixTimeMilliseconds(),
            };
            var mock = new TimeSeriesWrapper(ts);
            _timeSeries[id] = mock;
            return mock;
        }

        /// <summary>
        /// Mock a timeseries with the given identity and metadata.
        /// Timeseries IDs are ignored, so if you pass an identity 
        /// </summary>
        /// <param name="id">Timeseries ID.</param>
        /// <param name="timeSeries">Timeseries metadata.</param>
        public void MockTimeSeries(Identity id, TimeSeries timeSeries)
        {
            if (id == null) throw new ArgumentNullException(nameof(id));
            if (timeSeries == null) throw new ArgumentNullException(nameof(timeSeries));
            timeSeries.Id = Interlocked.Increment(ref _nextId);
            _timeSeries[id] = new TimeSeriesWrapper(timeSeries);
        }

        /// <summary>
        /// Get a timeseries by identity, if it exists.
        /// </summary>
        /// <param name="id">Timeseries ID.</param>
        /// <returns>Timeseries wrapper if found, null otherwise.</returns>
        public TimeSeriesWrapper? GetTimeSeries(Identity id)
        {
            if (id == null) throw new ArgumentNullException(nameof(id));
            if (_timeSeries.TryGetValue(id, out var ts))
            {
                return ts;
            }
            return null;
        }

        /// <summary>
        /// Get a timeseries by external ID, if it exists.
        /// </summary>
        /// <param name="id">Timeseries external ID.</param>
        /// <returns>Timeseries wrapper if found, null otherwise.</returns>
        public TimeSeriesWrapper? GetTimeSeries(string id)
        {
            if (id == null) throw new ArgumentNullException(nameof(id));
            if (_timeSeries.TryGetValue(new Identity(id), out var ts))
            {
                return ts;
            }
            return null;
        }

        /// <summary>
        /// Automatically mock timeseries with the given external IDs.
        /// If the name contains "String", the timeseries will be of string type.
        /// If the name contains "Missing", the timeseries will not be created.
        /// This is useful for testing error handling.
        /// </summary>
        /// <param name="names">List of timeseries to mock.</param>
        public void AutoMockTimeSeries(params string[] names)
        {
            if (names == null) throw new ArgumentNullException(nameof(names));
            foreach (var name in names)
            {
                if (name.Contains("Missing")) continue;
                var isString = name.Contains("String");
                MockTimeSeries(new Identity(name), isString);
            }
        }

        /// <summary>
        /// Clear the timeseries mock, removing all mocked timeseries.
        /// </summary>
        public void Clear()
        {
            _timeSeries.Clear();
        }

        /// <summary>
        /// Get a matcher for the /timeseries/byids endpoint.
        /// </summary>
        /// <param name="times">Expected number of executions.</param>
        public RequestMatcher GetByIdsMatcher(Times times)
        {
            return new SimpleMatcher("POST", "/timeseries/byids", MockTimeSeriesByIds, times);
        }

        /// <summary>
        /// Get a matcher for the /timeseries/data endpoint.
        /// </summary>
        /// <param name="times">Expected number of executions.</param>
        public RequestMatcher CreateDatapointsMatcher(Times times)
        {
            return new SimpleMatcher("POST", "/timeseries/data", MockCreateDatapoints, times);
        }

        private static Dictionary<string, MultiValue> ToMultiValueDict(Identity id)
        {
            var dict = new Dictionary<string, MultiValue>();
            if (id.Id.HasValue)
            {
                dict["id"] = MultiValue.Create(id.Id.Value);
            }
            else if (id.InstanceId != null)
            {
                dict["instanceId"] = MultiValue.Create(id.InstanceId);
            }
            else if (!string.IsNullOrEmpty(id.ExternalId))
            {
                dict["externalId"] = MultiValue.Create(id.ExternalId);
            }
            return dict;
        }

        private async Task<HttpResponseMessage> MockTimeSeriesByIds(RequestContext context, CancellationToken token)
        {
            var ids = await context.ReadJsonBody<ItemsWithIgnoreUnknownIds<RawIdentity>>().ConfigureAwait(false);
            Assert.NotNull(ids);
            var found = new List<TimeSeries>();
            var missing = new List<Identity>();
            foreach (var rawid in ids.Items)
            {
                var id = rawid.ToIdentity();
                if (_timeSeries.TryGetValue(id, out var ts))
                {
                    found.Add(ts.Instance);
                }
                else
                {
                    missing.Add(id);
                }
            }

            if (missing.Count > 0 && !ids.IgnoreUnknownIds)
            {
                return context.CreateError(new CogniteError
                {
                    Code = 400,
                    Message = "Timeseries not found",
                    Missing = missing.Distinct().Select(ToMultiValueDict).ToList(),
                });
            }
            return context.CreateJsonResponse(new ItemsWithoutCursor<TimeSeries>
            {
                Items = found,
            });
        }

        private Identity GetIdentity(DataPointInsertionItem item)
        {
            if (item.Id != 0)
            {
                return new Identity(item.Id);
            }
            if (!string.IsNullOrEmpty(item.ExternalId))
            {
                return new Identity(item.ExternalId);
            }
            if (item.InstanceId != null) return new Identity(new InstanceIdentifier
            (
                item.InstanceId.Space,
                item.InstanceId.ExternalId
            ));

            return new Identity(0);
        }

        private async Task<HttpResponseMessage> MockCreateDatapoints(RequestContext context, CancellationToken token)
        {
            var data = await context.ReadProtobufBody(DataPointInsertionRequest.Parser, token).ConfigureAwait(false);

            var missing = new List<Identity>();
            string? mismatchedExpected = null;

            foreach (var item in data.Items)
            {
                var id = GetIdentity(item);
                if (!_timeSeries.TryGetValue(id, out var ts))
                {
                    missing.Add(id);
                    continue;
                }

                if (item.NumericDatapoints != null && ts.Instance.IsString)
                {
                    mismatchedExpected = "string";
                    continue;
                }
                if (item.StringDatapoints != null && !ts.Instance.IsString)
                {
                    mismatchedExpected = "numeric";
                    continue;
                }
            }

            if (missing.Count > 0)
            {
                return context.CreateError(new CogniteError
                {
                    Code = 400,
                    Message = "Timeseries not found",
                    Missing = missing.Distinct().Select(ToMultiValueDict).ToList(),
                });
            }
            if (mismatchedExpected != null)
            {
                return context.CreateError(new CogniteError
                {
                    Code = 400,
                    Message = $"Expected {mismatchedExpected} value for datapoint",
                });
            }

            foreach (var item in data.Items)
            {
                var id = GetIdentity(item);
                var ts = _timeSeries[id];
                if (item.NumericDatapoints != null)
                {
                    ts.NumericDatapoints.AddRange(item.NumericDatapoints.Datapoints);
                }
                else if (item.StringDatapoints != null)
                {
                    ts.StringDatapoints.AddRange(item.StringDatapoints.Datapoints);
                }
            }

            return context.CreateJsonResponse(new EmptyResponse());
        }
    }
}