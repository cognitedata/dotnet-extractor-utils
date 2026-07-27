using CogniteSdk;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Cognite.Extensions
{
    public static partial class Sanitation
    {
        /// <summary>
        /// Sanitize this datapoint so that its value is safe for CDF consumption.
        /// This does not touch timestamp.
        /// </summary>
        /// <param name="point">Datapoint to sanitize</param>
        /// <param name="nanReplacement">Replacement for Infinite or NaN values</param>
        /// <returns>Sanitized datapoint. Same datapoint object if nothing required changing</returns>
        public static Datapoint Sanitize(this Datapoint point, double? nanReplacement)
        {
            if (point.IsString)
            {
                if (point.StringValue == null && !point.Status.IsBad)
                {
                    return new Datapoint(point.Timestamp, (string?)"", point.Status);
                }
                else if (SafeByteCount(point.StringValue) > CogniteUtils.TimeSeriesStringBytesMax)
                {
                    return new Datapoint(point.Timestamp, (string?)(point.StringValue.TruncateBytes(CogniteUtils.TimeSeriesStringBytesMax) ?? ""), point.Status);
                }
                return point;
            }
            // The string-byte-length limit is a hard wire-format constraint, so unlike the numeric
            // range checks below it applies regardless of Status.IsBad, matching the IsString branch above.
            string? safeStringValue = point.IsState && SafeByteCount(point.StringValue) > CogniteUtils.TimeSeriesStringBytesMax
                ? point.StringValue.TruncateBytes(CogniteUtils.TimeSeriesStringBytesMax) ?? ""
                : point.StringValue;
            bool stringChanged = point.IsState && safeStringValue != point.StringValue;

            Datapoint Rebuild(double v) => point.IsState
                ? new Datapoint(point.Timestamp, v, safeStringValue ?? "", point.Status)
                : new Datapoint(point.Timestamp, v, point.Status);

            if (!point.Status.IsBad)
            {
                if (!point.NumericValue.HasValue)
                {
                    return Rebuild(0);
                }
                double value = point.NumericValue.Value;
                if (!double.IsNaN(value))
                {
                    value = Math.Max(CogniteUtils.NumericValueMin, value);
                    value = Math.Min(CogniteUtils.NumericValueMax, value);
                    return value == point.NumericValue.Value && !stringChanged ? point : Rebuild(value);
                }
                else if (nanReplacement.HasValue)
                {
                    return Rebuild(nanReplacement.Value);
                }
            }
            return stringChanged ? Rebuild(point.NumericValue ?? 0) : point;
        }
        /// <summary>
        /// Verify that this datapoint can be safely consumed by CDF.
        /// </summary>
        /// <param name="point">Point to verify</param>
        /// <returns>Either DataPointValue or DataPointTimestamp if these are wrong, else null</returns>
        public static ResourceType? Verify(this Datapoint point)
        {
            if (point.IsString)
            {
                if (point.StringValue == null && !point.Status.IsBad
                    || SafeByteCount(point.StringValue) > CogniteUtils.TimeSeriesStringBytesMax)
                {
                    return ResourceType.DataPointValue;
                }
            }
            else
            {
                // Unconditional, like the IsString byte-length check above: a hard wire-format
                // constraint that applies regardless of Status.IsBad.
                if (point.IsState && SafeByteCount(point.StringValue) > CogniteUtils.TimeSeriesStringBytesMax)
                {
                    return ResourceType.DataPointValue;
                }
                if (!point.Status.IsBad)
                {
                    if (!point.NumericValue.HasValue)
                    {
                        return ResourceType.DataPointValue;
                    }
                    double value = point.NumericValue.Value;
                    if (double.IsNaN(value)
                        || double.IsInfinity(value)
                        || value > CogniteUtils.NumericValueMax
                        || value < CogniteUtils.NumericValueMin)
                    {
                        return ResourceType.DataPointValue;
                    }
                }
            }
            if (point.Timestamp > CogniteUtils.TimestampMax
                || point.Timestamp < CogniteUtils.TimestampMin)
            {
                return ResourceType.DataPointTimestamp;
            }

            return null;
        }

        /// <summary>
        /// Clean a request to insert datapoints.
        /// </summary>
        /// <param name="points">Datapoint insertion request to clean</param>
        /// <param name="mode">Sanitation mode</param>
        /// <param name="nonFiniteReplacement">Optional replacement for non-finite values</param>
        /// <returns>Cleaned request and optional list of errors</returns>
        public static (IDictionary<Identity, IEnumerable<Datapoint>>, IEnumerable<CogniteError<DataPointInsertError>>) CleanDataPointsRequest(
            IDictionary<Identity, IEnumerable<Datapoint>> points,
            SanitationMode mode,
            double? nonFiniteReplacement)
        {
            if (mode == SanitationMode.None) return (points, Enumerable.Empty<CogniteError<DataPointInsertError>>());
            if (points == null) throw new ArgumentNullException(nameof(points));

            var result = new Dictionary<Identity, IEnumerable<Datapoint>>();

            var badDpGroups = new List<(ResourceType type, Identity id, IEnumerable<Datapoint> dps)>();

            foreach (var kvp in points)
            {
                if (!kvp.Value.Any()) continue;

                var isString = kvp.Value.First().IsString;
                var isState = kvp.Value.First().IsState;

                var id = kvp.Key;
                var dps = kvp.Value;

                var cleanDps = new List<Datapoint>();
                var badDps = new List<(ResourceType type, Datapoint point)>();

                foreach (var dp in dps)
                {
                    if (dp.IsString != isString || dp.IsState != isState)
                    {
                        badDps.Add((ResourceType.DataPointValue, dp));
                        CdfMetrics.DatapointsSkipped.Inc();
                        continue;
                    }

                    var cleanDp = dp;
                    if (mode == SanitationMode.Clean)
                    {
                        cleanDp = dp.Sanitize(nonFiniteReplacement);
                    }
                    var err = cleanDp.Verify();
                    if (err.HasValue)
                    {
                        badDps.Add((err.Value, dp));
                        CdfMetrics.DatapointsSkipped.Inc();
                    }
                    else
                    {
                        cleanDps.Add(cleanDp);
                    }
                }

                if (cleanDps.Any())
                {
                    result[id] = cleanDps;
                }
                else
                {
                    CdfMetrics.DatapointTimeseriesSkipped.Inc();
                }
                if (badDps.Count > 0)
                {
                    badDpGroups.AddRange(badDps
                        .GroupBy(pair => pair.type)
                        .Select(group => (group.Key, id, group.Select(pair => pair.point))));
                }
            }

            IEnumerable<CogniteError<DataPointInsertError>> errors;

            if (badDpGroups.Count > 0)
            {
                errors = badDpGroups
                    .GroupBy(group => group.type)
                    .Select(group =>
                        new CogniteError<DataPointInsertError>
                        {
                            Status = 400,
                            Message = "Sanitation failed",
                            Resource = group.Key,
                            Type = ErrorType.SanitationFailed,
                            Skipped = group.Select(g => new DataPointInsertError(g.id, g.dps)).ToList(),
                        }
                    ).ToList();
            }
            else
            {
                errors = Enumerable.Empty<CogniteError<DataPointInsertError>>();
            }

            return (result, errors);
        }
    }
    /// <summary>
    /// Container for error on datapoint insertion.
    /// </summary>
    public class DataPointInsertError
    {
        /// <summary>
        /// Skipped datapoints
        /// </summary>
        public IEnumerable<Datapoint> DataPoints { get; }
        /// <summary>
        /// Id of timeseries skipped for
        /// </summary>
        public Identity Id { get; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="id">Id of timeseries skipped for</param>
        /// <param name="dps">Skipped datapoints</param>
        public DataPointInsertError(Identity id, IEnumerable<Datapoint> dps)
        {
            DataPoints = dps;
            Id = id;
        }
    }
}
