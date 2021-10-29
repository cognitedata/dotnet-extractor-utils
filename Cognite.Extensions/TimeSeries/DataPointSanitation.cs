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
                if (point.StringValue == null || point.StringValue.Length > CogniteUtils.StringLengthMax)
                {
                    return new Datapoint(point.Timestamp, point.StringValue.Truncate(CogniteUtils.StringLengthMax) ?? "");
                }
                return point;
            }
            else
            {
                double value = point.NumericValue!.Value;
                if (!double.IsNaN(value))
                {
                    value = Math.Max(CogniteUtils.NumericValueMin, value);
                    value = Math.Min(CogniteUtils.NumericValueMax, value);
                    return value == point.NumericValue.Value ? point :
                        new Datapoint(point.Timestamp, value);
                }
                else if (nanReplacement.HasValue)
                {
                    return new Datapoint(point.Timestamp, nanReplacement.Value);
                }
            }
            return point;
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
                if (point.StringValue == null || point.StringValue.Length > CogniteUtils.StringLengthMax)
                {
                    return ResourceType.DataPointValue;
                }
            }
            else
            {
                double value = point.NumericValue!.Value;
                if (double.IsNaN(value)
                    || double.IsInfinity(value)
                    || value > CogniteUtils.NumericValueMax
                    || value < CogniteUtils.NumericValueMin)
                {
                    return ResourceType.DataPointValue;
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
                var id = kvp.Key;
                var dps = kvp.Value;

                var cleanDps = new List<Datapoint>();
                var badDps = new List<(ResourceType type, Datapoint point)>();

                foreach (var dp in dps)
                {
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
                if (badDps.Any())
                {
                    badDpGroups.AddRange(badDps
                        .GroupBy(pair => pair.type)
                        .Select(group => (group.Key, id, group.Select(pair => pair.point))));
                }
            }

            IEnumerable<CogniteError<DataPointInsertError>> errors;

            if (badDpGroups.Any())
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
