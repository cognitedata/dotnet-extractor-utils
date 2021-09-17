using CogniteSdk;
using CogniteSdk.Resources;
using Prometheus;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.Extensions
{
    public static partial class ResultHandlers
    {
        private static void ParseDatapointsException(ResponseException ex, CogniteError err)
        {
            if (ex.Missing?.Any() ?? false)
            {
                err.Type = ErrorType.ItemMissing;
                err.Resource = ResourceType.Id;
                err.Values = ex.Missing.Select(dict =>
                {
                    if (dict.TryGetValue("id", out var idVal) && idVal is MultiValue.Long longVal)
                    {
                        return Identity.Create(longVal.Value);
                    }
                    else if (dict.TryGetValue("externalId", out var extIdVal) && extIdVal is MultiValue.String stringVal)
                    {
                        return Identity.Create(stringVal.Value);
                    }
                    return null;
                }).Where(id => id != null);
            }
            else if (ex.Message == "Expected string value for datapoint" || ex.Message == "Expected numeric value for datapoint")
            {
                err.Type = ErrorType.MismatchedType;
                err.Resource = ResourceType.DataPointValue;
                err.Complete = false;
            }
        }

        /// <summary>
        /// Clean a list of identity/datapoint pairs based on CogniteError
        /// </summary>
        /// <param name="error">Error to clean from</param>
        /// <param name="datapoints">Datapoints to remove</param>
        /// <returns>A modified version of <paramref name="datapoints"/> or an empty dictionary</returns>
        public static IDictionary<Identity, IEnumerable<Datapoint>> CleanFromError(
            CogniteError error,
            IDictionary<Identity, IEnumerable<Datapoint>> datapoints)
        {
            if (datapoints == null) throw new ArgumentNullException(nameof(datapoints));
            if (error == null) return datapoints;
            // In this case we've already finished the skipping.
            if (error.Skipped?.Any() ?? false) return datapoints;
            if (!error.Values?.Any() ?? true)
            {
                error.Skipped = datapoints.Select(kvp => new DataPointInsertError(kvp.Key, kvp.Value)).ToList();
                error.Values = error.Skipped.Select(pair => ((DataPointInsertError)pair).Id);
                return new Dictionary<Identity, IEnumerable<Datapoint>>(new IdentityComparer());
            }

            var skipped = new List<object>();

            foreach (var idt in error.Values)
            {
                if (!datapoints.TryGetValue(idt, out var dps)) continue;
                skipped.Add(new DataPointInsertError(idt, dps));
                CdfMetrics.DatapointsSkipped.Inc(dps.Count());
                datapoints.Remove(idt);
                CdfMetrics.DatapointTimeseriesSkipped.Inc();
            }

            if (skipped.Any())
            {
                error.Skipped = skipped;
            }
            else
            {
                error.Skipped = datapoints.Select(kvp => new DataPointInsertError(kvp.Key, kvp.Value)).ToList();
                return new Dictionary<Identity, IEnumerable<Datapoint>>(new IdentityComparer());
            }

            return datapoints;
        }
        /// <summary>
        /// Ensure that the given list of datapoints have timeseries with matching types in CDF.
        /// Does not report missing timeseries, but will skip them. Typically this will be called
        /// after trying to insert to CDF, for efficiency.
        /// </summary>
        /// <param name="resource">CogniteSdk TimeSeries resource</param>
        /// <param name="error">Cognite error, optional</param>
        /// <param name="datapoints">Datapoints to verify</param>
        /// <param name="timeseriesChunkSize">Max number of timeseries to read at a time</param>
        /// <param name="timeseriesThrottleSize">Maximum number of parallel requests for timeseries</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>Verified datapoint insertions and optional error</returns>
        public static async Task<(CogniteError, IDictionary<Identity, IEnumerable<Datapoint>>)> VerifyDatapointsFromCDF(
            TimeSeriesResource resource,
            CogniteError error,
            IDictionary<Identity, IEnumerable<Datapoint>> datapoints,
            int timeseriesChunkSize,
            int timeseriesThrottleSize,
            CancellationToken token)
        {
            if (datapoints == null) throw new ArgumentNullException(nameof(datapoints));
            IEnumerable<TimeSeries> timeseries;
            using (CdfMetrics.TimeSeries.WithLabels("retrieve").NewTimer())
            {
                timeseries = await resource
                    .GetTimeSeriesByIdsIgnoreErrors(datapoints.Select(kvp => kvp.Key),
                        timeseriesChunkSize, timeseriesThrottleSize, token)
                    .ConfigureAwait(false);
            }

            var badDps = new List<DataPointInsertError>();
            var result = new Dictionary<Identity, IEnumerable<Datapoint>>(new IdentityComparer());

            foreach (var ts in timeseries)
            {
                var idt = datapoints.ContainsKey(Identity.Create(ts.Id)) ? Identity.Create(ts.Id) : Identity.Create(ts.ExternalId);
                var points = datapoints[idt];

                var bad = new List<Datapoint>();
                var good = new List<Datapoint>();

                foreach (var dp in points)
                {
                    if (dp.IsString == ts.IsString) good.Add(dp);
                    else bad.Add(dp);
                }

                if (bad.Any())
                {
                    CdfMetrics.DatapointsSkipped.Inc(bad.Count);
                    badDps.Add(new DataPointInsertError(idt, bad));
                }
                if (good.Any())
                {
                    result[idt] = good;
                }
                else
                {
                    CdfMetrics.DatapointTimeseriesSkipped.Inc();
                }
            }

            if (badDps.Any())
            {
                if (error == null) error = new CogniteError { Message = "Mismatched timeseries" };
                error.Type = ErrorType.MismatchedType;
                error.Resource = ResourceType.DataPointValue;
                error.Skipped = badDps;
            }

            return (error, result);
        }
    }
}
