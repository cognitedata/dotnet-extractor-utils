using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extensions.DataModels;
using Cognite.Extensions.DataModels.CogniteExtractorExtensions;
using CogniteSdk;
using CogniteSdk.Alpha;
using CogniteSdk.Beta.DataModels;
using CogniteSdk.Beta.DataModels.Core;
using CogniteSdk.Resources;
using CogniteSdk.Resources.DataModels;
using Prometheus;

namespace Cognite.Extensions
{
    public static partial class ResultHandlers
    {
        /// <summary>
        /// Clean a list of identity/datapoint pairs based on CogniteError
        /// </summary>
        /// <param name="error">Error to clean from</param>
        /// <param name="datapoints">Datapoints to remove</param>
        /// <returns>A modified version of <paramref name="datapoints"/> or an empty dictionary</returns>
        public static IDictionary<IdentityWithInstanceId, IEnumerable<Datapoint>> CleanFromError(
            CogniteError<DataPointInsertErrorWithInstanceId> error,
            IDictionary<IdentityWithInstanceId, IEnumerable<Datapoint>> datapoints)
        {
            if (datapoints == null) throw new ArgumentNullException(nameof(datapoints));
            if (error == null) return datapoints;
            // In this case we've already finished the skipping.
            if (error.Skipped?.Any() ?? false) return datapoints;
            if (error.Values == null || !error.Values.Any())
            {
                error.Skipped = datapoints.Select(kvp => new DataPointInsertErrorWithInstanceId(kvp.Key, kvp.Value)).ToList();

                error.Values = error.Skipped.Select(pair => pair.Id);
                return new Dictionary<IdentityWithInstanceId, IEnumerable<Datapoint>>();
            }

            var skipped = new List<DataPointInsertErrorWithInstanceId>();

            foreach (IdentityWithInstanceId idt in error.Values)
            {
                if (!datapoints.TryGetValue(idt, out var dps)) continue;
                skipped.Add(new DataPointInsertErrorWithInstanceId(idt, dps));
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
                error.Skipped = datapoints.Select(kvp => new DataPointInsertErrorWithInstanceId(kvp.Key, kvp.Value)).ToList();
                return new Dictionary<IdentityWithInstanceId, IEnumerable<Datapoint>>();
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
        public static async Task<(CogniteError<DataPointInsertErrorWithInstanceId>, IDictionary<IdentityWithInstanceId, IEnumerable<Datapoint>>)> VerifyDatapointsFromCDF(
            CoreTimeSeriesResource<CogniteTimeSeriesBase> resource,
            CogniteError<DataPointInsertErrorWithInstanceId> error,
            IDictionary<IdentityWithInstanceId, IEnumerable<Datapoint>> datapoints,
            int timeseriesChunkSize,
            int timeseriesThrottleSize,
            CancellationToken token)
        {
            if (datapoints == null) throw new ArgumentNullException(nameof(datapoints));
            IEnumerable<SourcedNode<CogniteTimeSeriesBase>> timeseries;
            using (CdfMetrics.TimeSeries.WithLabels("retrieve").NewTimer())
            {
                try
                {
                    timeseries = await resource
                        .GetTimeSeriesByIdsIgnoreErrors<CogniteTimeSeriesBase>(datapoints.Select(kvp => kvp.Key),
                            timeseriesChunkSize, timeseriesThrottleSize, token)
                        .ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    var err = ParseSimpleError(
                        ex,
                        datapoints.Select(kvp => kvp.Key),
                        datapoints.Select(kvp => new DataPointInsertErrorWithInstanceId(kvp.Key, kvp.Value)));
                    return (err, new Dictionary<IdentityWithInstanceId, IEnumerable<Datapoint>>());
                }
            }

            var badDps = new List<DataPointInsertErrorWithInstanceId>();
            var result = new Dictionary<IdentityWithInstanceId, IEnumerable<Datapoint>>();

            foreach (var ts in timeseries)
            {
                var idt = IdentityWithInstanceId.Create(new InstanceIdentifier(ts.Space, ts.ExternalId));
                var points = datapoints[idt];

                var bad = new List<Datapoint>();
                var good = new List<Datapoint>();

                foreach (var dp in points)
                {
                    if (dp.IsString == (ts.Properties.Type == TimeSeriesType.String)) good.Add(dp);
                    else bad.Add(dp);
                }

                if (bad.Any())
                {
                    CdfMetrics.DatapointsSkipped.Inc(bad.Count);
                    badDps.Add(new DataPointInsertErrorWithInstanceId(idt, bad));
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
                if (error == null) error = new CogniteError<DataPointInsertErrorWithInstanceId> { Message = "Mismatched timeseries" };
                error.Type = ErrorType.MismatchedType;
                error.Resource = ResourceType.DataPointValue;
                error.Skipped = badDps;
            }

            return (error, result);
        }
    }
}
