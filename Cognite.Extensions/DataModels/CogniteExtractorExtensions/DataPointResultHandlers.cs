using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extensions.DataModels.CogniteExtractorExtensions;
using CogniteSdk;
using CogniteSdk.DataModels;
using CogniteSdk.DataModels.Core;
using CogniteSdk.Resources.DataModels;
using Prometheus;

namespace Cognite.Extensions
{
    public static partial class ResultHandlers
    {
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
        public static async Task<(CogniteError<DataPointInsertError>, IDictionary<Identity, IEnumerable<Datapoint>>)> VerifyDatapointsFromCDF(
            CoreTimeSeriesResource<CogniteTimeSeriesBase> resource,
            CogniteError<DataPointInsertError> error,
            IDictionary<Identity, IEnumerable<Datapoint>> datapoints,
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
                        datapoints.Select(kvp => new DataPointInsertError(kvp.Key, kvp.Value)));
                    return (err, new Dictionary<Identity, IEnumerable<Datapoint>>());
                }
            }

            var badDps = new List<DataPointInsertError>();
            var result = new Dictionary<Identity, IEnumerable<Datapoint>>();

            foreach (var ts in timeseries)
            {
                var idt = Identity.Create(new InstanceIdentifier(ts.Space, ts.ExternalId));
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
                if (error == null) error = new CogniteError<DataPointInsertError> { Message = "Mismatched timeseries" };
                error.Type = ErrorType.MismatchedType;
                error.Resource = ResourceType.DataPointValue;
                error.Skipped = badDps;
            }

            return (error, result);
        }
    }
}
