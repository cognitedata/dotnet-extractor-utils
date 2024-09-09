using System;
using System.Collections.Generic;
using System.Linq;
using CogniteSdk.Alpha;

namespace Cognite.Extensions.DataModels
{
    public static partial class CoreTSSanitation
    {
        /// <summary>
        /// Clean a request to insert datapoints.
        /// </summary>
        /// <param name="points">Datapoint insertion request to clean</param>
        /// <param name="mode">Sanitation mode</param>
        /// <param name="nonFiniteReplacement">Optional replacement for non-finite values</param>
        /// <returns>Cleaned request and optional list of errors</returns>
        public static (IDictionary<IdentityWithInstanceId, IEnumerable<Datapoint>>, IEnumerable<CogniteError<DataPointInsertErrorWithInstanceId>>) CleanDataPointsRequest(
            IDictionary<IdentityWithInstanceId, IEnumerable<Datapoint>> points,
            SanitationMode mode,
            double? nonFiniteReplacement)
        {
            var ret = Sanitation.CleanDataPointsRequest(points.ToDictionary(x => (CogniteSdk.IIdentity)x.Key, x => x.Value), mode, nonFiniteReplacement);
            var pts = ret.Item1.ToDictionary(x => (IdentityWithInstanceId)x.Key, x => x.Value);
            var errors = ret.Item2.Select(x => x.ReplaceSkipped<DataPointInsertErrorWithInstanceId>(y => new DataPointInsertErrorWithInstanceId(y)));

            return (pts, errors);
        }
    }
    /// <summary>
    /// Container for error on datapoint insertion.
    /// </summary>
    public class DataPointInsertErrorWithInstanceId
    {
        /// <summary>
        /// Skipped datapoints
        /// </summary>
        public IEnumerable<Datapoint> DataPoints { get; }
        /// <summary>
        /// Id of timeseries skipped for
        /// </summary>
        public IdentityWithInstanceId Id { get; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="id">Id of timeseries skipped for</param>
        /// <param name="dps">Skipped datapoints</param>
        public DataPointInsertErrorWithInstanceId(IdentityWithInstanceId id, IEnumerable<Datapoint> dps)
        {
            DataPoints = dps;
            Id = id;
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="err">DataPointInsertError to cast from</param>
        public DataPointInsertErrorWithInstanceId(DataPointInsertError err)
        {
            if (err == null)
            {
                throw new ArgumentNullException(nameof(err));
            }
            DataPoints = err.DataPoints;
            if (!(err.Id is IdentityWithInstanceId))
            {
                throw new ArgumentException("Invalid identity type contained within DataPointInsertError");
            }
            Id = (IdentityWithInstanceId)err.Id;
        }
    }
}

