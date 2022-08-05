using System.Collections.Generic;
using CogniteSdk;

namespace Cognite.Extensions
{
    /// <summary>
    /// Class contaning errors resulting from delete operations 
    /// </summary>
    public sealed class DeleteError
    {
        /// <summary>
        /// Ids/External ids not found in CDF
        /// </summary>
        public IEnumerable<Identity> IdsNotFound { get; }
        
        /// <summary>
        /// Ids of time series with unconfirmed data point deletions.
        /// DEPRECATED, timeseries is now immediately consistent, and we no longer verify deletion.
        /// </summary>
        public IEnumerable<Identity> IdsDeleteNotConfirmed { get; }

        /// <summary>
        /// Creates an instance with the provided identifiers that were not
        /// found or with unconfirmed deletions
        /// </summary>
        /// <param name="idsNotFound">Ids not found</param>
        /// <param name="idsDeleteNotConfirmed">Mismatched ids. DEPRECATED, leave empty.</param>
        public DeleteError(IEnumerable<Identity> idsNotFound, IEnumerable<Identity> idsDeleteNotConfirmed)
        {
            IdsNotFound = idsNotFound;
            IdsDeleteNotConfirmed = idsDeleteNotConfirmed;
        }
    }
}