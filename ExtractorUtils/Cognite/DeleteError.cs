using System.Collections.Generic;
using CogniteSdk;

namespace Cognite.Extractor.Utils
{
    /// <summary>
    /// Class contaning errors resulting from delete operations 
    /// </summary>
    public sealed class DeleteError
    {
        /// <summary>
        /// Ids/External ids not found in CDF
        /// </summary>
        public readonly IEnumerable<Identity> IdsNotFound;
        
        /// <summary>
        /// Ids of time series with unconfirmed data point deletions
        /// </summary>
        public readonly IEnumerable<Identity> IdsDeleteNotConfirmed;

        /// <summary>
        /// Creates an instance with the provided identifiers that were not
        /// found or with unconfirmed deletions
        /// </summary>
        /// <param name="idsNotFound">Ids not found</param>
        /// <param name="idsDeleteNotConfirmed">Mismatched ids</param>
        public DeleteError(IEnumerable<Identity> idsNotFound, IEnumerable<Identity> idsDeleteNotConfirmed)
        {
            IdsNotFound = idsNotFound;
            IdsDeleteNotConfirmed = idsDeleteNotConfirmed;
        }
    }
}