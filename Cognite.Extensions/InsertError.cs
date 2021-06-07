using System;
using System.Collections.Generic;
using CogniteSdk;

namespace Cognite.Extensions
{
    /// <summary>
    /// Class contaning errors resulting from insert operations 
    /// </summary>
    public sealed class InsertError
    {
        /// <summary>
        /// Ids/External ids not found in CDF
        /// </summary>
        public IEnumerable<Identity> IdsNotFound { get; } 
        
        /// <summary>
        /// Ids with mismatched type in CDF
        /// </summary>
        public IEnumerable<Identity> IdsWithMismatchedData { get; }

        /// <summary>
        /// Creates an instance with the provided identifiers that were not
        /// found or are mismatched
        /// </summary>
        /// <param name="idsNotFound">Ids not found</param>
        /// <param name="idsWithMismatchedData">Mismatched ids</param>
        public InsertError(IEnumerable<Identity> idsNotFound, IEnumerable<Identity> idsWithMismatchedData)
        {
            IdsNotFound = idsNotFound;
            IdsWithMismatchedData = idsWithMismatchedData;
        }

        /// <summary>
        /// Combines this InsertError object with the one provided and return
        /// a new object
        /// </summary>
        /// <param name="other">Other InsertError</param>
        public InsertError UnionWith(InsertError other) {
            if (other == null)
            {
                throw new ArgumentNullException(nameof(other));
            }
            var comparer = new IdentityComparer();
            var idsNotFound = new HashSet<Identity>(IdsNotFound, comparer);
            idsNotFound.UnionWith(other.IdsNotFound);
            var idsWithMismatchedData = new HashSet<Identity>(IdsWithMismatchedData, comparer);
            idsWithMismatchedData.UnionWith(other.IdsWithMismatchedData);
            return new InsertError(idsNotFound, idsWithMismatchedData);
        }
    }
}