using System;

namespace Cognite.Common
{
    /// <summary>
    /// A wrapper for yaml types that are serialized either as lists or as space separated values.
    /// </summary>
    public class ListOrSpaceSeparated
    {
        /// <summary>
        /// Inner values
        /// </summary>
        public string[] Values { get; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="values">List of values, will always be serialized as a list</param>
        public ListOrSpaceSeparated(params string[] values)
        {
            Values = values;
        }

        /// <summary>
        /// Explicit conversion to string array
        /// </summary>
        /// <param name="list">List of values</param>
        public static implicit operator string[](ListOrSpaceSeparated list) => list?.Values ?? throw new ArgumentNullException(nameof(list));
    }
}