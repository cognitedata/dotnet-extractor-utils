using System;
using System.Collections.Generic;

namespace Cognite.Utils
{
    /// <summary>
    /// Various utility functions
    /// </summary>
    public static class CogniteUtils
    {
        /// <summary>
        /// Trim this string object to null
        /// </summary>
        /// <returns>A string with all leading and trailing white-space. If empty or null, returns null</returns>
        public static string TrimToNull(this string @this)
        {
            string s = @this?.Trim() ?? null;
            return string.IsNullOrEmpty(s) ? null : s;
        }

        /// <summary>
        /// Try to get the value associated with the provided <paramref name="key"/>.
        /// If it is not found, returns <paramref name="defaultValue"/>
        /// </summary>
        /// <param name="dict">This dictionary</param>
        /// <param name="key">Key to search</param>
        /// <param name="defaultValue">Default value to return</param>
        /// <returns>Value or default</returns>
        public static TValue GetValue<TKey,TValue>(this IDictionary<TKey, TValue> dict, TKey key, TValue defaultValue)
        {
            return dict.TryGetValue(key, out TValue value) ? value : defaultValue;
        }
    }
}
