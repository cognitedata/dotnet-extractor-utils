using System;

namespace ExtractorUtils
{
    /// <summary>
    /// Various utility functions
    /// </summary>
    public static class CommonUtils
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

    }
}
