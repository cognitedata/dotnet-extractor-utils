using System;

namespace ExtractorUtils
{
    internal static class CommonUtil
    {
        public static bool IsNumericType(this Type t)
        {
            var tc = Type.GetTypeCode(t);
            return tc >= TypeCode.SByte && tc <= TypeCode.Decimal;
        }
    }

    /// <summary>
    /// Various utility functions
    /// </summary>
    public static class Utils
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
