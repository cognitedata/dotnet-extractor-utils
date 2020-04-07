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

    public static class Utils
    {
        public static string TrimToNull(this string @this)
        {
            string s = @this?.Trim() ?? null;
            return string.IsNullOrEmpty(s) ? null : s;
        }

    }
}
