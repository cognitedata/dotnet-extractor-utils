using System;

namespace ExtractorUtils
{
    public static class CommonUtil
    {
        public static bool IsNumericType(this Type t)
        {
            var tc = Type.GetTypeCode(t);
            return tc >= TypeCode.SByte && tc <= TypeCode.Decimal;
        }

    }
}
