using System;
using System.Text.RegularExpressions;
using YamlDotNet.Core;
using YamlDotNet.Core.Events;
using YamlDotNet.Serialization;

namespace Cognite.Extractor.Configuration
{
    internal class TemplatedValueDeserializer : INodeDeserializer
    {
        public TemplatedValueDeserializer()
        {
        }

        private static bool IsNumericType(Type t)
        {
            var tc = Type.GetTypeCode(t);
            return tc >= TypeCode.SByte && tc <= TypeCode.Decimal;
        }

        private static readonly Regex _envRegex = new Regex(@"\$\{([A-Za-z0-9_]+)\}", RegexOptions.Compiled | RegexOptions.CultureInvariant);

        bool INodeDeserializer.Deserialize(IParser parser, Type expectedType, Func<IParser, Type, object?> nestedObjectDeserializer, out object? value, ObjectDeserializer deserializer)
        {
            if (expectedType != typeof(string) && !IsNumericType(expectedType))
            {
                value = null;
                return false;
            }

            if (parser.Accept<Scalar>(out var scalar) && scalar != null && _envRegex.IsMatch(scalar.Value))
            {
                parser.MoveNext();
                value = Replace(scalar.Value);
                return true;
            }
            value = null;
            return false;
        }

        public static string Replace(string toReplace)
        {
            return _envRegex.Replace(toReplace, LookupEnvironment);
        }

        private static string LookupEnvironment(Match match)
        {
            return Environment.GetEnvironmentVariable(match.Groups[1].Value) ?? "";
        }
    }
}
