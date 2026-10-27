using System;
using System.Globalization;
using System.Text.RegularExpressions;
using System.Text.Json.Nodes;
using YamlDotNet.Core;
using YamlDotNet.Core.Events;
using YamlDotNet.Serialization;

namespace Cognite.Extractor.Utils.Unstable.Configuration
{
    /// <summary>
    /// Untyped parameters of a <see cref="CdfMappingItem"/>, read from YAML as JSON. Plain scalars are typed
    /// the way YAML does it (<c>true</c>, <c>5</c>, <c>1.5</c>, <c>null</c>), quoted scalars stay strings.
    /// </summary>
    public class CdfMappingParameters : IYamlConvertible
    {
        private static readonly Regex _intRegex = new Regex(@"^[-+]?[0-9]+$", RegexOptions.CultureInvariant);
        private static readonly Regex _floatRegex = new Regex(@"^[-+]?(\.[0-9]+|[0-9]+(\.[0-9]*)?)([eE][-+]?[0-9]+)?$", RegexOptions.CultureInvariant);

        /// <summary>
        /// The parsed value. Normally a <see cref="JsonObject"/>, other nodes are rejected by validation.
        /// </summary>
        public JsonNode? Value { get; set; }

        void IYamlConvertible.Read(IParser parser, Type expectedType, ObjectDeserializer nestedObjectDeserializer)
        {
            Value = ReadNode(parser);
        }

        void IYamlConvertible.Write(IEmitter emitter, ObjectSerializer nestedObjectSerializer)
        {
            throw new NotSupportedException("Writing CDF mapping parameters to YAML is not supported.");
        }

        private static JsonNode? ReadNode(IParser parser)
        {
            if (parser.Accept<Scalar>(out var scalar))
            {
                parser.MoveNext();
                return ResolveScalar(scalar);
            }
            if (parser.Accept<MappingStart>(out _))
            {
                parser.MoveNext();
                var obj = new JsonObject();
                while (!parser.Accept<MappingEnd>(out _))
                {
                    var key = parser.Consume<Scalar>().Value;
                    obj[key] = ReadNode(parser);
                }
                parser.MoveNext();
                return obj;
            }
            if (parser.Accept<SequenceStart>(out _))
            {
                parser.MoveNext();
                var arr = new JsonArray();
                while (!parser.Accept<SequenceEnd>(out _))
                {
                    arr.Add(ReadNode(parser));
                }
                parser.MoveNext();
                return arr;
            }
            throw new YamlException("Unexpected YAML event in cdf-mapping parameters");
        }

        private static JsonNode? ResolveScalar(Scalar scalar)
        {
            if (scalar.Style != ScalarStyle.Plain) return JsonValue.Create(scalar.Value);

            var text = scalar.Value;
            switch (text)
            {
                case "":
                case "~":
                case "null":
                case "Null":
                case "NULL":
                    return null;
                case "true":
                case "True":
                case "TRUE":
                    return JsonValue.Create(true);
                case "false":
                case "False":
                case "FALSE":
                    return JsonValue.Create(false);
            }

            if (_intRegex.IsMatch(text) && long.TryParse(text, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out var l))
            {
                return JsonValue.Create(l);
            }
            if ((_intRegex.IsMatch(text) || _floatRegex.IsMatch(text))
                && double.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out var d))
            {
                return JsonValue.Create(d);
            }
            return JsonValue.Create(text);
        }
    }
}
