using System;
using System.Linq;
using System.Reflection;
using System.Runtime.Serialization;
using YamlDotNet.Core;
using YamlDotNet.Core.Events;
using YamlDotNet.Serialization;

namespace Cognite.Extractor.Configuration
{
    internal class YamlEnumConverter : IYamlTypeConverter
    {
        public bool Accepts(Type type)
        {
            return type.IsEnum || (Nullable.GetUnderlyingType(type)?.IsEnum ?? false);
        }

        public object? ReadYaml(IParser parser, Type type)
        {
            var scalar = parser.Consume<Scalar>();

            if (scalar.Value == null)
            {
                if (Nullable.GetUnderlyingType(type) != null)
                {
                    return null;
                }
                throw new YamlException($"Failed to deserialize null value to enum {type.Name}");
            }

            type = Nullable.GetUnderlyingType(type) ?? type;

            var values = type.GetMembers()
                .Select(m => (m.GetCustomAttributes<EnumMemberAttribute>(true).Select(f => f.Value).FirstOrDefault(), m))
                .Where(pair => !string.IsNullOrEmpty(pair.Item1))
                .ToDictionary(pair => pair.Item1!, pair => pair.m);

            if (values.TryGetValue(scalar.Value, out var enumMember))
            {
                return Enum.Parse(type, enumMember.Name, true);
            }
            return Enum.Parse(type, scalar.Value, true);
        }

        public void WriteYaml(IEmitter emitter, object? value, Type type)
        {
            if (value == null) return;
            var member = type.GetMember(value.ToString() ?? "").FirstOrDefault();
            var stringValue = member?.GetCustomAttributes<EnumMemberAttribute>(true)?.Select(f => f.Value)?.FirstOrDefault() ?? value.ToString();
            emitter.Emit(new Scalar(AnchorName.Empty, TagName.Empty, stringValue!, ScalarStyle.DoubleQuoted, false, true));
        }
    }
}
