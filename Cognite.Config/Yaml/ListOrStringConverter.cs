using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using Cognite.Common;
using YamlDotNet.Core;
using YamlDotNet.Core.Events;
using YamlDotNet.Serialization;

namespace Cognite.Extractor.Configuration
{
    internal class ListOrStringConverter : IYamlTypeConverter
    {
        private static readonly Regex _whitespaceRegex = new Regex(@"\s", RegexOptions.Compiled | RegexOptions.CultureInvariant);
        public bool Accepts(Type type)
        {
            return type == typeof(ListOrSpaceSeparated);
        }

        public object? ReadYaml(IParser parser, Type type)
        {
            if (parser.TryConsume<Scalar>(out var scalar))
            {
                var items = _whitespaceRegex.Split(scalar.Value);
                return new ListOrSpaceSeparated(items.Select(TemplatedValueDeserializer.Replace).ToArray());
            }
            if (parser.TryConsume<SequenceStart>(out _))
            {
                var items = new List<string>();
                while (!parser.Accept<SequenceEnd>(out _))
                {
                    var seqScalar = parser.Consume<Scalar>();
                    items.Add(seqScalar.Value);
                }

                parser.Consume<SequenceEnd>();

                return new ListOrSpaceSeparated(items.Select(TemplatedValueDeserializer.Replace).ToArray());
            }

            throw new InvalidOperationException("Expected list or value");
        }

        public void WriteYaml(IEmitter emitter, object? value, Type type)
        {
            emitter.Emit(new SequenceStart(AnchorName.Empty, TagName.Empty, true,
                SequenceStyle.Block, Mark.Empty, Mark.Empty));
            var it = value as ListOrSpaceSeparated;
            foreach (var elem in it!.Values)
            {
                emitter.Emit(new Scalar(AnchorName.Empty, TagName.Empty, elem, ScalarStyle.DoubleQuoted, false, true));
            }
            emitter.Emit(new SequenceEnd());
        }
    }
}
