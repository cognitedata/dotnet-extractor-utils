using System;
using System.Globalization;
using Cognite.Extractor.Common;
using YamlDotNet.Core;
using YamlDotNet.Core.Events;
using YamlDotNet.Serialization;

namespace Cognite.Extractor.Configuration
{
    /// <summary>
    /// Wrapper around a timestamp, for configuration objects.
    /// </summary>
    public class TimestampWrapper
    {
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="value">Raw value</param>
        /// <exception cref="ConfigurationException">If the provided value is non-null, and an invalid timestamp</exception>
        public TimestampWrapper(string? value)
        {
            if (!string.IsNullOrWhiteSpace(value) && CogniteTime.ParseTimestampString(value) is null)
            {
                throw new ConfigurationException($"Invalid timestamp {value}, must be on the form 'yyyy-MM-dd[THH:mm:ss]Z', or N[ms|s|m|h|d](-ago)");
            }
            RawValue = value;
        }

        /// <summary>
        /// Create a timestamp wrapper from a datetime. Will be set to a constant value.
        /// </summary>
        /// <param name="value"></param>
        public TimestampWrapper(DateTime value)
        {
            RawValue = value.ToString("yyyy-MM-ddTHH:mm:ssZ", CultureInfo.InvariantCulture);
        }

        /// <summary>
        /// The raw value of the timestamp, written to during deserialization.
        /// </summary>
        public string? RawValue { get; }
        /// <summary>
        /// Get the current datetime value.
        /// </summary>
        /// <returns></returns>
        public DateTime? Get()
        {
            if (RawValue == null) return null;
            // Should never fail, but we throw an error here to be safe.
            return CogniteTime.ParseTimestampString(RawValue) ?? throw new ConfigurationException("Invalid timestamp");
        }
    }

    internal class TimestampWrapperConverter : IYamlTypeConverter
    {
        public bool Accepts(Type type)
        {
            return type == typeof(TimestampWrapper);
        }

        public object? ReadYaml(IParser parser, Type type)
        {
            var scalar = parser.Consume<Scalar>();
            return new TimestampWrapper(scalar.Value);
        }

        public void WriteYaml(IEmitter emitter, object? value, Type type)
        {
            var it = value as TimestampWrapper;
            emitter.Emit(new Scalar(AnchorName.Empty, TagName.Empty, it?.RawValue ?? "", ScalarStyle.DoubleQuoted, false, true));
        }
    }
}