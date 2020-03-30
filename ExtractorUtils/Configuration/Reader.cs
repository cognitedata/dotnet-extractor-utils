using System;
using System.IO;
using System.Text.RegularExpressions;
using YamlDotNet.Core;
using YamlDotNet.Core.Events;
using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;

namespace ExtractorUtils
{
    /// <summary>
    /// Configuration utility class that uses YamlDotNet to read and deserialize YML documents to extractor config objects.
    /// The standard format for extractor config files uses hyphenated tag names (this-is-a-tag in yml is mapped to ThisIsATag object property).
    /// Values containing ${ENV_VARIABLE} will be replaced by the environment variable of the same name.
    /// </summary>
    public static class Configuration
    {
        private static DeserializerBuilder builder = new DeserializerBuilder()
            .WithNamingConvention(HyphenatedNamingConvention.Instance)
            .WithNodeDeserializer(new TemplatedValueDeserializer());
        private static IDeserializer deserializer = builder
            .Build();

        /// <summary>
        /// Reads the provided string containing yml and deserializes it to an object of type T 
        /// </summary>
        /// <param name="yaml">String containing yml</param>
        /// <typeparam name="T">Type to read to</typeparam>
        /// <returns>Object of type T. YamlDotNet exceptions are returned in case of deserialization errors.</returns>
        public static T ReadString<T>(string yaml)
        {
            return deserializer.Deserialize<T>(yaml);
        }

        /// <summary>
        /// Reads the yml file found in the provided path and deserializes it to an object of type T 
        /// </summary>
        /// <param name="path">String containing the path to a yml file</param>
        /// <typeparam name="T">Type to read to</typeparam>
        /// <returns>Object of type T. YamlDotNet exceptions are returned in case of deserialization errors.</returns>
        public static T Read<T>(string path)
        {
            using (var reader = File.OpenText(path))
            {
                return deserializer.Deserialize<T>(reader);
            }

        }

        /// <summary>
        /// Maps the given tag to the type T.
        /// Mapping is only required for custom tags.
        /// </summary>
        /// <param name="tag">Tag to be mapped</param>
        /// <typeparam name="T">Type to map to</typeparam>
        public static void AddTagMapping<T>(string tag) {
            builder = builder.WithTagMapping(tag, typeof(T));
            deserializer = builder.Build();
        }
    }

    internal class TemplatedValueDeserializer : INodeDeserializer
    {
        public TemplatedValueDeserializer()
        {
        }

        bool INodeDeserializer.Deserialize(IParser parser, Type expectedType, Func<IParser, Type, object> nestedObjectDeserializer, out object value)
        {
            if (expectedType != typeof(string) && !expectedType.IsNumericType())
            {
                value = null;
                return false;
            }

            parser.TryConsume<Scalar>(out var scalar);
            if (scalar == null)
            {
                value = null;
                return false;
            }

            value = Regex.Replace(scalar.Value, @"\$\{([A-Za-z0-9_]+)\}", LookupEnvironment);
            return true;
        }

        static string LookupEnvironment(Match match)
        {
            return Environment.GetEnvironmentVariable(match.Groups[1].Value) ?? "";
        }
    }

}