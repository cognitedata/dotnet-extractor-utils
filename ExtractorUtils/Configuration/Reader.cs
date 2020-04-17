using System;
using System.IO;
using System.Text.RegularExpressions;
using Microsoft.Extensions.DependencyInjection;
using YamlDotNet.Core;
using YamlDotNet.Core.Events;
using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;
using System.Collections.Generic;
using Microsoft.CSharp;

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
        /// Get the integer value of the <c>version</c> tag.
        /// </summary>
        /// <param name="path">Path to the config yml file</param>
        /// <returns></returns>        
        /// <exception cref="ConfigurationException">Thrown when the version tag is 
        /// not found or is not of the integer type.</exception>
        public static int GetVersion(string path)
        {
            Dictionary<object, object> versionedConfig = Configuration.Read<dynamic>(path);
            if (versionedConfig.TryGetValue("version", out dynamic version)) {
                if(int.TryParse(version, out int intVersion)) {
                    return intVersion;
                }
                throw new ConfigurationException("The value of the 'version' tag should be integer");
            }
            throw new ConfigurationException("The yaml configuration file should contain a 'version' tag");
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

    /// <summary>
    /// Extension utilities for configuration.
    /// </summary>
    public static class ConfigurationExtensions {
        
        /// <summary>
        /// Read the config of type <typeparamref name="T"/> from the YAML file in <paramref name="path"/>
        /// and adds it as a singleton to the service collection <paramref name="services"/>
        /// </summary>
        /// <param name="services">The service collection</param>
        /// <param name="path">Path to the YAML file</param>
        /// <param name="acceptedConfigVersions">Accepted versions of the config file</param>
        /// <typeparam name="T">A type that inherits from the BaseConfig.</typeparam>
        /// <exception cref="ConfigurationException">Thrown when the version is not valid, 
        /// the config file is not found or in case of YAML parsing error.</exception>
        public static void AddConfig<T>(this IServiceCollection services,
                                        string path,
                                        params int[] acceptedConfigVersions) where T : BaseConfig {
            try {
                // Check config version
                int configVersion = Configuration.GetVersion(path);
                var accept = new List<int>(acceptedConfigVersions);
                if (!accept.Contains(configVersion)) {
                    throw new ConfigurationException($"Config version {configVersion} is not supported by this extractor");
                }

                var config = Configuration.Read<T>(path);
                services.AddSingleton<T>(config);
                services.AddSingleton<BaseConfig>((BaseConfig) config); // Allows it to be resolved as BaseConfig

            }
            catch (System.IO.FileNotFoundException fnfe)
            {
                throw new ConfigurationException($"Config file not found: {path}", fnfe);
            }
            catch (YamlDotNet.Core.YamlException ye)
            {
                throw new ConfigurationException($"Failed to load config at {ye.Start}: {ye.InnerException?.Message ?? ye.Message}", ye);
            }
        }
    }

    /// <summary>
    /// Exception produced by the configuration utils 
    /// </summary>
    public class ConfigurationException : Exception
    {
        /// <summary>
        /// Create a new configuration exception with the given <paramref name="message"/>
        /// </summary>
        /// <param name="message">Exception message</param>
        /// <returns></returns>
        public ConfigurationException(string message) : base(message)
        {
        }

        /// <summary>
        /// Create a new configuration exception with the given <paramref name="message"/>
        /// and containing the given <paramref name="innerException"/>
        /// </summary>
        /// <param name="message">Exception message</param>
        /// <param name="innerException">Inner exception</param>
        /// <returns></returns>
        public ConfigurationException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }

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

        bool INodeDeserializer.Deserialize(IParser parser, Type expectedType, Func<IParser, Type, object> nestedObjectDeserializer, out object value)
        {
            if (expectedType != typeof(string) && !IsNumericType(expectedType))
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