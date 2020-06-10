using System;
using System.Collections.Generic;
using System.IO;
using System.Text.RegularExpressions;
using YamlDotNet.Core;
using YamlDotNet.Core.Events;
using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;

namespace Cognite.Extractor.Configuration
{
    /// <summary>
    /// Configuration utility class that uses YamlDotNet to read and deserialize YML documents to extractor config objects.
    /// The standard format for extractor config files uses hyphenated tag names (this-is-a-tag in yml is mapped to ThisIsATag object property).
    /// Values containing ${ENV_VARIABLE} will be replaced by the environment variable of the same name.
    /// </summary>
    public static class ConfigurationUtils
    {
        private static DeserializerBuilder builder = new DeserializerBuilder()
            .WithNamingConvention(HyphenatedNamingConvention.Instance)
            .WithNodeDeserializer(new TemplatedValueDeserializer());
        private static IDeserializer deserializer = builder
            .Build();

        /// <summary>
        /// Reads the provided string containing yml and deserializes it to an object of type <typeparamref name="T"/>
        /// </summary>
        /// <param name="yaml">String containing yml</param>
        /// <typeparam name="T">Type to read to</typeparam>
        /// <returns>Object of type <typeparamref name="T"/></returns>
        /// <exception cref="ConfigurationException">Thrown in case of yaml parsing errors, with an inner <see cref="YamlException"/></exception>
        public static T ReadString<T>(string yaml)
        {
            try
            {
                return deserializer.Deserialize<T>(yaml);
            }
            catch (YamlDotNet.Core.YamlException ye)
            {
                throw new ConfigurationException($"Failed to load config string at {ye.Start}: {ye.InnerException?.Message ?? ye.Message}", ye);
            }
        }

        /// <summary>
        /// Reads the yml file found in the provided path and deserializes it to an object of type <typeparamref name="T"/> 
        /// </summary>
        /// <param name="path">String containing the path to a yml file</param>
        /// <typeparam name="T">Type to read to</typeparam>
        /// <returns>Object of type <typeparamref name="T"/></returns>
        /// <exception cref="ConfigurationException">Thrown in case of yaml parsing errors, with an inner <see cref="YamlException"/>.
        /// Or in case the config file is not found, with an inner <see cref="FileNotFoundException"/></exception>
        public static T Read<T>(string path)
        {
            try
            {
                using (var reader = File.OpenText(path))
                {
                    return deserializer.Deserialize<T>(reader);
                }
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

        /// <summary>
        /// Read the the integer value of the <c>version</c> tag from the yaml file in the provided <paramref name="path"/>
        /// </summary>
        /// <param name="path">Path to the config yml file</param>
        /// <returns>Version</returns>        
        /// <exception cref="ConfigurationException">Thrown when the file is not found, version tag is 
        /// not found or is not of the integer type.</exception>
        public static int GetVersionFromFile(string path)
        {
            Dictionary<object, object> versionedConfig = ConfigurationUtils.Read<dynamic>(path);
            return GetVersion(versionedConfig);
        }

        /// <summary>
        /// Read the the integer value of the <c>version</c> tag from the  provided <paramref name="yaml"/> string
        /// </summary>
        /// <param name="yaml">String containing a yaml configuration</param>
        /// <returns>Version</returns>
        /// <exception cref="ConfigurationException">Thrown when the version tag is 
        /// not found or is not of the integer type.</exception>
        public static int GetVersionFromString(string yaml)
        {
            Dictionary<object, object> versionedConfig = ConfigurationUtils.ReadString<dynamic>(yaml);
            return GetVersion(versionedConfig);
        }

        /// <summary>
        /// Try to read a configuration object of type <typeparamref name="T"/> from the provided <paramref name="yaml"/>
        /// string. Matching the configuration object version with the versions provided in <paramref name="acceptedConfigVersions"/>.
        /// Also calls GenerateDefaults() on the retrieved configuration object after reading.
        /// </summary>
        /// <param name="yaml">String containing a yaml configuration</param>
        /// <param name="acceptedConfigVersions">Accepted versions</param>
        /// <typeparam name="T">A type that inherits from <see cref="VersionedConfig"/></typeparam>
        /// <returns>A configuration object of type <typeparamref name="T"/></returns>
        /// <exception cref="ConfigurationException">Thrown when the version is not valid or
        /// in case of yaml parsing errors.</exception>
        public static T TryReadConfigFromString<T>(string yaml, params int[] acceptedConfigVersions) where T : VersionedConfig
        {
            int configVersion = ConfigurationUtils.GetVersionFromString(yaml);
            CheckVersion(configVersion, acceptedConfigVersions);

            var config = ConfigurationUtils.ReadString<T>(yaml);
            config.GenerateDefaults();
            return config;
        }

        /// <summary>
        /// Try to read a configuration object of type <typeparamref name="T"/> from the yaml file located in
        /// the provided <paramref name="path"/>. Matching the configuration object version with the versions 
        /// provided in <paramref name="acceptedConfigVersions"/>.
        /// Also calls GenerateDefaults() on the retrieved configuration object after reading.
        /// </summary>
        /// <param name="path">Path to the yml file</param>
        /// <param name="acceptedConfigVersions">Accepted versions</param>
        /// <typeparam name="T">A type that inherits from <see cref="VersionedConfig"/></typeparam>
        /// <returns>A configuration object of type <typeparamref name="T"/></returns>
        /// <exception cref="ConfigurationException">Thrown when the version is not valid, 
        /// the yaml file is not found or in case of yaml parsing error.</exception>
        public static T TryReadConfigFromFile<T>(string path, params int[] acceptedConfigVersions) where T : VersionedConfig
        {
            int configVersion = ConfigurationUtils.GetVersionFromFile(path);
            CheckVersion(configVersion, acceptedConfigVersions);

            var config = ConfigurationUtils.Read<T>(path);
            config.GenerateDefaults();
            return config;
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

        private static void CheckVersion(int version, params int[] acceptedConfigVersions) {
            if (acceptedConfigVersions == null || acceptedConfigVersions.Length == 0)
            {
                return;
            }
            var accept = new List<int>(acceptedConfigVersions);
            if (!accept.Contains(version)) {
                throw new ConfigurationException($"Config version {version} is not supported by this extractor");
            }
        }

        private static int GetVersion(Dictionary<object, object> versionedConfig)
        {
            if (versionedConfig.TryGetValue("version", out dynamic version)) {
                if (int.TryParse(version, out int intVersion))
                {
                    return intVersion;
                }
                throw new ConfigurationException("The value of the 'version' tag should be integer");
            }
            throw new ConfigurationException("The yaml configuration file should contain a 'version' tag");
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
