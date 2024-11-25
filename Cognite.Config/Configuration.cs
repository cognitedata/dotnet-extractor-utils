using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using Cognite.Extractor.Common;
using Cognite.Extractor.KeyVault;
using Microsoft.Extensions.DependencyInjection;
using YamlDotNet.Core;
using YamlDotNet.Serialization;

namespace Cognite.Extractor.Configuration
{
    /// <summary>
    /// Configuration utility class that uses YamlDotNet to read and deserialize YML documents to extractor config objects.
    /// The standard format for extractor config files uses hyphenated tag names (this-is-a-tag in yml is mapped to ThisIsATag object property).
    /// Values containing ${ENV_VARIABLE} will be replaced by the environment variable of the same name.
    /// </summary>
    public static class ConfigurationUtils
    {
        private static YamlConfigBuilder _builder = new YamlConfigBuilder();

        /// <summary>
        /// Reads the provided string containing yml and deserializes it to an object of type <typeparamref name="T"/>.
        /// Values containing ${ENV_VARIABLE} will be replaced by the environment variable of the same name.
        /// </summary>
        /// <param name="yaml">Yaml string to parse</param>
        /// <param name="ignoreUnmatched">Set to true to ignore unmatched properties</param>
        /// <typeparam name="T">Type to read to</typeparam>
        /// <returns>Object of type <typeparamref name="T"/></returns>
        /// <exception cref="ConfigurationException">Thrown in case of yaml parsing errors, with an inner <see cref="YamlException"/></exception>
        public static T ReadString<T>(string yaml, bool ignoreUnmatched = false)
        {
            try
            {
                _builder.IgnoreUnmatchedProperties = ignoreUnmatched;
                return _builder.Deserializer.Deserialize<T>(yaml);
            }
            catch (YamlException ye)
            {
                throw new ConfigurationException($"Failed to load config string at {ye.Start}: {ye.InnerException?.Message ?? ye.Message}", ye);
            }
        }

        /// <summary>
        /// Reads the yaml file found in the provided path and deserializes it to an object of type <typeparamref name="T"/> 
        /// Values containing ${ENV_VARIABLE} will be replaced by the environment variable of the same name.
        /// </summary>
        /// <param name="path">String containing the path to a yaml file</param>
        /// <param name="ignoreUnmatched">Set to true to ignore unmatched properties</param>
        /// <typeparam name="T">Type to read to</typeparam>
        /// <returns>Object of type <typeparamref name="T"/></returns>
        /// <exception cref="ConfigurationException">Thrown in case of yaml parsing errors, with an inner <see cref="YamlException"/>.
        /// Or in case the config file is not found, with an inner <see cref="FileNotFoundException"/></exception>
        public static T Read<T>(string path, bool? ignoreUnmatched = null)
        {
            try
            {
                using (var reader = File.OpenText(path))
                {
                    _builder.IgnoreUnmatchedProperties = ignoreUnmatched ?? false;
                    return _builder.Deserializer.Deserialize<T>(reader);
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
        /// Values containing ${ENV_VARIABLE} will be replaced by the environment variable of the same name.
        /// </summary>
        /// <param name="yaml">String containing a yaml configuration</param>
        /// <param name="acceptedConfigVersions">Accepted versions</param>
        /// <typeparam name="T">A type that inherits from <see cref="VersionedConfig"/></typeparam>
        /// <returns>A configuration object of type <typeparamref name="T"/></returns>
        /// <exception cref="ConfigurationException">Thrown when the version is not valid or
        /// in case of yaml parsing errors.</exception>
        public static T TryReadConfigFromString<T>(string yaml, params int[]? acceptedConfigVersions) where T : VersionedConfig
        {
            int configVersion = GetVersionFromString(yaml);
            CheckVersion(configVersion, acceptedConfigVersions);

            var config = ReadString<T>(yaml);
            config.GenerateDefaults();
            return config;
        }

        /// <summary>
        /// Try to read a configuration object of type <typeparamref name="T"/> from the yaml file located in
        /// the provided <paramref name="path"/>. Matching the configuration object version with the versions 
        /// provided in <paramref name="acceptedConfigVersions"/>.
        /// Also calls GenerateDefaults() on the retrieved configuration object after reading.
        /// Values containing ${ENV_VARIABLE} will be replaced by the environment variable of the same name.
        /// </summary>
        /// <param name="path">Path to the yaml file</param>
        /// <param name="acceptedConfigVersions">Accepted versions</param>
        /// <typeparam name="T">A type that inherits from <see cref="VersionedConfig"/></typeparam>
        /// <returns>A configuration object of type <typeparamref name="T"/></returns>
        /// <exception cref="ConfigurationException">Thrown when the version is not valid, 
        /// the yaml file is not found or in case of yaml parsing error.</exception>
        public static T TryReadConfigFromFile<T>(string path, params int[]? acceptedConfigVersions) where T : VersionedConfig
        {
            int configVersion = GetVersionFromFile(path);
            CheckVersion(configVersion, acceptedConfigVersions);

            var config = Read<T>(path);
            config.GenerateDefaults();
            return config;
        }

        /// <summary>
        /// Try to read a configuration object of type <typeparamref name="T"/> from the provided <paramref name="yaml"/>
        /// string. Matching the configuration object version with the versions provided in <paramref name="acceptedConfigVersions"/>.
        /// Also calls GenerateDefaults() on the retrieved configuration object after reading.
        /// Values containing ${ENV_VARIABLE} will be replaced by the environment variable of the same name.
        /// </summary>
        /// <param name="yaml">String containing a yaml configuration</param>
        /// <param name="ignoreUnmatched">Set to true to ignore unmatched properties</param>
        /// <param name="acceptedConfigVersions">Accepted versions</param>
        /// <typeparam name="T">A type that inherits from <see cref="VersionedConfig"/></typeparam>
        /// <returns>A configuration object of type <typeparamref name="T"/></returns>
        /// <exception cref="ConfigurationException">Thrown when the version is not valid or
        /// in case of yaml parsing errors.</exception>
        public static T TryReadConfigFromString<T>(string yaml, bool ignoreUnmatched, params int[]? acceptedConfigVersions) where T : VersionedConfig
        {
            int configVersion = GetVersionFromString(yaml);
            CheckVersion(configVersion, acceptedConfigVersions);

            var config = ReadString<T>(yaml, ignoreUnmatched);
            config.GenerateDefaults();
            return config;
        }

        /// <summary>
        /// Try to read a configuration object of type <typeparamref name="T"/> from the yaml file located in
        /// the provided <paramref name="path"/>. Matching the configuration object version with the versions 
        /// provided in <paramref name="acceptedConfigVersions"/>.
        /// Also calls GenerateDefaults() on the retrieved configuration object after reading.
        /// Values containing ${ENV_VARIABLE} will be replaced by the environment variable of the same name.
        /// </summary>
        /// <param name="path">Path to the yaml file</param>
        /// <param name="ignoreUnmatched">Set to true to ignore unmatched properties</param>
        /// <param name="acceptedConfigVersions">Accepted versions</param>
        /// <typeparam name="T">A type that inherits from <see cref="VersionedConfig"/></typeparam>
        /// <returns>A configuration object of type <typeparamref name="T"/></returns>
        /// <exception cref="ConfigurationException">Thrown when the version is not valid, 
        /// the yaml file is not found or in case of yaml parsing error.</exception>
        public static T TryReadConfigFromFile<T>(string path, bool ignoreUnmatched, params int[]? acceptedConfigVersions) where T : VersionedConfig
        {
            int configVersion = GetVersionFromFile(path);
            CheckVersion(configVersion, acceptedConfigVersions);

            var config = Read<T>(path, ignoreUnmatched);
            config.GenerateDefaults();
            return config;
        }

        /// <summary>
        /// Maps the given tag to the type T.
        /// Mapping is only required for custom tags.
        /// </summary>
        /// <param name="tag">Tag to be mapped</param>
        /// <typeparam name="T">Type to map to</typeparam>
        public static void AddTagMapping<T>(string tag)
        {
            _builder.AddTagMapping<T>(tag);
        }

        /// <summary>
        /// Adds a YAML type converter to the config deserializer.
        /// </summary>
        /// <param name="converter">Type converter to add</param>
        public static void AddTypeConverter(IYamlTypeConverter converter)
        {
            _builder.AddTypeConverter(converter);
        }

        /// <summary>
        /// Add an internally tagged type to the yaml deserializer.
        /// </summary>
        /// <typeparam name="TBase">The type in your actual config structure that
        /// indicates that this is a custom mapping.
        /// </typeparam>
        /// <param name="key">The key for the discriminator, i.e. "type"</param>
        /// <param name="variants">A map from discriminator key value to type</param>
        public static void AddDiscriminatedType<TBase>(string key, IDictionary<string, Type> variants)
        {
            _builder.AddDiscriminatedType<TBase>(key, variants);
        }

        /// <summary>
        /// Configures the deserializer to ignore unmatched properties.
        /// </summary>
        public static void IgnoreUnmatchedProperties()
        {
            _builder.IgnoreUnmatchedProperties = true;
        }

        /// <summary>
        /// Configures the deserializer to throw an exception on unmatched properties, this is the default.
        /// </summary>
        public static void DisallowUnmatchedProperties()
        {
            _builder.IgnoreUnmatchedProperties = false;
        }

        /// <summary>
        /// Add key vault support to the config loader, given a key vault config.
        /// </summary>
        /// <param name="config"></param>
        public static void AddKeyVault(KeyVaultConfig config)
        {
            _builder.AddKeyVault(config);
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Maintainability", "CA1508: Avoid dead conditional code", Justification = "Other methods using this can still pass null as parameter")]
        private static void CheckVersion(int version, params int[]? acceptedConfigVersions)
        {
            if (acceptedConfigVersions == null || acceptedConfigVersions.Length == 0)
            {
                return;
            }
            var accept = new List<int>(acceptedConfigVersions);
            if (!accept.Contains(version))
            {
                throw new ConfigurationException($"Config version {version} is not supported by this extractor");
            }
        }

        private static int GetVersion(Dictionary<object, object> versionedConfig)
        {
            if (versionedConfig.TryGetValue("version", out dynamic? version))
            {
                if (int.TryParse(version, out int intVersion))
                {
                    return intVersion;
                }
                throw new ConfigurationException("The value of the 'version' tag should be integer");
            }
            throw new ConfigurationException("The yaml configuration file should contain a 'version' tag");
        }

        /// <summary>
        /// Attempt to add the list of config-object types <paramref name="types"/> to the <see cref="ServiceCollection"/>
        /// by iterating through the public properties of <paramref name="config"/>.
        /// Applies to supertypes of the types given in <paramref name="types"/>. Having multiple candidates
        /// for a type can be unpredictable.
        /// </summary>
        /// <typeparam name="T">Configuration object type</typeparam>
        /// <param name="services">Services to add to</param>
        /// <param name="config">Configuration object to add from</param>
        /// <param name="types">List of types that should be added</param>
        public static void AddConfig<T>(this IServiceCollection services, T? config, params Type[]? types) where T : class
        {
            if (types == null || !types.Any() || config is null) return;
            foreach (var type in types)
            {
                if (type.IsAssignableFrom(typeof(T)))
                {
                    services.AddSingleton(typeof(T), config);
                    break;
                }
            }

            foreach (var prop in typeof(T).GetProperties(BindingFlags.Instance | BindingFlags.Public))
            {
                if (!prop.CanRead) continue;
                foreach (var type in types)
                {
                    if (type.IsAssignableFrom(prop.PropertyType))
                    {
                        var value = prop.GetValue(config);
                        if (value is null) break;
                        services.AddSingleton(type, value);
                        break;
                    }
                }
            }
        }

        /// <summary>
        /// Convert an object to yaml, comparing it to a new instance of itself.
        /// Note that this should not be used on objects with cycles.
        /// This creates a new instance of <typeparamref name="T"/>, calls "GenerateDefaults" on it, if it exists,
        /// then compares each field in the default and <paramref name="config"/>.
        /// The generated yaml is cleaned and trimmed to ensure pretty results.
        /// </summary>
        /// <typeparam name="T">Type to serialize</typeparam>
        /// <param name="config">Object to serialize</param>
        /// <param name="toAlwaysKeep">List of items to keep even if they match defaults.</param>
        /// <param name="toIgnore">List of field names to ignore. You should put secrets and passwords in here</param>
        /// <param name="namePrefixes">Prefixes on full type names for types that should be explored internally</param>
        /// <param name="allowReadOnly">Allow read only properties</param>
        /// <returns>Printable serialized object</returns>
        public static string ConfigToString<T>(
            T config,
            IEnumerable<string> toAlwaysKeep,
            IEnumerable<string> toIgnore,
            IEnumerable<string> namePrefixes,
            bool allowReadOnly)
        {
            if (config is null) return "";

            var serializer = _builder.GetSafeSerializer(toAlwaysKeep, toIgnore, namePrefixes, allowReadOnly);

            string raw = serializer.Serialize(config);

            return TrimConfigString(raw);
        }

        /// <summary>
        /// Used for trimming useless elements from config written to yaml, to ensure nice and readable results.
        /// </summary>
        /// <param name="raw">Raw input string</param>
        /// <returns>Formatted config string</returns>
        public static string TrimConfigString(string raw)
        {
            var clearEmptyRegex = new Regex("^\\s*[a-zA-Z-_\\d]*:\\s*({}|\\[\\])\\s*\n", RegexOptions.Multiline);
            var doubleIndentRegex = new Regex("(^ +)", RegexOptions.Multiline);
            var fixListIndentRegex = new Regex("(^ +-)", RegexOptions.Multiline);

            raw = clearEmptyRegex.Replace(raw, "");
            raw = doubleIndentRegex.Replace(raw, "$1$1");
            raw = fixListIndentRegex.Replace(raw, "  $1");

            return raw;
        }
    }
}
