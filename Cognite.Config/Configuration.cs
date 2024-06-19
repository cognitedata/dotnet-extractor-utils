using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.Serialization;
using System.Text.RegularExpressions;
using Cognite.Common;
using Cognite.Extractor.Common;
using Cognite.Extractor.KeyVault;
using Microsoft.Extensions.DependencyInjection;
using YamlDotNet.Core;
using YamlDotNet.Core.Events;
using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;
using YamlDotNet.Serialization.TypeInspectors;
using YamlDotNet.Serialization.Utilities;

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
            .WithTypeConverter(new ListOrStringConverter())
            .WithTagMapping("!keyvault", typeof(object))
            .WithNodeDeserializer(new TemplatedValueDeserializer())
            .WithTypeConverter(new YamlEnumConverter());
        private static IDeserializer deserializer = builder
            .Build();
        private static DeserializerBuilder ignoreUnmatchedBuilder = new DeserializerBuilder()
            .WithNamingConvention(HyphenatedNamingConvention.Instance)
            .WithNodeDeserializer(new TemplatedValueDeserializer())
            .WithTagMapping("!keyvault", typeof(object))
            .WithTypeConverter(new ListOrStringConverter())
            .WithTypeConverter(new YamlEnumConverter())
            .IgnoreUnmatchedProperties();
        private static IDeserializer ignoreUnmatchedDeserializer = ignoreUnmatchedBuilder.Build();
        private static IDeserializer failOnUnmatchedDeserializer = builder.Build();

        private static readonly List<IYamlTypeConverter> converters = new List<IYamlTypeConverter>() {
            new ListOrStringConverter(),
            new YamlEnumConverter(),
        };

        private static bool ignoreUnmatchedProperties;

        private static object _deserializerLock = new object();

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
                lock (_deserializerLock)
                {
                    return GetDeserializer(ignoreUnmatched).Deserialize<T>(yaml);
                }
            }
            catch (YamlException ye)
            {
                throw new ConfigurationException($"Failed to load config string at {ye.Start}: {ye.InnerException?.Message ?? ye.Message}", ye);
            }
        }

        private static IDeserializer GetDeserializer(bool? ignoreUnmatched)
        {
            if (ignoreUnmatched == null) return deserializer;

            return ignoreUnmatched.Value ? ignoreUnmatchedDeserializer : failOnUnmatchedDeserializer;
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
                    lock (_deserializerLock)
                    {
                        return GetDeserializer(ignoreUnmatched).Deserialize<T>(reader);
                    }
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

        private static void Rebuild()
        {
            ignoreUnmatchedDeserializer = ignoreUnmatchedBuilder.Build();
            failOnUnmatchedDeserializer = builder.Build();

            if (ignoreUnmatchedProperties)
            {
                deserializer = ignoreUnmatchedDeserializer;
            }
            else
            {
                deserializer = failOnUnmatchedDeserializer;
            }
        }

        /// <summary>
        /// Maps the given tag to the type T.
        /// Mapping is only required for custom tags.
        /// </summary>
        /// <param name="tag">Tag to be mapped</param>
        /// <typeparam name="T">Type to map to</typeparam>
        public static void AddTagMapping<T>(string tag)
        {
            lock (_deserializerLock)
            {
                builder = builder.WithTagMapping(tag, typeof(T));
                ignoreUnmatchedBuilder = ignoreUnmatchedBuilder.WithTagMapping(tag, typeof(T));
                Rebuild();
            }
        }

        /// <summary>
        /// Adds a YAML type converter to the config deserializer.
        /// </summary>
        /// <param name="converter">Type converter to add</param>
        public static void AddTypeConverter(IYamlTypeConverter converter)
        {
            lock (_deserializerLock)
            {
                builder = builder.WithTypeConverter(converter);
                ignoreUnmatchedBuilder = ignoreUnmatchedBuilder.WithTypeConverter(converter);
                converters.Add(converter);
                Rebuild();
            }
        }

        /// <summary>
        /// Configures the deserializer to ignore unmatched properties.
        /// </summary>
        public static void IgnoreUnmatchedProperties()
        {
            ignoreUnmatchedProperties = true;
            lock (_deserializerLock)
            {
                Rebuild();
            }
        }

        /// <summary>
        /// Configures the deserializer to throw an exception on unmatched properties, this is the default.
        /// </summary>
        public static void DisallowUnmatchedProperties()
        {
            ignoreUnmatchedProperties = false;
            lock (_deserializerLock)
            {
                Rebuild();
            }
        }

        /// <summary>
        /// Add key vault support to the config loader, given a key vault config.
        /// </summary>
        /// <param name="config"></param>
        public static void AddKeyVault(KeyVaultConfig config)
        {
            if (config == null) throw new ArgumentNullException(nameof(config));

            lock (_deserializerLock)
            {
                config.AddKeyVault(builder);
                config.AddKeyVault(ignoreUnmatchedBuilder);
                Rebuild();
            }
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

            var builder = new SerializerBuilder()
                .WithTypeInspector(insp => new DefaultFilterTypeInspector(
                    insp,
                    toAlwaysKeep,
                    toIgnore,
                    namePrefixes,
                    allowReadOnly))
                .WithNamingConvention(HyphenatedNamingConvention.Instance);

            foreach (var converter in converters)
            {
                builder.WithTypeConverter(converter);
            }

            var serializer = builder.Build();

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

        bool INodeDeserializer.Deserialize(IParser parser, Type expectedType, Func<IParser, Type, object?> nestedObjectDeserializer, out object? value)
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

    /// <summary>
    /// YamlDotNet type inspector, used to filter out default values from the generated config.
    /// Instead of serializing the entire config file, which ends up being complicated and difficult to read,
    /// this just serializes the properties that do not simply equal the default values.
    /// This does sometimes produce empty arrays, but we can strip those later.
    /// </summary>
    internal class DefaultFilterTypeInspector : TypeInspectorSkeleton
    {
        private readonly ITypeInspector _innerTypeDescriptor;
        private readonly HashSet<string> _toAlwaysKeep;
        private readonly HashSet<string> _toIgnore;
        private readonly IEnumerable<string> _namePrefixes;
        private readonly bool _allowReadOnly;
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="innerTypeDescriptor">Inner type descriptor</param>
        /// <param name="toAlwaysKeep">Fields to always keep</param>
        /// <param name="toIgnore">Fields to exclude</param>
        /// <param name="namePrefixes">Prefixes on full type names for types that should be explored internally</param>
        /// <param name="allowReadOnly">Allow read only properties</param>
        public DefaultFilterTypeInspector(
            ITypeInspector innerTypeDescriptor,
            IEnumerable<string> toAlwaysKeep,
            IEnumerable<string> toIgnore,
            IEnumerable<string> namePrefixes,
            bool allowReadOnly)
        {
            _innerTypeDescriptor = innerTypeDescriptor;
            _toAlwaysKeep = new HashSet<string>(toAlwaysKeep);
            _toIgnore = new HashSet<string>(toIgnore);
            _namePrefixes = namePrefixes;
            _allowReadOnly = allowReadOnly;
        }

        /// <inheritdoc />
        public override IEnumerable<IPropertyDescriptor> GetProperties(Type type, object? container)
        {
            if (container is null || type is null) return Enumerable.Empty<IPropertyDescriptor>();
            var props = _innerTypeDescriptor.GetProperties(type, container);

            object? dfs = null;
            try
            {
                dfs = Activator.CreateInstance(type);
                var genD = type.GetMethod("GenerateDefaults");
                genD?.Invoke(dfs, null);
            }
            catch { }

            props = props.Where(p =>
            {
                var name = PascalCaseNamingConvention.Instance.Apply(p.Name);

                // Some config objects have private properties, since this is a write-back of config we shouldn't save those
                if (!p.CanWrite && !_allowReadOnly) return false;
                // Some custom properties are kept on the config object for convenience
                if (_toIgnore.Contains(name)) return false;
                // Some should be kept to encourage users to set them
                if (_toAlwaysKeep.Contains(name)) return true;

                var prop = type.GetProperty(name);
                object? df = null;
                if (dfs != null) df = prop?.GetValue(dfs);
                var val = prop?.GetValue(container);

                if (val != null && prop != null && !type.IsValueType
                    && _namePrefixes.Any(prefix => prop.PropertyType.FullName!.StartsWith(prefix, StringComparison.InvariantCulture)))
                {
                    var pr = GetProperties(prop.PropertyType, val);
                    if (!pr.Any()) return false;
                }


                // No need to emit empty lists.
                if (val != null && (val is IEnumerable list) && !list.GetEnumerator().MoveNext()) return false;

                // Compare the value of each property with its default, and check for empty arrays, don't save those.
                // This creates minimal config files
                return df != null && !df.Equals(val) || df == null && val != null;
            });

            return props;
        }
    }

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
