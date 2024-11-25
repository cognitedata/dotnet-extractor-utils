using System;
using System.Collections.Generic;
using System.Linq;
using Cognite.Extractor.KeyVault;
using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;
using YamlDotNet.Serialization.Utilities;

namespace Cognite.Extractor.Configuration
{
    internal class DiscriminatedUnionConfig
    {
        public Type BaseType { get; }
        public string Key { get; }
        public IDictionary<string, Type> Variants { get; }

        public DiscriminatedUnionConfig(Type baseType, string key, IDictionary<string, Type> variants)
        {
            BaseType = baseType;
            Key = key;
            Variants = variants;
        }
    }

    /// <summary>
    /// Custom builder for YamlDotNet deserializers.
    /// 
    /// This handles idempotency better, so that it can be used in a static context.
    /// </summary>
    public class YamlConfigBuilder
    {
        private IDeserializer? _deserializer;

        /// <summary>
        /// Get a deserializer with the current config.
        /// 
        /// This will only rebuild if the config has changed.
        /// </summary>
        public IDeserializer Deserializer
        {
            get
            {
                lock (_changeLock)
                {
                    if (_deserializer == null || _changed)
                    {
                        _deserializer = Build();
                    }
                    _changed = false;
                    return _deserializer;
                }
            }
        }


        private bool _changed;
        private object _changeLock = new object();

        private INamingConvention _namingConvention = HyphenatedNamingConvention.Instance;

        /// <summary>
        /// Current naming convention. Defaults to hyphenated.
        /// </summary>
        public INamingConvention NamingConvention
        {
            get => _namingConvention; set
            {
                lock (_changeLock)
                {
                    _namingConvention = value;
                    _changed = true;
                }

            }
        }
        private List<IYamlTypeConverter> _typeConverters = new List<IYamlTypeConverter> {
            new ListOrStringConverter(),
            new YamlEnumConverter(),
        };

        private Dictionary<string, Type> _tagMappings = new Dictionary<string, Type>
        {
            { "!keyvault", typeof(object) }
        };

        private List<INodeDeserializer> _nodeDeserializers = new List<INodeDeserializer>
        {
            new TemplatedValueDeserializer()
        };
        private bool _ignoreUnmatchedProperties;

        /// <summary>
        /// Whether to ignore unmatched properties in the structure.
        /// </summary>
        public bool IgnoreUnmatchedProperties
        {
            get => _ignoreUnmatchedProperties;
            set
            {
                lock (_changeLock)
                {
                    if (_ignoreUnmatchedProperties != value)
                    {
                        _changed = true;
                        _ignoreUnmatchedProperties = value;
                    }
                }
            }
        }

        private Dictionary<Type, DiscriminatedUnionConfig> _discriminatedUnions = new Dictionary<Type, DiscriminatedUnionConfig>();

        /// <summary>
        /// Maps the given tag to the type T.
        /// Mapping is only required for custom tags.
        /// </summary>
        /// <param name="tag">Tag to be mapped</param>
        /// <typeparam name="T">Type to map to</typeparam>
        public YamlConfigBuilder AddTagMapping<T>(string tag)
        {
            if (tag == null) throw new ArgumentNullException(nameof(tag));
            lock (_changeLock)
            {
                _tagMappings[tag] = typeof(T);
                _changed = true;
            }
            return this;
        }

        /// <summary>
        /// Adds a YAML type converter to the config deserializer.
        /// </summary>
        /// <param name="converter">Type converter to add</param>
        public YamlConfigBuilder AddTypeConverter(IYamlTypeConverter converter)
        {
            if (converter == null) throw new ArgumentNullException(nameof(converter));
            lock (_changeLock)
            {
                _changed = true;
                for (int i = 0; i < _typeConverters.Count; i++)
                {
                    if (_typeConverters[i].GetType() == converter.GetType())
                    {
                        _typeConverters[i] = converter;
                        return this;
                    }
                }
                _typeConverters.Add(converter);
            }
            return this;
        }

        /// <summary>
        /// Add a custom node deserializer to the config.
        /// </summary>
        /// <param name="nodeDeserializer">Node deserializer to add</param>
        public YamlConfigBuilder AddNodeDeserializer(INodeDeserializer nodeDeserializer)
        {
            if (nodeDeserializer == null) throw new ArgumentNullException(nameof(nodeDeserializer));
            lock (_changeLock)
            {
                _changed = true;
                for (int i = 0; i < _nodeDeserializers.Count; i++)
                {
                    if (_nodeDeserializers[i].GetType() == nodeDeserializer.GetType())
                    {
                        _nodeDeserializers[i] = nodeDeserializer;
                        return this;
                    }
                }
                _nodeDeserializers.Add(nodeDeserializer);
            }
            return this;
        }

        /// <summary>
        /// Add key vault support to the config loader, given a key vault config.
        /// </summary>
        /// <param name="config"></param>
        public YamlConfigBuilder AddKeyVault(KeyVaultConfig config)
        {
            if (config == null) throw new ArgumentNullException(nameof(config));

            AddNodeDeserializer(new KeyVaultResolver(config));
            return this;
        }


        /// <summary>
        /// Add an internally tagged type to the yaml deserializer.
        /// </summary>
        /// <typeparam name="TBase">The type in your actual config structure that
        /// indicates that this is a custom mapping.
        /// </typeparam>
        /// <param name="key">The key for the discriminator, i.e. "type"</param>
        /// <param name="variants">A map from discriminator key value to type</param>
        public YamlConfigBuilder AddDiscriminatedType<TBase>(string key, IDictionary<string, Type> variants)
        {
            lock (_changeLock)
            {
                _discriminatedUnions[typeof(TBase)] = new DiscriminatedUnionConfig(typeof(TBase), key, variants);
                _changed = true;
            }
            return this;
        }

        private IDeserializer Build()
        {
            var builder = new DeserializerBuilder()
                .WithNamingConvention(NamingConvention);

            foreach (var tagMapping in _tagMappings)
            {
                builder = builder.WithTagMapping(tagMapping.Key, tagMapping.Value);
            }
            foreach (var deserializer in _nodeDeserializers)
            {
                builder = builder.WithNodeDeserializer(deserializer);
            }
            foreach (var converter in _typeConverters)
            {
                builder = builder.WithTypeConverter(converter);
            }
            if (IgnoreUnmatchedProperties)
            {
                builder = builder.IgnoreUnmatchedProperties();
            }

            if (_discriminatedUnions.Count > 0)
            {
                builder = builder.WithTypeDiscriminatingNodeDeserializer(o =>
                {
                    var method = o.GetType().GetMethod("AddKeyValueTypeDiscriminator");
                    foreach (var union in _discriminatedUnions.Values)
                    {
                        method!.MakeGenericMethod(union.BaseType)
                            .Invoke(o, new object[] { union.Key, union.Variants });
                    }
                });
            }

            return builder.Build();
        }

        /// <summary>
        /// Get a filtered serializer, used for displaying configuration objects
        /// without logging secrets like passwords.
        /// 
        /// Note, avoid using this on objects with cycles.
        /// </summary>
        /// <param name="toAlwaysKeep">List of items to keep even if they match defaults.</param>
        /// <param name="toIgnore">List of field names to ignore. You should put secrets and passwords in here</param>
        /// <param name="namePrefixes">Prefixes on full type names for types that should be explored internally</param>
        /// <param name="allowReadOnly">Allow read only properties</param>
        /// <returns></returns>
        public ISerializer GetSafeSerializer(
            IEnumerable<string> toAlwaysKeep,
            IEnumerable<string> toIgnore,
            IEnumerable<string> namePrefixes,
            bool allowReadOnly)
        {
            lock (_changeLock)
            {
                var builder = new SerializerBuilder()
                    .WithTypeInspector(insp => new DefaultFilterTypeInspector(
                        insp,
                        toAlwaysKeep,
                        toIgnore,
                        namePrefixes,
                        _typeConverters,
                        allowReadOnly))
                    .WithNamingConvention(NamingConvention);

                foreach (var converter in _typeConverters)
                {
                    builder.WithTypeConverter(converter);
                }

                return builder.Build();
            }
        }
    }


}