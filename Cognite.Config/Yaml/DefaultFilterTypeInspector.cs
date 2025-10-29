using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;
using YamlDotNet.Serialization.TypeInspectors;

namespace Cognite.Extractor.Configuration
{
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
        private readonly IEnumerable<IYamlTypeConverter> _customConverters;
        private readonly bool _allowReadOnly;
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="innerTypeDescriptor">Inner type descriptor</param>
        /// <param name="toAlwaysKeep">Fields to always keep</param>
        /// <param name="toIgnore">Fields to exclude</param>
        /// <param name="namePrefixes">Prefixes on full type names for types that should be explored internally</param>
        /// <param name="customConverters">List of registered custom converters.</param>
        /// <param name="allowReadOnly">Allow read only properties</param>
        public DefaultFilterTypeInspector(
            ITypeInspector innerTypeDescriptor,
            IEnumerable<string> toAlwaysKeep,
            IEnumerable<string> toIgnore,
            IEnumerable<string> namePrefixes,
            IEnumerable<IYamlTypeConverter> customConverters,
            bool allowReadOnly)
        {
            _innerTypeDescriptor = innerTypeDescriptor;
            _toAlwaysKeep = new HashSet<string>(toAlwaysKeep);
            _toIgnore = new HashSet<string>(toIgnore);
            _namePrefixes = namePrefixes;
            _allowReadOnly = allowReadOnly;
            _customConverters = customConverters;
        }

        private bool ShouldIgnoreForSecurity(string name)
        {
            var lowerName = name.ToLowerInvariant();
            return lowerName.Contains("password")
                || lowerName.Contains("secret")
                || lowerName.Contains("connectionstring");
        }

        public override string GetEnumName(Type enumType, string name)
        {
            return _innerTypeDescriptor.GetEnumName(enumType, name);
        }

        public override string GetEnumValue(object enumValue)
        {
            return _innerTypeDescriptor.GetEnumValue(enumValue);
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
                // Security-sensitive properties should be ignored
                // We put this after toAlwaysKeep, so that users can force keeping something
                // that looks like a secret but isn't sensitive. In that case it would clearly
                // be deliberate.
                // These should ideally be listed in toIgnore instead,
                // but this serves as a safety net.
                if (ShouldIgnoreForSecurity(name)) return false;

                var prop = type.GetProperty(name);
                object? df = null;
                if (dfs != null) df = prop?.GetValue(dfs);
                var val = prop?.GetValue(container);

                if (val != null && prop != null && !type.IsValueType
                    && _namePrefixes.Any(prefix => prop.PropertyType.FullName!.StartsWith(prefix, StringComparison.InvariantCulture))
                    // Any type covered by a custom converter shouldn't be passed through here. We don't know
                    // how those are serialized, it is likely not just by listing their properties.
                    && _customConverters.All(conv => !conv.Accepts(prop.PropertyType)))
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
}
