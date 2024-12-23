using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using Cognite.Common;
using Cognite.Extractor.Common;
using Cognite.Extractor.Configuration;
using Cognite.Extractor.Logging;
using Cognite.Extractor.Metrics;
using Cognite.Extractor.StateStorage;
using Cognite.Extractor.Utils;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace ExtractorUtils.Test.Unit
{
#pragma warning disable CA1812
    class TestConfig : VersionedConfig
#pragma warning restore CA1812
    {
        public string Foo { get; set; } = "";
        public int Bar { get; set; }
        public string Baz { get; set; }

        public override void GenerateDefaults()
        {
        }
    }

    class TestBaseConfig : BaseConfig
    {
        public string Foo { get; set; }

        public string Bar { get; set; }

        public override void GenerateDefaults()
        {
            base.GenerateDefaults();
            if (Foo == null) Foo = "";
            if (Bar == null) Bar = "default";
        }
    }

    enum TestEnum
    {
        [EnumMember(Value = "not-foo-at-all")]
        Foo,
        Bar,
    }

    public static class ConfigurationTest
    {
        [Fact]
        public static void ParseNormal()
        {
            string yaml = "foo: bar";
            TestConfig x = ConfigurationUtils.ReadString<TestConfig>(yaml);
            Assert.Equal("bar", x.Foo);
        }

        [Fact]
        public static void ParseNull()
        {
            var nullValues = new List<string>() { "null", "NULL", "Null", "", "~" };
            var assertions = nullValues.Select<string, Action>((value, _) => AssertNull(value)).ToArray();
            Assert.Multiple(assertions);
        }

        private static Action AssertNull(string value)
        {
            return () =>
            {
                TestConfig x = ConfigurationUtils.ReadString<TestConfig>($"foo: {value}");
                Assert.Null(x.Foo);
            };
        }

        [Fact]
        public static void ParseReplaceInt()
        {
            string value = "23";
            Environment.SetEnvironmentVariable("TEST_CONFIG_REPLACE", value);
            string yaml = @"bar: ${TEST_CONFIG_REPLACE}";
            TestConfig x = ConfigurationUtils.ReadString<TestConfig>(yaml);
            Assert.Equal(23, x.Bar);
        }

        [Fact]
        public static void ParseReplaceString()
        {
            string value = "foo and bar";
            Environment.SetEnvironmentVariable("TEST_CONFIG_REPLACE", value);
            string yaml = @"foo: ${TEST_CONFIG_REPLACE}";
            TestConfig x = ConfigurationUtils.ReadString<TestConfig>(yaml);
            Assert.Equal(value, x.Foo);
        }

        [Fact]
        public static void ParseReplaceStringQuoted()
        {
            string value = "foo and bar";
            Environment.SetEnvironmentVariable("TEST_CONFIG_REPLACE", value);
            string yaml = @"foo: '${TEST_CONFIG_REPLACE}'";
            TestConfig x = ConfigurationUtils.ReadString<TestConfig>(yaml);
            Assert.Equal(value, x.Foo);
        }

        [Fact]
        public static void ParseSubstitute()
        {
            Environment.SetEnvironmentVariable("TEST_CONFIG_SUBSTITUTE", "bar");
            string yaml = @"foo: 'more ${TEST_CONFIG_SUBSTITUTE} here'";
            TestConfig x = ConfigurationUtils.ReadString<TestConfig>(yaml);
            Assert.Equal("more bar here", x.Foo);
        }

        [Fact]
        public static void ParseInjection()
        {
            Environment.SetEnvironmentVariable("TEST_CONFIG_SUBSTITUTE", "bar: too");
            string yaml = @"foo: 'more ${TEST_CONFIG_SUBSTITUTE} here'";
            TestConfig x = ConfigurationUtils.ReadString<TestConfig>(yaml);
            Assert.Equal("more bar: too here", x.Foo);
        }

        [Fact]
        public static void TagMapping()
        {
            string yaml = $"!test{Environment.NewLine}foo: bar{Environment.NewLine}bar: 1";
            ConfigurationUtils.AddTagMapping<TestConfig>("!test");
            TestConfig x = ConfigurationUtils.ReadString<TestConfig>(yaml);
            Assert.Equal("bar", x.Foo);
            Assert.Equal(1, x.Bar);
        }

        [Fact]
        public static void BaseConfig()
        {
            string yaml = "version: 2";
            BaseConfig config = ConfigurationUtils.ReadString<BaseConfig>(yaml);
            Assert.Equal(2, config.Version);
        }

        [Fact]
        public static void ReadFromFile()
        {
            string path = "test-file-config.yml";
            string[] lines = { "version: 1", "foo: bar" };
            File.WriteAllLines(path, lines);
            var config = ConfigurationUtils.Read<TestBaseConfig>(path);
            Assert.Equal(1, config.Version);
            Assert.Equal("bar", config.Foo);
            File.Delete(path);
        }

        [Fact]
        public static void TestInvalidFile()
        {
            ConfigurationUtils.DisallowUnmatchedProperties();
            var e = Assert.Throws<ConfigurationException>(() => ConfigurationUtils.TryReadConfigFromFile<TestBaseConfig>("./invalid", 0));
            Assert.IsType<FileNotFoundException>(e.InnerException);
        }


        [Theory]
        [InlineData("version: 0\nfooo: foo")]
        [InlineData("version: 0\ncognite: foo")]
        public static void TestInvalidString(string yaml)
        {
            ConfigurationUtils.DisallowUnmatchedProperties();
            var e = Assert.Throws<ConfigurationException>(() => ConfigurationUtils.TryReadConfigFromString<TestBaseConfig>(yaml, 0));
            Assert.IsType<YamlDotNet.Core.YamlException>(e.InnerException);
        }

        [Fact]
        public static void TestCleanException()
        {
            var yaml = "version: 0\nbad: foo";
            var e = Assert.Throws<ConfigurationException>(() => ConfigurationUtils.TryReadConfigFromString<TestBaseConfig>(yaml, 0));
            var ye = Assert.IsType<YamlDotNet.Core.YamlException>(e.InnerException);
            Assert.Equal("Failed to load config string at Line: 2, Col: 1, Idx: 11: Property 'bad' not found on type 'ExtractorUtils.Test.Unit.TestBaseConfig'.", e.Message);
        }

        [Fact]
        public static void TestValidString()
        {
            var yaml = "version: 0\ncognite: \n  project: project\nlogger:\n  console:\n    level: debug";
            var conf = ConfigurationUtils.TryReadConfigFromString<TestBaseConfig>(yaml, 0);
            Assert.NotNull(conf.Cognite);
            Assert.NotNull(conf.Logger);
            Assert.NotNull(conf.Logger.Console);
        }

        [Fact]
        public static void TestEmptyVersion()
        {
            var yaml = "version: 0";
            Assert.Throws<ConfigurationException>(() => ConfigurationUtils.TryReadConfigFromString<VersionedConfig>(yaml, 1));
            var conf = ConfigurationUtils.TryReadConfigFromString<TestBaseConfig>(yaml); // no version specified, accept any version
            Assert.Equal(0, conf.Version);
            var conf2 = ConfigurationUtils.TryReadConfigFromString<TestBaseConfig>(yaml, null); // null input, accept any version
            Assert.Equal(0, conf2.Version);
        }

        [Fact]
        public static void TestGenerateDefaults()
        {
            var yaml = "version: 0";
            var conf = ConfigurationUtils.ReadString<TestBaseConfig>(yaml);
            Assert.Null(conf.Cognite);
            Assert.Null(conf.Logger);
            Assert.Null(conf.Metrics);
            Assert.Null(conf.Foo);
            conf.GenerateDefaults();
            Assert.NotNull(conf.Cognite);
            Assert.NotNull(conf.Logger);
            Assert.NotNull(conf.Metrics);
            Assert.Equal("", conf.Foo);
            AssemblyTraitAttribute.Equals("default", conf.Bar);
        }

        [Fact]
        public static void InjectConfiguration()
        {
            string path = "test-inject-config.yml";
            string[] lines = { "version: 2", "newfoo: bar" };

            string path1 = "test-inject-config1.yml";
            string[] lines1 = { "version: \"2.0\"", "newfoo: bar" };

            string path2 = "test-inject-config2.yml";
            string[] lines2 = { "ver: 2", "newfoo: bar" };

            string path3 = "test-inject-config3.yml";
            string[] lines3 = { "version: 2", "foo: bar" };

            File.WriteAllLines(path, lines);
            File.WriteAllLines(path1, lines1);
            File.WriteAllLines(path2, lines2);
            File.WriteAllLines(path3, lines3);

            var services = new ServiceCollection();
            var ex = Assert.Throws<ConfigurationException>(() => services.AddConfig<TestBaseConfig>(path, 1));
            Assert.Contains("version 2 is not supported", ex.Message);

            ex = Assert.Throws<ConfigurationException>(() => services.AddConfig<TestBaseConfig>(path1, 1));
            Assert.Contains("tag should be integer", ex.Message);

            ex = Assert.Throws<ConfigurationException>(() => services.AddConfig<TestBaseConfig>(path2, 1));
            Assert.Contains("should contain a 'version' tag", ex.Message);

            services.AddConfig<TestBaseConfig>(path3, 1, 2);
            using (var provider = services.BuildServiceProvider())
            {
                var config = provider.GetRequiredService<TestBaseConfig>();
                Assert.Equal(2, config.Version);
                Assert.Equal("bar", config.Foo);
                Assert.Equal("default", config.Bar);
                Assert.NotNull(provider.GetService<CogniteConfig>());
                Assert.NotNull(provider.GetService<LoggerConfig>());
                Assert.NotNull(provider.GetService<MetricsConfig>());
            }
            File.Delete(path);
            File.Delete(path1);
            File.Delete(path2);
            File.Delete(path3);
        }

        public class ExtendedCogniteConfig : CogniteConfig
        {
            public int DataSetId { get; set; }
        }

        public class CustomConfig
        {
            public string SomeValue { get; set; }
        }

        public class ExtendedConfig : VersionedConfig
        {
            public LoggerConfig Logger { get; set; }

            public ExtendedCogniteConfig Cognite { get; set; }

            public StateStoreConfig StateStore { get; set; }

            public CustomConfig CustomConfig { get; set; }
            public override void GenerateDefaults()
            {
                Logger = Logger ?? new LoggerConfig();
            }
        }

        [Fact]
        public static void TestExtendedConfiguration()
        {
            string path = "test-custom-config.yml";
            string[] lines = {  "version: 2",
                                "logger:",
                                "  console:",
                                "    level: verbose",
                                "cognite:",
                                "  data-set-id: 123",
                                "custom-config:",
                                "  some-value: value" };
            var services = new ServiceCollection();

            File.WriteAllLines(path, lines);

            var config = services.AddConfig<ExtendedConfig>(path, 2);
            services.AddConfig(config, typeof(CustomConfig));
            using var provider = services.BuildServiceProvider();

            var cogniteConfig = provider.GetRequiredService<CogniteConfig>();
            var loggerConfig = provider.GetRequiredService<LoggerConfig>();
            var customConfig = provider.GetRequiredService<CustomConfig>();

            Assert.NotNull(cogniteConfig);
            Assert.NotNull(loggerConfig);
            Assert.NotNull(customConfig);
            Assert.Equal(123, (cogniteConfig as ExtendedCogniteConfig).DataSetId);
            Assert.Equal("value", customConfig.SomeValue);

            File.Delete(path);
        }

        class TestListOrSpaceSep
        {
            public ListOrSpaceSeparated Foo { get; set; }
            public ListOrSpaceSeparated Bar { get; set; }
            public ListOrSpaceSeparated Baz { get; set; }
        }

        [Fact]
        public static void TestListOrSpaceSeparated()
        {
            string input =
@"foo: some space separated strings
bar:
    - some
    - strings
    - in
    - list";
            var res = ConfigurationUtils.ReadString<TestListOrSpaceSep>(input);
            Assert.Equal(4, res.Foo.Values.Length);
            Assert.Equal(4, res.Bar.Values.Length);
            Assert.Equal("list", res.Bar.Values.ElementAt(3));
            Assert.Equal("strings", res.Foo.Values.ElementAt(3));
            string output =
@"foo:
- ""some""
- ""space""
- ""separated""
- ""strings""
bar:
- ""some""
- ""strings""
- ""in""
- ""list""
";
            var outRes = ConfigurationUtils.ConfigToString(res, Enumerable.Empty<string>(), Enumerable.Empty<string>(), Enumerable.Empty<string>(), false);
            Assert.Equal(output, outRes);
        }

        [Fact]
        public static void TestListOrSpaceSeparatedWithEnv()
        {
            string input =
@"foo: ${ENV_1} ${ENV_2}
bar:
- ${ENV_1}
- ${ENV_2}
";
            Environment.SetEnvironmentVariable("ENV_1", "v1");
            Environment.SetEnvironmentVariable("ENV_2", "v2");

            var res = ConfigurationUtils.ReadString<TestListOrSpaceSep>(input);
            Assert.Equal(2, res.Foo.Values.Length);
            Assert.Equal(2, res.Bar.Values.Length);
            Assert.Equal("v1", res.Bar.Values.ElementAt(0));
            Assert.Equal("v2", res.Foo.Values.ElementAt(1));

            Environment.SetEnvironmentVariable("ENV_1", null);
            Environment.SetEnvironmentVariable("ENV_2", null);
        }

        [Fact]
        public static void TestLogConfiguration()
        {
            var config = new ExtendedConfig
            {
                Cognite = new ExtendedCogniteConfig
                {
                    IdpAuthentication = new Cognite.Extensions.AuthenticatorConfig
                    {
                        ClientId = "123",
                        Secret = "321"
                    },
                    Host = "https://api.cognitedata.com"
                },
                CustomConfig = new CustomConfig
                {
                    SomeValue = "Some Value"
                },
                Version = 1
            };
            config.GenerateDefaults();
            var str = ConfigurationUtils.ConfigToString(config, new[] { "DataSetId" }, new[] { "Secret" }, new[] { "Cognite", "ExtractorUtils" }, false);
            Assert.Equal(@"cognite:
    data-set-id: 0
    idp-authentication:
        client-id: 123
custom-config:
    some-value: Some Value
version: 1
", str);
        }

        [Fact]
        public static void TestEnumConversion()
        {
            Assert.Equal(TestEnum.Foo, ConfigurationUtils.ReadString<TestEnum>("not-foo-at-all"));
            Assert.Equal(TestEnum.Foo, ConfigurationUtils.ReadString<TestEnum>("foo"));
            Assert.Equal(TestEnum.Foo, ConfigurationUtils.ReadString<TestEnum>("FOO"));
            Assert.Equal(TestEnum.Foo, ConfigurationUtils.ReadString<TestEnum>("Foo"));
            Assert.Equal(TestEnum.Bar, ConfigurationUtils.ReadString<TestEnum>("bar"));
        }

        class UnionWrapper
        {
            public List<DiscriminatedUnionBase> Items { get; set; }
        }

        abstract class DiscriminatedUnionBase
        {
            public string Type { get; set; }
        }

        class DiscriminatedUnionA : DiscriminatedUnionBase
        {
            public string FieldA { get; set; }
        }

        class DiscriminatedUnionB : DiscriminatedUnionBase
        {
            public string FieldB { get; set; }
        }

        [Fact]
        public static void TestDiscriminatedUnion()
        {
            ConfigurationUtils.AddDiscriminatedType<DiscriminatedUnionBase>("type", new Dictionary<string, Type> {
                { "typeA", typeof(DiscriminatedUnionA) },
                { "typeB", typeof(DiscriminatedUnionB) }
            });

            var val = ConfigurationUtils.ReadString<UnionWrapper>(@"items:
    - type: typeA
      field-a: it's an A!
    - type: typeB
      field-b: it's a B!
    ");
            Assert.Equal(2, val.Items.Count);
            var it = Assert.IsType<DiscriminatedUnionA>(val.Items[0]);
            Assert.Equal("it's an A!", it.FieldA);
            var it2 = Assert.IsType<DiscriminatedUnionB>(val.Items[1]);
            Assert.Equal("it's a B!", it2.FieldB);
        }
    }
}
