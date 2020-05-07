using System;
using System.IO;
using Microsoft.Extensions.DependencyInjection;
using Xunit;
using Cognite.Extractor.Configuration;
using Cognite.Extractor.Logging;
using Cognite.Extractor.Metrics;
using Cognite.Extractor.Utils;

namespace ExtractorUtils.Test
{
#pragma warning disable CA1812
    class TestConfig
#pragma warning restore CA1812
    {
        public string Foo { get; set; } = "";
        public int Bar { get; set; }
    }

    class TestBaseConfig : BaseConfig {
        public string Foo { get; set; }

        public string Bar { get; set; }

        public override void GenerateDefaults() {
            base.GenerateDefaults();
            if (Foo == null) Foo = "";
            if (Bar == null) Bar = "default";
        }
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
        public static void ReadFromFile() {
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
            var e = Assert.Throws<ConfigurationException>(() => ConfigurationUtils.TryReadConfigFromFile<TestBaseConfig>("./invalid", 0));
            Assert.IsType<FileNotFoundException>(e.InnerException);
        }


        [Theory]
        [InlineData("version: 0\nfooo: foo")]
        [InlineData("version: 0\ncognite: foo")]
        public static void TestInvalidString(string yaml)
        {
            var e = Assert.Throws<ConfigurationException>(() => ConfigurationUtils.TryReadConfigFromString<TestBaseConfig>(yaml, 0));
            Assert.IsType<YamlDotNet.Core.YamlException>(e.InnerException);
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
            var conf = ConfigurationUtils.TryReadConfigFromString<TestBaseConfig>(yaml);
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
        public static void InjectConfiguration() {
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
            using (var provider = services.BuildServiceProvider()) {
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
    }
}
