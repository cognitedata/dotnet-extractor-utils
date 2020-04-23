using System.IO;
using System;
using Xunit;
using Microsoft.Extensions.DependencyInjection;

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
        public string Foo { get; set; } = "";
    }

    public class ConfigurationTest
    {
        [Fact]
        public static void ParseNormal() 
        {
            string yaml = "foo: bar";
            TestConfig x = Configuration.ReadString<TestConfig>(yaml);
            Assert.Equal("bar", x.Foo);
        }

        [Fact]
        public static void ParseReplaceInt()
        {
            string value = "23";
            Environment.SetEnvironmentVariable("TEST_CONFIG_REPLACE", value);
            string yaml = @"bar: ${TEST_CONFIG_REPLACE}";
            TestConfig x = Configuration.ReadString<TestConfig>(yaml);
            Assert.Equal(23, x.Bar);
        }

        [Fact]
        public static void ParseReplaceString()
        {
            string value = "foo and bar";
            Environment.SetEnvironmentVariable("TEST_CONFIG_REPLACE", value);
            string yaml = @"foo: ${TEST_CONFIG_REPLACE}";
            TestConfig x = Configuration.ReadString<TestConfig>(yaml);
            Assert.Equal(value, x.Foo);
        }

        [Fact]
        public static void ParseReplaceStringQuoted()
        {
            string value = "foo and bar";
            Environment.SetEnvironmentVariable("TEST_CONFIG_REPLACE", value);
            string yaml = @"foo: '${TEST_CONFIG_REPLACE}'";
            TestConfig x = Configuration.ReadString<TestConfig>(yaml);
            Assert.Equal(value, x.Foo);
        }

        [Fact]
        public static void ParseSubstitute()
        {
            Environment.SetEnvironmentVariable("TEST_CONFIG_SUBSTITUTE", "bar");
            string yaml = @"foo: 'more ${TEST_CONFIG_SUBSTITUTE} here'";
            TestConfig x = Configuration.ReadString<TestConfig>(yaml);
            Assert.Equal("more bar here", x.Foo);
        }

        [Fact]
        public static void ParseInjection()
        {
            Environment.SetEnvironmentVariable("TEST_CONFIG_SUBSTITUTE", "bar: too");
            string yaml = @"foo: 'more ${TEST_CONFIG_SUBSTITUTE} here'";
            TestConfig x = Configuration.ReadString<TestConfig>(yaml);
            Assert.Equal("more bar: too here", x.Foo);
        }

        [Fact]
        public static void TagMapping()
        {
            string yaml = $"!test{Environment.NewLine}foo: bar{Environment.NewLine}bar: 1";
            Configuration.AddTagMapping<TestConfig>("!test");
            TestConfig x = Configuration.ReadString<TestConfig>(yaml);
            Assert.Equal("bar", x.Foo);
            Assert.Equal(1, x.Bar);
        }

        [Fact]
        public static void BaseConfig()
        {
            string yaml = "version: 2";
            BaseConfig config = Configuration.ReadString<BaseConfig>(yaml);
            Assert.Equal(2, config.Version);
        }

        [Fact]
        public static void ReadFromFile() {
            string path = "test-file-config.yml";
            string[] lines = { "version: 1", "foo: bar" };
            File.WriteAllLines(path, lines);
            var config = Configuration.Read<TestBaseConfig>(path);
            Assert.Equal(1, config.Version);
            Assert.Equal("bar", config.Foo);
            File.Delete(path);
        }

        [Fact]
        public static void TestInvalidFile()
        {
            var e = Assert.Throws<ConfigurationException>(() => Configuration.TryReadConfigFromFile<TestBaseConfig>("./invalid", 0));
            Assert.IsType<FileNotFoundException>(e.InnerException);
        }


        [Theory]
        [InlineData("version: 0\nfooo: foo")]
        [InlineData("version: 0\ncognite: foo")]
        public static void TestInvalidString(string yaml)
        {
            var e = Assert.Throws<ConfigurationException>(() => Configuration.TryReadConfigFromString<TestBaseConfig>(yaml, 0));
            Assert.IsType<YamlDotNet.Core.YamlException>(e.InnerException);
        }

        [Fact]
        public static void TestValidString()
        {
            var yaml = "version: 0\ncognite: \n  project: project\nlogger:\n  console:\n    level: debug";
            var conf = Configuration.TryReadConfigFromString<TestBaseConfig>(yaml, 0);
            Assert.NotNull(conf.Cognite);
            Assert.NotNull(conf.Logger);
            Assert.NotNull(conf.Logger.Console);
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
                Assert.Null(provider.GetService<CogniteConfig>());
                Assert.Null(provider.GetService<LoggerConfig>());
                Assert.Null(provider.GetService<MetricsConfig>());
            }
            File.Delete(path);
            File.Delete(path1);
            File.Delete(path2);
            File.Delete(path3);
        }
    }
}
