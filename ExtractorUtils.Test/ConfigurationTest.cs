using System.Collections.Generic;
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
            string path = "test-config.yml";
            string[] lines = { "version: 1", "foo: bar" };
            File.WriteAllLines(path, lines);
            var config = Configuration.Read<TestBaseConfig>(path);
            Assert.Equal(1, config.Version);
            Assert.Equal("bar", config.Foo);
            File.Delete(path);
        }

        [Fact]
        public static void InjectConfiguration() {
            string path = "test-config.yml";
            string[] lines = { "version: 2", "foo: bar" };
            File.WriteAllLines(path, lines);
            var versions = new List<int>() { 1 };

            var services = new ServiceCollection();
            var ex = Assert.Throws<ConfigurationException>(() => services.AddConfig<TestBaseConfig>(path, versions));
            Assert.Contains("version 2 is not supported", ex.Message);

            versions.Add(2);
            services.AddConfig<TestBaseConfig>(path, versions);
            using (var provider = services.BuildServiceProvider()) {
                var config = provider.GetRequiredService<TestBaseConfig>();
                Assert.Equal(2, config.Version);
                Assert.Equal("bar", config.Foo);
            }
            File.Delete(path);
        }
    }
}
