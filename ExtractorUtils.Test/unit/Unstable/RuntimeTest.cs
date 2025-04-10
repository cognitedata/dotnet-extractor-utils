using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extractor.Testing;
using Cognite.Extractor.Utils.Unstable.Runtime;
using Cognite.Extractor.Utils.Unstable.Configuration;
using ExtractorUtils.Test.Unit.Unstable;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Xunit;
using Xunit.Abstractions;

namespace ExtractorUtils.Test.unit.Unstable
{
    public class RuntimeTest
    {
        private readonly ITestOutputHelper _output;

        public RuntimeTest(ITestOutputHelper output)
        {
            _output = output;
        }

        private string GetConnectionConfig()
        {
            return @"
project: project
base-url: https://api.cognitedata.com
integration: test-integration
authentication:
  type: client-credentials
  client-id: someId
  client-secret: thisIsASecret
  token-url: http://example.url/token
  scopes:
    - https://greenfield.cognitedata.com/.default
";
        }

        [Fact(Timeout = 5000)]
        public async Task TestRuntimeBuilder()
        {
            var builder = new ExtractorRuntimeBuilder<DummyConfig, DummyExtractor>("dotnetutilstest", "utilstest/1");
            builder.ConfigSource = ConfigSourceType.Local;
            var services = new ServiceCollection();
            services.AddTestLogging(_output);
            builder.StartupLogger = services.BuildServiceProvider().GetRequiredService<ILogger<RuntimeTest>>();

            var configPath = TestUtils.AlphaNumericPrefix("dotnet_extractor_test") + "_config";
            builder.ConfigFolder = configPath;
            Directory.CreateDirectory(configPath);
            File.WriteAllText(configPath + "/connection.yml", GetConnectionConfig());

            var runtime = await builder.MakeRuntime(CancellationToken.None);

            var configSource = runtime.GetType().GetField("_configSource", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            Assert.IsType<LocalConfigSource<DummyConfig>>(configSource.GetValue(runtime));

            Directory.Delete(configPath, true);
        }

        [Fact(Timeout = 5000)]
        public async Task TestRuntimeBuilderRemote()
        {
            var builder = new ExtractorRuntimeBuilder<DummyConfig, DummyExtractor>("dotnetutilstest", "utilstest/1");
            builder.ConfigSource = ConfigSourceType.Remote;
            var services = new ServiceCollection();
            services.AddTestLogging(_output);
            builder.StartupLogger = services.BuildServiceProvider().GetRequiredService<ILogger<RuntimeTest>>();

            var configPath = TestUtils.AlphaNumericPrefix("dotnet_extractor_test") + "_config";
            builder.ConfigFolder = configPath;
            Directory.CreateDirectory(configPath);
            File.WriteAllText(configPath + "/connection.yml", GetConnectionConfig());

            var runtime = await builder.MakeRuntime(CancellationToken.None);

            var configSource = runtime.GetType().GetField("_configSource", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            Assert.IsType<RemoteConfigSource<DummyConfig>>(configSource.GetValue(runtime));

            Directory.Delete(configPath, true);
        }

        [Fact(Timeout = 5000)]
        public async Task TestRuntimeBuilderStatic()
        {
            var builder = new ExtractorRuntimeBuilder<DummyConfig, DummyExtractor>("dotnetutilstest", "utilstest/1");
            builder.ConfigSource = ConfigSourceType.Remote;
            builder.NoConnection = true;
            builder.ExternalConfig = new DummyConfig();
            var services = new ServiceCollection();
            services.AddTestLogging(_output);
            builder.StartupLogger = services.BuildServiceProvider().GetRequiredService<ILogger<RuntimeTest>>();

            var runtime = await builder.MakeRuntime(CancellationToken.None);

            var configSource = runtime.GetType().GetField("_configSource", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            Assert.IsType<StaticConfigSource<DummyConfig>>(configSource.GetValue(runtime));
        }
    }
}