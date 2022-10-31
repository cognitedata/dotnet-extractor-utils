using Cognite.Extractor.Common;
using Cognite.Extractor.Configuration;
using Cognite.Extractor.Logging;
using Cognite.Extractor.Utils;
using CogniteSdk;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace ExtractorUtils.Test.Integration
{
    public class RemoteConfigTest
    {
        private readonly ITestOutputHelper _output;
        public RemoteConfigTest(ITestOutputHelper output)
        {
            _output = output;
        }

        private async Task<ExtPipe> Setup(CDFTester tester)
        {
            var pipeline = (await tester.Destination.CogniteClient.ExtPipes.CreateAsync(new[] { new ExtPipeCreate
            {
                DataSetId = await tester.GetDataSetId(),
                ExternalId = $"{tester.Prefix}-config-extpipe-1",
                Name = "Config test extpipe"
            } })).First();
            return pipeline;
        }

        private void WriteConfig(CogniteHost host, bool remote, string path, string pipelineId)
        {
            var lines = CDFTester.GetConfig(host, true);
            lines = lines.Prepend($"type: {(remote ? "remote" : "local")}").Append("  extraction-pipeline:").Append($"    external-id: {pipelineId}").ToArray();
            System.IO.File.WriteAllLines(path, lines);
        }

        [Theory]
        // [InlineData(CogniteHost.GreenField)]
        [InlineData(CogniteHost.BlueField)]
        public async Task TestRemoteConfigDI(CogniteHost host)
        {
            var tester = new CDFTester(host, _output);
            var localConfigPath = $"{tester.Prefix}-local-config.yml";
            var remoteConfigPath = $"{tester.Prefix}-remote-config.yml";
            var pipeline = await Setup(tester);
            WriteConfig(host, false, localConfigPath, pipeline.ExternalId);
            WriteConfig(host, true, remoteConfigPath, pipeline.ExternalId);
            try
            {
                var services = new ServiceCollection();
                // Should just load local config
                var config = await services.AddRemoteConfig<BaseConfig>(null, localConfigPath, null, "utils-test-app", null, true, false, null, tester.Source.Token, 2);
                Assert.Equal(tester.Config.Cognite.Project, config.Cognite.Project);
                var provider = services.BuildServiceProvider();
                Assert.Null(provider.GetService<RemoteConfigManager<BaseConfig>>());

                // Should fail because there is no remote config
                services = new ServiceCollection();
                await Assert.ThrowsAsync<ConfigurationException>(async () => await services.AddRemoteConfig<BaseConfig>(null, remoteConfigPath, null, "utils-test-app", null, true, false, null, tester.Source.Token, 2));

                // Create a remote config, then try again
                await tester.Destination.CogniteClient.ExtPipes.CreateConfigAsync(new ExtPipeConfigCreate
                {
                    Config = "{\"version\": 2, \"logger\": { \"console\": {\"level\": \"verbose\"}}}",
                    ExternalId = pipeline.ExternalId
                });
                services = new ServiceCollection();
                config = await services.AddRemoteConfig<BaseConfig>(null, remoteConfigPath, null, "utils-test-app", null, true, false, null, tester.Source.Token, 2);
                Assert.Equal("verbose", config.Logger.Console.Level);
                Assert.Equal(tester.Config.Cognite.Project, config.Cognite.Project);

                // Pass the remote config in, this could be useful for creating extractors that just always pull configuration from env
                var localCognite = new CogniteConfig
                {
                    ApiKey = tester.Config.Cognite.ApiKey,
                    Project = tester.Config.Cognite.Project,
                    IdpAuthentication = tester.Config.Cognite.IdpAuthentication,
                    Host = tester.Config.Cognite.Host,
                    ExtractionPipeline = new ExtractionRunConfig
                    {
                        PipelineId = pipeline.ExternalId
                    }
                };
                services = new ServiceCollection();
                config = await services.AddRemoteConfig<BaseConfig>(null, null, null, "utils-test-app", null, true, false, new RemoteConfig
                {
                    Cognite = localCognite,
                    Version = 2,
                    Type = ConfigurationMode.Remote
                }, tester.Source.Token, 2);
                Assert.Equal("verbose", config.Logger.Console.Level);
                Assert.Equal(tester.Config.Cognite.Project, config.Cognite.Project);

                // Pass the remote config in as local, but no path, which should fail
                
                services = new ServiceCollection();
                await Assert.ThrowsAsync<ConfigurationException>(async () => await services.AddRemoteConfig<BaseConfig>(null, null, null, "utils-test-app", null, true, false, new RemoteConfig
                {
                    Cognite = localCognite,
                    Version = 2,
                    Type = ConfigurationMode.Local
                }, tester.Source.Token, 2));

                // Try again, this time passing path
                services = new ServiceCollection();
                config = await services.AddRemoteConfig<BaseConfig>(null, localConfigPath, null, "utils-test-app", null, true, false, new RemoteConfig
                {
                    Cognite = localCognite,
                    Version = 2,
                    Type = ConfigurationMode.Local
                }, tester.Source.Token, 2);
                Assert.Null(config.Logger.Console);
                Assert.Equal(tester.Config.Cognite.Project, config.Cognite.Project);
            }
            finally
            {
                await tester.Destination.CogniteClient.ExtPipes.DeleteAsync(new[] { pipeline.Id });
                System.IO.File.Delete(localConfigPath);
                System.IO.File.Delete(remoteConfigPath);
            }
        }

        [Theory]
        // [InlineData(CogniteHost.GreenField)]
        [InlineData(CogniteHost.BlueField)]
        public async Task TestRemoteConfigManager(CogniteHost host)
        {
            var tester = new CDFTester(host, _output);
            var remoteConfigPath = $"{tester.Prefix}-remote-config.yml";
            var pipeline = await Setup(tester);
            try
            {
                var localCognite = new CogniteConfig
                {
                    ApiKey = tester.Config.Cognite.ApiKey,
                    Project = tester.Config.Cognite.Project,
                    IdpAuthentication = tester.Config.Cognite.IdpAuthentication,
                    Host = tester.Config.Cognite.Host,
                    ExtractionPipeline = new ExtractionRunConfig
                    {
                        PipelineId = pipeline.ExternalId
                    }
                };
                var manager = new RemoteConfigManager<BaseConfig>(tester.Destination, tester.Logger, new RemoteConfig
                {
                    Cognite = localCognite,
                    Type = ConfigurationMode.Remote,
                    Version = 2
                }, new RemoteConfigState<BaseConfig>(), remoteConfigPath, false, new[] { 2 });

                // Try fetching, it should fail, and we should get null
                Assert.Null(await manager.FetchLatest(tester.Source.Token));

                // Again, this time successfully
                await tester.Destination.CogniteClient.ExtPipes.CreateConfigAsync(new ExtPipeConfigCreate
                {
                    Config = "{\"version\": 2, \"logger\": { \"console\": {\"level\": \"verbose\"}}}",
                    ExternalId = pipeline.ExternalId
                });
                var config = await manager.FetchLatest(tester.Source.Token);
                Assert.Equal("verbose", config.Logger.Console.Level);
                Assert.Equal(tester.Config.Cognite.Project, config.Cognite.Project);

                // Create a manager that writes a buffer file
                manager = new RemoteConfigManager<BaseConfig>(tester.Destination, tester.Logger, new RemoteConfig
                {
                    Cognite = localCognite,
                    Type = ConfigurationMode.Remote,
                    Version = 2
                }, new RemoteConfigState<BaseConfig>(), remoteConfigPath, true, new[] { 2 });
                await manager.FetchLatest(tester.Source.Token);
                Assert.True(System.IO.File.Exists($"_temp_{remoteConfigPath}"));

                // Again, this time with bad pipeline id
                localCognite.ExtractionPipeline.PipelineId = "doesnotexist";
                manager = new RemoteConfigManager<BaseConfig>(tester.Destination, tester.Logger, new RemoteConfig
                {
                    Cognite = localCognite,
                    Type = ConfigurationMode.Remote,
                    Version = 2
                }, new RemoteConfigState<BaseConfig>(), remoteConfigPath, true, new[] { 2 });
                config = await manager.FetchLatest(tester.Source.Token);
                Assert.Equal("verbose", config.Logger.Console.Level);
                Assert.Equal(tester.Config.Cognite.Project, config.Cognite.Project);
            }
            finally
            {
                await tester.Destination.CogniteClient.ExtPipes.DeleteAsync(new[] { pipeline.Id });
                System.IO.File.Delete($"_temp_{remoteConfigPath}");
            }
        }

        class CogniteSubType : CogniteConfig
        {
            public string ExtraField { get; set; }
        }

        class SubTypeConfig : VersionedConfig
        {
            public CogniteSubType Cognite { get; set; }
            public LoggerConfig Logger { get; set; }

            public override void GenerateDefaults()
            {
            }
        }

        [Theory]
        [InlineData(CogniteHost.BlueField)]
        public async Task TestRemoteConfigSubtype(CogniteHost host)
        {
            var tester = new CDFTester(host, _output);
            var remoteConfigPath = $"{tester.Prefix}-remote-config.yml";
            var pipeline = await Setup(tester);
            WriteConfig(host, true, remoteConfigPath, pipeline.ExternalId);
            try
            {
                var services = new ServiceCollection();
                await tester.Destination.CogniteClient.ExtPipes.CreateConfigAsync(new ExtPipeConfigCreate
                {
                    Config = "{\"version\": 2, \"logger\": { \"console\": {\"level\": \"verbose\"}}}",
                    ExternalId = pipeline.ExternalId
                });
                var config = await services.AddRemoteConfig<SubTypeConfig>(null, remoteConfigPath, null, "utils-test-app", null, true, false, null, tester.Source.Token, 2);
                Assert.NotNull(config.Cognite);
            }
            finally
            {
                await tester.Destination.CogniteClient.ExtPipes.DeleteAsync(new[] { pipeline.Id });
                System.IO.File.Delete($"_temp_{remoteConfigPath}");
            }
        }
    }
}
