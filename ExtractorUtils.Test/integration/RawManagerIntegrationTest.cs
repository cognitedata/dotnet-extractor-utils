using Cognite.Extensions;
using Cognite.Extractor.Utils;
using CogniteSdk;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;
using Microsoft.Extensions.DependencyInjection;



namespace ExtractorUtils.Test.Integration
{
    class MyExtractor : BaseExtractor<MyManagerConfig>
    {
        public MyExtractor(MyManagerConfig config, IServiceProvider provider, CogniteDestination destination, ExtractionRun run)
            : base(config, provider, destination, run)
        {
            if (run != null) run.Continuous = true;
        }

        protected override async Task Start()
        {
            await AddHighAvailability(Config.Manager, interval: new TimeSpan(0, 0, 2), inactivityThreshold: new TimeSpan(0, 0, 5)).ConfigureAwait(false);

            var result = await Destination.EnsureTimeSeriesExistsAsync(new[]
            {
                new TimeSeriesCreate {
                    ExternalId = "sine-wave",
                    Name = "Sine Wave"
                }
            }, RetryMode.OnError, SanitationMode.Clean, Source.Token).ConfigureAwait(false);
            result.ThrowOnFatal();
            CreateTimeseriesQueue(1000, TimeSpan.FromSeconds(1), null);
            ScheduleDatapointsRun("datapoints", TimeSpan.FromMilliseconds(100), token =>
            {
                var dp = (
                    Identity.Create("sine-wave"),
                    new Datapoint(DateTime.UtcNow, Math.Sin(DateTime.UtcNow.Ticks))
                );
                return Task.FromResult<IEnumerable<(Identity, Datapoint)>>(new[] { dp });
            });
        }
    }

    class MyManagerConfig : BaseConfig
    {
        public RawManagerConfig Manager { get; set; }
    }

    public class RawManagerIntegrationTest
    {
        private readonly ITestOutputHelper _output;
        private readonly string _dbName = "test-db-integration";
        private readonly string _tableName = "test-table-integration";
        public RawManagerIntegrationTest(ITestOutputHelper output)
        {
            _output = output;
        }
        [Fact(Timeout = 55000)]
        public async void TestExtractorManagerRun()
        {
            // Creating configs for three different extractors
            string configPath_0 = SetupConfig(index: 0);
            string configPath_1 = SetupConfig(index: 1);
            string configPath_2 = SetupConfig(index: 2);

            using var source_0 = new CancellationTokenSource();
            using var source_1 = new CancellationTokenSource();
            using var source_2 = new CancellationTokenSource();

            //Creating three extractors
            Task extractor_0 = CreateExtractor(configPath_0, source_0.Token);
            Task extractor_1 = CreateExtractor(configPath_1, source_1.Token);
            Task extractor_2 = CreateExtractor(configPath_2, source_2.Token);

            // Turning off extractor 0 after 15s, 1 after 35s and 2 after 50s
            Task cancel_0 = TurnOffAfterDelay(15000, source_0);
            Task cancel_1 = TurnOffAfterDelay(35000, source_1);
            Task cancel_2 = TurnOffAfterDelay(50000, source_2);

            try
            {
                // Testing running all three extractors at the same, then turning them off one by one
                // When each extractor is turned off the standby extractor with highest priority will start
                Task tasks = Task.WhenAll(extractor_0, extractor_1, extractor_2, cancel_0, cancel_1, cancel_2);
                await tasks;
                Assert.True(tasks.IsCompleted);
            }
            finally
            {
                await DeleteDatabase(configPath_0);

                System.IO.File.Delete(configPath_0);
                System.IO.File.Delete(configPath_1);
                System.IO.File.Delete(configPath_2);
            }
        }

        [Fact(Timeout = 55000)]
        public async void TestRestartExtractor()
        {
            // Creating config for two extractors
            string configPath_0 = SetupConfig(index: 0);
            string configPath_1 = SetupConfig(index: 1);

            using var source_0 = new CancellationTokenSource();
            using var source_1 = new CancellationTokenSource();
            using var source_2 = new CancellationTokenSource();

            // Creating extractor 0 and 1
            Task extractor_0 = CreateExtractor(configPath_0, source_0.Token);
            Task extractor_1 = CreateExtractor(configPath_1, source_1.Token);
            // Creating a copy of extractor 0 that will start after 25s
            Task restart_0 = CreateExtractor(configPath_0, source_2.Token, delay: 25000);

            //Turning off extractor 0 after 15s, 1 after 30s and the restarted 0 after 50s
            Task cancel_0 = TurnOffAfterDelay(15000, source_0);
            Task cancel_1 = TurnOffAfterDelay(30000, source_1);
            Task cancel_2 = TurnOffAfterDelay(50000, source_2);

            try
            {
                // Testing turning off extractor 0 and then restarting it after a delay
                // When extractor 0 is turned off, extractor 1 will start
                // Then when extractor 0 is restarted it will go into standby 
                Task tasks = Task.WhenAll(extractor_0, extractor_1, cancel_0, cancel_1, cancel_2, restart_0);
                await tasks;

                Assert.True(tasks.IsCompleted);
            }
            finally
            {
                await DeleteDatabase(configPath_0);

                System.IO.File.Delete(configPath_0);
                System.IO.File.Delete(configPath_1);
            }
        }

        private async Task DeleteDatabase(string path)
        {
            var services = new ServiceCollection();
            services.AddConfig<MyManagerConfig>(path, 2);
            services.AddCogniteClient("testApp");

            using (var provider = services.BuildServiceProvider())
            {
                var destination = provider.GetRequiredService<CogniteDestination>();
                await destination.CogniteClient.Raw.DeleteDatabasesAsync(new RawDatabaseDelete
                {
                    Items = new[]
                {
                    new RawDatabase { Name = _dbName}
                },
                    Recursive = true
                });
            }
        }
        private string SetupConfig(int index)
        {
            var config = CDFTester.GetConfig(CogniteHost.BlueField);
            string[] lines = {
                        "manager:",
                        $"  index: {index}",
                        $"  database-name: {_dbName}",
                        $"  table-name: {_tableName}"};

            foreach (string line in lines) config = config.Append(line).ToArray();

            string path = $"test-extractor-manager-{index}-config";
            System.IO.File.WriteAllLines(path, config);

            return path;
        }

        private async Task<Task> CreateExtractor(string configPath, CancellationToken token, int delay = 0)
        {
            if (delay > 0) await Task.Delay(delay);

            Task extractor = Task.Run(async () =>
                await ExtractorRunner.Run<MyManagerConfig, MyExtractor>(
                configPath,
                null,
                "test-extractor-manager",
                null,
                false,
                true,
                false,
                false,
                token).ConfigureAwait(false));

            return extractor;
        }

        private Task TurnOffAfterDelay(int delay, CancellationTokenSource source)
        {
            Task cancel = Task.Run(async () =>
            {
                await Task.Delay(delay);
                source.Cancel();
            });

            return cancel;
        }
    }
}