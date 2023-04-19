using Cognite.Extensions;
using Cognite.Extractor.Utils;
using CogniteSdk;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using Xunit;
using Xunit.Abstractions;
using Microsoft.Extensions.DependencyInjection;

namespace ExtractorUtils.Test.Integration
{
    class MyExtractor : BaseExtractor<MyTestConfig>
    {
        public MyExtractor(MyTestConfig config, IServiceProvider provider, CogniteDestination destination, ExtractionRun run)
            : base(config, provider, destination, run)
        {
            if (run != null) run.Continuous = true;
        }

        protected override async Task Start()
        {
            await RunWithHighAvailabilityAndWait(
                Config.HighAvailability,
                interval: new TimeSpan(0, 0, 2),
                inactivityThreshold: new TimeSpan(0, 0, 4)).ConfigureAwait(false);

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

    class MyTestConfig : BaseConfig
    {
        public HighAvailabilityConfig HighAvailability { get; set; }
    }

    public class RawHighAvailabilityTest
    {
        private readonly ITestOutputHelper _output;

        private readonly string _dbName = "test-db-integration";

        private readonly string _tableName = "test-table-integration";

        public RawHighAvailabilityTest(ITestOutputHelper output)
        {
            _output = output;
        }

        [Fact(Timeout = 30000)]
        public async void TestExtractorManagerRun()
        {
            // Creating configs for two different extractors.
            string configPath_0 = SetupConfig(index: 0);
            string configPath_1 = SetupConfig(index: 1);

            using var source_0 = new CancellationTokenSource();
            using var source_1 = new CancellationTokenSource();

            // Creating two extractors.
            Task extractor_0 = CreateExtractor(configPath_0, source_0.Token);
            Task extractor_1 = CreateExtractor(configPath_1, source_1.Token);

            // Turning off extractor 0 after 15s and 1 after 25s.
            Task cancel_0 = TurnOffAfterDelay(15000, source_0);
            Task cancel_1 = TurnOffAfterDelay(25000, source_1);

            try
            {
                // Testing running two extractors at the same, then turning them off one by one.
                // When the first extractor is turned off the second extractor will become active.
                Task tasks = Task.WhenAll(extractor_0, extractor_1, cancel_0, cancel_1);
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

        [Fact(Timeout = 45000)]
        public async void TestRestartExtractor()
        {
            // Creating config for two extractors.
            string configPath_0 = SetupConfig(index: 2);
            string configPath_1 = SetupConfig(index: 3);

            using var source_0 = new CancellationTokenSource();
            using var source_1 = new CancellationTokenSource();
            using var source_2 = new CancellationTokenSource();

            // Creating extractor 0 and 1.
            Task extractor_0 = CreateExtractor(configPath_0, source_0.Token);
            Task extractor_1 = CreateExtractor(configPath_1, source_1.Token);
            // Creating a copy of extractor 0 that will start after 20s.
            Task restart_0 = CreateExtractor(configPath_0, source_2.Token, delay: 20000);

            //Turning off extractor 0 after 10s, 1 after 30s and the restarted 0 after 40s.
            Task cancel_0 = TurnOffAfterDelay(10000, source_0);
            Task cancel_1 = TurnOffAfterDelay(30000, source_1);
            Task cancel_2 = TurnOffAfterDelay(40000, source_2);

            try
            {
                // Running two extractors.
                // When extractor 0 is turned off, extractor 1 will start.
                // Then when extractor 0 is restarted it will go into standby.
                // Lastly when extractor 1 is turned off, 0 will start again.
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

        private string SetupConfig(int index)
        {
            var config = CDFTester.GetConfig(CogniteHost.BlueField);
            string[] lines = {
                        "high-availability:",
                        $"  index: {index}",
                        $"  raw:",
                        $"    database-name: {_dbName}",
                        $"    table-name: {_tableName}"};

            foreach (string line in lines) config = config.Append(line).ToArray();

            string path = $"test-extractor-manager-{index}-config";
            System.IO.File.WriteAllLines(path, config);

            return path;
        }

        private async Task<Task> CreateExtractor(string configPath, CancellationToken token, int delay = 0)
        {
            if (delay > 0) await Task.Delay(delay);

            Task extractor = Task.Run(async () =>
                await ExtractorRunner.Run<MyTestConfig, MyExtractor>(
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

        private async Task DeleteDatabase(string path)
        {
            var services = new ServiceCollection();
            services.AddConfig<MyTestConfig>(path, 2);
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
    }
}