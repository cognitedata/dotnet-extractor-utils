using Cognite.Extensions;
using Cognite.Extractor.Utils;
using CogniteSdk;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace ExtractorUtils.Test.Integration
{
    class MyManagerConfig : BaseConfig
    {
        public RawManagerConfig Manager { get; set; }
    }

    class MyExtractor : BaseExtractor<MyManagerConfig>
    {
        public MyExtractor(MyManagerConfig config, IServiceProvider provider, CogniteDestination destination, ExtractionRun run)
            : base(config, provider, destination, run)
        {
            if (run != null) run.Continuous = true;
        }

        protected override async Task Start()
        {
            if (Config.Manager != null) await AddHighAvailability(Config.Manager).ConfigureAwait(false);

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

        protected override async Task OnStop()
        {
            if (Config.Manager.DatabaseName != null)
            {
                await Destination.CogniteClient.Raw.DeleteDatabasesAsync(new RawDatabaseDelete
                {
                    Items = new[]
                {
                        new RawDatabase { Name = Config.Manager.DatabaseName }
                    },
                    Recursive = true
                });
            }
        }
    }

    public class RawManagerIntegrationTest
    {
        private readonly ITestOutputHelper _output;

        private const string _dbName = "testDb";
        private const string _tableName = "testTable";
        public RawManagerIntegrationTest(ITestOutputHelper output)
        {
            _output = output;
        }
        [Fact(Timeout = 40000)]
        public async void TestExtractorManagerRun()
        {
            string configPath_0 = SetupConfig(index: 0);
            string configPath_1 = SetupConfig(index: 1);

            using var source_0 = new CancellationTokenSource();
            using var source_1 = new CancellationTokenSource();

            Task extractor_0 = CreateExtractor(configPath_0, source_0.Token);
            Task extractor_1 = CreateExtractor(configPath_1, source_1.Token);

            Task cancel_0 = TurnOffAfterDelay(15000, source_0);
            Task cancel_1 = TurnOffAfterDelay(35000, source_1);

            await Task.WhenAll(extractor_0, extractor_1, cancel_0);

            System.IO.File.Delete(configPath_0);
            System.IO.File.Delete(configPath_1);

        }

        private string SetupConfig(int index)
        {
            var config = CDFTester.GetConfig(CogniteHost.BlueField);

            string path = $"test-extractor-manager-{index}-config";
            string[] lines = {
                    "manager:",
                   $"  index: {index}",
                    "  database-name: ${BF_TEST_DB}",
                    "  table-name: ${BF_TEST_TABLE}"};

            foreach (string line in lines) config = config.Append(line).ToArray();
            System.IO.File.WriteAllLines(path, config);

            return path;
        }

        private Task CreateExtractor(string configPath, CancellationToken token)
        {
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