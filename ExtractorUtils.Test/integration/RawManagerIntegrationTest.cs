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
        [Fact(Timeout = 30000)]
        public async void TestExtractorManagerRun()
        {
            int index = 0;
            string path = "test-extractor-manager-config";
            var config = CDFTester.GetConfig(CogniteHost.BlueField);


            string[] lines = {
                    "manager:",
                    $"  index: {index}",
                    $"  database-name: {_dbName}",
                    $"  table-name: {_tableName}"};

            foreach (string line in lines)
            {
                config = config.Append(line).ToArray();
            }

            foreach (string line in config)
            {

                Console.WriteLine(line);
            }

            System.IO.File.WriteAllLines(path, config);

            using var source = new CancellationTokenSource();



            await ExtractorRunner.Run<MyManagerConfig, MyExtractor>(
                configPath: path,
                acceptedConfigVersions: new[] { 2 },
                appId: "my-extractor",
                userAgent: "myextractor/1.0.0",
                addStateStore: false,
                addLogger: true,
                addMetrics: true,
                restart: true,
                source.Token).ConfigureAwait(false);

            System.IO.File.Delete(path);

        }
    }
}