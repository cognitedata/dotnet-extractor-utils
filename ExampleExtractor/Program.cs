using Cognite.Extractor.Utils;
using CogniteSdk;
using System.Threading.Tasks;
using System.Threading;
using Cognite.Extensions;
using System;
using System.Collections.Generic;
using Cognite.Extractor.Utils.CommandLine;
using System.CommandLine;
using Cognite.Extractor.Common;

class MyExtractor : BaseExtractor<BaseConfig>
{
    public MyExtractor(BaseConfig config, IServiceProvider provider, CogniteDestination destination, ExtractionRun run, RemoteConfigManager<BaseConfig> configManager)
        : base(config, provider, destination, run, configManager)
    {
        // Configure extraction pipeline
        if (run != null) run.Continuous = true;
        // Configure extractor to check for updates to config every five minutes.
        // Here you could also use CronTimeSpanWrapper, or TimeSpanWrapper, to let users set this through configuration.
        if (configManager != null)
        {
            configManager.UpdatePeriod = new BasicTimeSpanProvider(TimeSpan.FromMinutes(5));
            // On configuration change, tell the extractor to stop.
            OnConfigUpdate += (sender, config, revision) =>
            {
                Source.Cancel();
            };
        }
    }

    protected override async Task Start()
    {
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

// Class for flat command line arguments
class Options
{
    [CommandLineOption("Specify path to config file", true, "-c")]
    public string ConfigPath { get; set; }
}

// Then, in the Main() method:
class Program
{
    static async Task Main(string[] args)
    {
        var command = new RootCommand
        {
            Description = "Simple example extractor"
        };
        var binder = new AttributeBinder<Options>();
        binder.AddOptionsToCommand(command);

        command.SetHandler<Options>(async opt =>
        {
            // This can also be invoked directly in main, to not have a CLI.
            await ExtractorRunner.Run<BaseConfig, MyExtractor>(
                configPath: opt.ConfigPath ?? "config.yml",
                acceptedConfigVersions: new[] { 1 },
                appId: "my-extractor",
                userAgent: "myextractor/1.0.0",
                addStateStore: false,
                addLogger: true,
                addMetrics: true,
                restart: true,
                CancellationToken.None).ConfigureAwait(false);
        }, binder);

        await command.InvokeAsync(args).ConfigureAwait(false);
    }
}
