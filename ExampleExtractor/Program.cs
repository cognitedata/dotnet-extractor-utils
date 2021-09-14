using Microsoft.Extensions.DependencyInjection;
using Cognite.Extractor.Utils;
using CogniteSdk;
using System.Threading.Tasks;
using System.Threading;
using Cognite.Extensions;
using System;
using System.Collections.Generic;

class MyExtractor : BaseExtractor
{
    public MyExtractor(BaseConfig config, CogniteDestination destination, CancellationToken token)
        : base(config, destination, token)
    {
    }

    public override async Task Start()
    {
        await Destination.EnsureTimeSeriesExistsAsync(new[]
        {
            new TimeSeriesCreate {
                ExternalId = "sine-wave",
                Name = "Sine Wave"
            }
        }, RetryMode.OnError, SanitationMode.Clean, Source.Token);
        CreateTimeseriesQueue(1000, TimeSpan.FromSeconds(1), null);
        ScheduleDatapointsRun("datapoints", TimeSpan.FromMilliseconds(100), token =>
        {
            var dp = (
                Identity.Create("sine-wave"),
                new Datapoint(DateTime.UtcNow, Math.Sin(DateTime.UtcNow.Ticks))
            );
            return Task.FromResult<IEnumerable<(Identity, Datapoint)>>(new[] { dp });
        });
        // So that the extractor doesn't terminate immediately
        await Scheduler.WaitForAll();
    }
}

// Then, in the Main() method:
class Program
{
    static void Main()
    {
        ExtractorRunner.Run<BaseConfig>(
            "config.yml",
            new[] { 1 },
            "my-extractor",
            "myextractor/1.0.0",
            false,
            true,
            true,
            true,
            (provider, token) => new MyExtractor(
                provider.GetRequiredService<BaseConfig>(),
                provider.GetRequiredService<CogniteDestination>(),
                token
            ),
            CancellationToken.None).Wait();
    }
}
