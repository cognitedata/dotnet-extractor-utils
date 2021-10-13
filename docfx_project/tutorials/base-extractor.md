# Using the BaseExtractor class

The BaseExtractor class is intended to be a simple way to create an extractor pushing datapoints, events or raw rows to CDF. Creating a basic extractor is easy: 


```c#
using Microsoft.Extensions.DependencyInjection;
using Cognite.Extractor.Utils;
using Cognite.Extensions;
using CogniteSdk;

class MyExtractor : BaseExtractor<BaseConfig>
{
    public MyExtractor(BaseConfig config, CogniteDestination destination)
        : base(config, destination)
    {
    }
    
    protected override async Task Start() 
    {
        // The start method is called when the extractor starts.
        // Here we use it to first ensure that the timeseries we want to write to exists.
        await Destination.EnsureTimeSeriesExistsAsync(new[]
        {
            new TimeSeriesCreate {
                ExternalId = "sine-wave",
                Name = "Sine Wave"
            }
        }, RetryMode.OnError, SanitationMode.Clean, Source.Token);
        
        // Next we create the timeseries queue, so that we can periodically upload datapoints to CDF.
        CreateTimeseriesQueue(1000, TimeSpan.FromSeconds(1), null);
        
        // Finally we schedule upload of datapoints on the scheduler, which will run every 100 ms, and write
        // to the datapoint queue
        ScheduleDatapointsRun("datapoints", TimeSpan.FromMilliseconds(100), token =>
        {
            var dp = (
                Identity.Create("sine-wave"),
                new Datapoint(DateTime.UtcNow, Math.Sin(DateTime.UtcNow.Ticks))
            );
            return Task.FromResult<IEnumerable<(Identity, Datapoint)>>(new [] { dp });
        });
    }
}

// Then, in the Main() method:
class Program
{
    static async Task Main()
    {
        await ExtractorRunner.Run<BaseConfig, MyExtractor>(
            "config.yml",
            new[] { 1 },
            "my-extractor",
            "myextractor/1.0.0",
            false,
            true,
            true,
            true,
            CancellationToken.None);
    }
}
```

[ExtractorRunner.Run](xref:Cognite.Extractor.Utils.ExtractorRunner) is used to wait for a valid config file, handle and log errors, and configure services.

If the extractor crashes it can optionally restart automatically, reloading config and building a new extractor object.
