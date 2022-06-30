using Cognite.Extractor.Utils;
using CogniteSdk;
using System.Threading.Tasks;
using System.Threading;
using Cognite.Extensions;
using System;
using System.Text.Json;
using System.Collections.Generic;
using Cognite.Extractor.Utils.CommandLine;
using System.CommandLine;

class MyExtractor : BaseExtractor<BaseConfig>
{
    public MyExtractor(BaseConfig config, IServiceProvider provider, CogniteDestination destination, ExtractionRun run)
        : base(config, provider, destination, run)
    {
        if (run != null) run.Continuous = true;
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

class Program
{
    static async Task Main(string[] args)
    {
        //await CreateExtractor(0, CancellationToken.None);

        await TestExtractors();
    }

    static public Task CreateExtractor(int index, CancellationToken ct)
    {
        return Task.Run(async () => {
            ct.ThrowIfCancellationRequested();
            await ExtractorRunner.Run<BaseConfig, MyExtractor>(
                index,
                configPath: "config.yml",
                acceptedConfigVersions: new[] { 1 },
                appId: "my-extractor",
                userAgent: "myextractor/1.0.0",
                addStateStore: false,
                addLogger: true,
                addMetrics: false,
                restart: true,
                ct).ConfigureAwait(false);
      
            }, ct);
    }

    static public async Task TestExtractors()
    {
        var source1 = new CancellationTokenSource();
        CancellationToken ct1 = source1.Token;

        var source2 = new CancellationTokenSource();
        CancellationToken ct2 = source2.Token;
        
        var source3 = new CancellationTokenSource();
        CancellationToken ct3 = source3.Token;

        Task extractor1 = CreateExtractor(0, ct1);
        Task extractor2 = CreateExtractor(1, ct2);
        Task extractor3 = CreateExtractor(2, ct3);

        Task cancel = Task.Run(async () => {
            await Task.Delay(25000);
            source1.Cancel();

            Console.WriteLine();
            Console.WriteLine("Turning off extractor 0...");
            Console.WriteLine();

            await Task.Delay(25000);
            source2.Cancel();

            Console.WriteLine();
            Console.WriteLine("Turning off extractor 1...");
            Console.WriteLine();

            await Task.Delay(25000);
            source3.Cancel();

            Console.WriteLine();
            Console.WriteLine("Turning off extractor 2...");
            Console.WriteLine();
        });

        await Task.WhenAll(extractor1, extractor2, extractor3, cancel).ConfigureAwait(false);   

        source1.Dispose(); 
        source2.Dispose();  
        source3.Dispose();
    }
}
