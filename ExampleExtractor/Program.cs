using Cognite.Extractor.Utils;
using CogniteSdk;
using System.Threading.Tasks;
using System.Threading;
using Cognite.Extensions;
using System;
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
        //await CreateExtractor(0, CancellationToken.None).ConfigureAwait(false);

        //await TestTurningOffExtractors().ConfigureAwait(false);
        //await TestRestartingExtractor().ConfigureAwait(false);
        await TestMultipleExtractorsActive().ConfigureAwait(false);
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

    //1. Start three extractors
    //2. The extractor with highest priority will start while the two others will go into standby
    //3. After 25 seconds turn off the first extractor, the standby extractor with highest priority will then start
    //4. After 25 more seconds turn off the second extractor, the last standby extractor will then start
    //5. Stop all the extractors
    static public async Task TestTurningOffExtractors()
    {
        Console.WriteLine("Test turning off extractors... \n");

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
            await Task.Delay(25000).ConfigureAwait(false);
            source1.Cancel();
            Console.WriteLine("\nTurning off extractor 0... \n");

            await Task.Delay(25000).ConfigureAwait(false);
            source2.Cancel();
            Console.WriteLine("\nTurning off extractor 1... \n");

            await Task.Delay(15000).ConfigureAwait(false);
            source3.Cancel();
            Console.WriteLine("\nTurning off extractor 2... \n");
        });

        await Task.WhenAll(extractor1, extractor2, extractor3, cancel).ConfigureAwait(false);   

        source1.Dispose(); 
        source2.Dispose();  
        source3.Dispose();
    }
    //1. Start two extractors
    //2. The extractor with highest priority will start while the other will go into standby
    //3. After 15 seconds turn off the first extractor, the second extractor will then start
    //4. After 25 more seoncds restart the first extractor, this extractor will then go into standby
    //5. After 20 more seconds turn off the second extractor, the restarted first extractor will then start again
    //6. Stop all the extractors
    static public async Task TestRestartingExtractor()
    {
        Console.WriteLine("Test restarting extractor... \n");

        var source1 = new CancellationTokenSource();
        CancellationToken ct1 = source1.Token;

        var source2 = new CancellationTokenSource();
        CancellationToken ct2 = source2.Token;

        var source3 = new CancellationTokenSource();
        CancellationToken ct3 = source3.Token;
        
        Task extractor1 = CreateExtractor(0, ct1);
        Task extractor2 = CreateExtractor(1, ct2);

        Task cancel = Task.Run(async () => {
            await Task.Delay(15000).ConfigureAwait(false);
            source1.Cancel();
            Console.WriteLine("\nTurning off extractor 0... \n");

            await Task.Delay(45000).ConfigureAwait(false);
            source2.Cancel();
            Console.WriteLine("\nTurning off extractor 1... \n");

            await Task.Delay(20000).ConfigureAwait(false);
            source3.Cancel();
        });

        Task restartExtractor = Task.Run(async () => {
            await Task.Delay(40000).ConfigureAwait(false);
            Task extractor1Restart = CreateExtractor(0, ct3);
            Console.WriteLine("\nRestarting extractor 0... \n");
        });

        await Task.WhenAll(extractor1, extractor2, cancel, restartExtractor).ConfigureAwait(false);   

        source1.Dispose(); 
        source2.Dispose();  
        source3.Dispose();
    }


    //1. Start two extractors
    //2. The extractor with highest priority will start while the other will go into standby
    //3. After 15 seconds turn off the first extractor, the second extractor will then start
    //4. After 15 more seconds restart the first extractor
    //5. Due to the second extractor not having had time to update its status to active yet, the first extractor will also start
    //6. Both extractors will detect that there are now two active extractors
    //7. The extractor with the lowest priority will then turn itself off, in this case the second extractor
    //8. The first extractor will continue running while the second goes back into standby mode
    //9. Stop all the extractors
    static public async Task TestMultipleExtractorsActive()
    {
        Console.WriteLine("Test running multiple active extractors... \n");

        var source1 = new CancellationTokenSource();
        CancellationToken ct1 = source1.Token;

        var source2 = new CancellationTokenSource();
        CancellationToken ct2 = source2.Token;

        var source3 = new CancellationTokenSource();
        CancellationToken ct3 = source2.Token;
        
        Task extractor1 = CreateExtractor(0, ct1);
        Task extractor2 = CreateExtractor(1, ct2);

        Task cancel = Task.Run(async () => {
            await Task.Delay(15000).ConfigureAwait(false);
            source1.Cancel();
            Console.WriteLine("\nTurning off extractor 0... \n");

            await Task.Delay(60000).ConfigureAwait(false);
            source2.Cancel();
            source3.Cancel();
        });

        Task restartExtractor = Task.Run(async () => {
            await Task.Delay(23000).ConfigureAwait(false);
            Console.WriteLine("\nRestarting extractor 0... \n");

            await CreateExtractor(0, ct3).ConfigureAwait(false);
        });

        await Task.WhenAll(extractor1, extractor2, cancel, restartExtractor).ConfigureAwait(false);   

        source1.Dispose(); 
        source2.Dispose(); 
        source3.Dispose(); 
    }
}
