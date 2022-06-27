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
        
        //createDatabase();    
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

    public async void createDatabase()
    {
        var res = await Destination.CogniteClient.Login.StatusAsync().ConfigureAwait(false);
        Console.WriteLine(res);

        var rows = await Destination.CogniteClient.Raw.ListRowsAsync<JsonElement>("kjerand-test-db", "kjerand-test-table").ConfigureAwait(false);
        Console.WriteLine(rows);
    }
}

class Program
{
    static async Task Main(string[] args)
    {
        List<Task> extractors = createExtractors();
        await Task.WhenAll(extractors).ConfigureAwait(false);
    }

    static public List<Task> createExtractors()
    {
        List<Task> extractors = new List<Task>();
        extractors.Add(createExtractor(1)); 
        return extractors;
    }

     static public Task createExtractor(int index)
     {
        return Task.Run(async () => await ExtractorRunner.Run<BaseConfig, MyExtractor>(
            index,
            configPath: "config.yml",
            acceptedConfigVersions: new[] { 1 },
            appId: "1",
            userAgent: "myextractor/1.0.0",
            addStateStore: false,
            addLogger: true,
            addMetrics: false,
            restart: true,
            CancellationToken.None).ConfigureAwait(false));
     }
}
