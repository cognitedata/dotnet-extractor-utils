# Basic Usage

The extractor utils contain a number of tools useful for creating extractors. To create a minimal extractor using the utils, first create a ```config.yml``` file with some basic configuration:

```yaml
version: 1

logger:
    console:
        level: "debug"

metrics:
    push-gateways:
      - host: "http://localhost:9091"
        job: "extractor-metrics"

cognite:
    project: ${COGNITE_PROJECT}
    # This is for microsoft as IdP, to use a different provider,
    # set implementation: Basic, and use token-url instead of tenant.
    # See the example config for the full list of options.
    idp-authentication:
        # Directory tenant
        tenant: ${COGNITE_TENANT_ID}
        # Application Id
        client-id: ${COGNITE_CLIENT_ID}
        # Client secret
        secret: ${COGNITE_CLIENT_SECRET}
        # List of resource scopes, ex:
        # scopes:
        #   - scopeA
        #   - scopeB
        scopes:
          - ${COGNITE_SCOPE}
```

See [full config object](xref:Cognite.Extractor.Utils.BaseConfig) for full list of options. 

Set the ```COGNITE_PROJECT``` and ```COGNITE_API_KEY``` environment variables. Set the ```metrics``` tag, only if collecting metrics is required by the extractor. If using a [Prometheus pushgateway](https://prometheus.io/docs/practices/pushing/), set ```host```to a valid endpoint.

The library is intended to be used with dependency injection, In ```Program.cs```:

```c#
using Microsoft.Extensions.DependencyInjection;
using Cognite.Extractor.Utils;
using Cognite.Extensions;
using CogniteSdk;

class MyExtractor : BaseExtractor
{
    public MyExtractor(BaseConfig config, CogniteDestination destination)
        : base(config, destination)
    {
    }
    
    protected override async Task Start() 
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
            return Task.FromResult<IEnumerable<(Identity, Datapoint)>>(new [] { dp });
        });
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
            addStateStore: false,
            addLogger: true,
            addMetrics: true,
            restart: true,
            CancellationToken.None).Wait();
    }
}
```