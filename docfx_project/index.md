# .NET Extractor Utils for Cognite Data Fusion
This is a collection of libraries to simplify the creation of extractors in .NET.

## Installation

The Cognite Extractor Utils can be downloaded from [NuGet](https://www.nuget.org/packages/Cognite.ExtractorUtils). 

To create a console application and add the **1.0.0** version of the library:

Using .NET CLI:
```sh
mkdir NewExtractor
cd NewExtractor
dotnet new console
dotnet add package Cognite.ExtractorUtils -v 1.0.0
```

## Quickstart

Create a ```config.yml``` file containing the extractor configuration

```yaml
version: 1

logger:
    console:
        level: "debug"

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

Create an extractor by subclassing the `BaseExtractor` class:

```c#
class MyExtractor : BaseExtractor<BaseConfig>
{
    public MyExtractor(BaseConfig config, CogniteDestination destination)
        : base(config, destination)
    {
    }
    
    protected override async Task Start() 
    {
        await Destination.EnsureTimeSeriesExistsAsync(...) // to create timeseries
        CreateTimeseriesQueue(...) // to create an upload queue
        Scheduler.ScheduleTask(...) // to run some one-off task
        Scheduler.SchedulePeriodicTask(...) // to run a task with fixed frequency
    }
}
```

Run the extractor from `Main()`:

```c#
class Program
{
    static async Task Main()
    {
        await ExtractorRunner.Run<BaseConfig, MyExtractor>(
            configPath: opt.ConfigPath ?? "config.yml",
            acceptedConfigVersions: new[] { 1 },
            appId: "my-extractor",
            userAgent: "myextractor/1.0.0",
            addStateStore: false,
            addLogger: true,
            addMetrics: true,
            restart: true,
            CancellationToken.None);
    }
}
```

See the [tutorials](tutorials/intro.md) section for more details, or the [API](api/index.md) section for generated API documentation.