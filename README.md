<a href="https://cognite.com/">
    <img src="https://images.squarespace-cdn.com/content/5bd167cf65a707203855d3c0/1540463676940-6USHZRRF36KCAZLUPM2P/Logo-H.jpg?format=300w&content-type=image%2Fjpeg" alt="Cognite logo" title="Cognite" align="right" height="40" />
</a>

.Net Utilities for Building Cognite Extractors
=======================
![Build and Test](https://github.com/cognitedata/dotnet-extractor-utils/workflows/Build%20and%20Test/badge.svg?branch=master&event=push)
![Release](https://github.com/cognitedata/dotnet-extractor-utils/workflows/Create%20Release/badge.svg)
[![codecov](https://codecov.io/gh/cognitedata/dotnet-extractor-utils/branch/master/graph/badge.svg?token=2IX9UN9ING)](https://codecov.io/gh/cognitedata/dotnet-extractor-utils)
[![Nuget](https://img.shields.io/nuget/vpre/Cognite.ExtractorUtils)](https://www.nuget.org/packages/Cognite.ExtractorUtils/)

_**Under development**, not recommended for production use cases_

A library containing utilities for building extractors in .Net

## Installation

The Cognite Extractor Utils can be downloaded from [NuGet](https://www.nuget.org/packages/Cognite.ExtractorUtils). 

To create a console application and add the **1.0.0-alpha-017** version of library:

Using .NET CLI:
```sh
mkdir NewExtractor
cd NewExtractor
dotnet new console
dotnet add package Cognite.ExtractorUtils -v 1.0.0-alpha-017
```

[Documentation (WIP)](https://cognitedata.github.io/dotnet-extractor-utils/index.html)

## Quickstart

Create a ```config.yml``` file containing the extractor configuration

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
  api-key: ${COGNITE_API_KEY}
```

See the [example configuration](ExtractorUtils/config/config.example.yml) for a full example with all available options.

Set the ```COGNITE_PROJECT``` and ```COGNITE_API_KEY``` environment variables. Set the ```metrics``` tag, only if collecting metrics is required by the extractor. If using a [Prometheus pushgateway](https://prometheus.io/docs/practices/pushing/), set ```host```to a valid endpoint.

The easiest way to use the library utilities is through **dependency injection**. Open ```Program.cs``` and use the library as follows:

```c#
using Microsoft.Extensions.DependencyInjection;
using Cognite.Extractor.Configuration;
using Cognite.Extractor.Logging;
using Cognite.Extractor.Metrics;
using Cognite.Extractor.Utils;

// Then, in the Main() method:
var services = new ServiceCollection();
services.AddConfig<BaseConfig>("./config.yml", 1);
services.AddLogger();
services.AddMetrics();
services.AddCogniteClient("MyExtractor", true, true);

// Create a service provider and resolve the required services
using (var provider = services.BuildServiceProvider()) {
    // Resolve a logger for this class
    var logger = provider.GetRequiredService<ILogger<Program>>();
    logger.LogInformation("Hello Extractor");

    // Resolve the metrics service and start it
    var metrics = provider.GetRequiredService<MetricsService>();
    metrics.Start();
    
    // Resolve the cognite destination
    var destination = provider.GetRequiredService<CogniteDestination>();
    await destination.TestCogniteConfig(cancellationToken);
    
    // Use the Cognite destination to create time series and insert data points.
    // For instance: The line below gets or create the time series. The buildTimeSeriesObjects is a callback function that creates
    // TimeSeriesCreate objects for any missing time series.
    var ts = await destination.GetOrCreateTimeSeriesAsync(
        externalIds,
        buildTimeSeriesObjects,
        cancellationToken
    );
    
    // Stops the metrics service
    await metrics.Stop();
}
```

## Inserting data points:
```c#
// Create a dictonary of time series identities and datapoint objects
datapoints = new Dictionary<Identity, IEnumerable<Datapoint>>() {
    { new Identity("externalId1"), new Datapoint[] { new Datapoint(DateTime.UtcNow, "A")}},
    { new Identity("externalId2"), new Datapoint[] { new Datapoint(DateTime.UtcNow, 1), 
                                                     new Datapoint(DateTime.UtcNow, 2)}},
    { new Identity(12345789), new Datapoint[] { new Datapoint(DateTime.UtcNow, 1)}}}
};

// Insert the data points, ignoring and returning any errors.
var errors = await destination.InsertDataPointsIgnoreErrorsAsync(
    datapoints,
    cancellationToken);
if (errors.IdsNotFound.Any() || errors.IdsWithMismatchedData.Any())
{
    logger.LogError("Ids not found: {NfIds}. Time series with mismatched type: {MmIds}",
        errors.IdsNotFound, errors.IdsWithMismatchedData);
}
```

## Using Raw upload queues:
```c#
// Data type object representing raw columns
private class ColumnsDto
{
    public string Name { get; set; }
    public int Number { get; set; }
}

// Creates an queue that uploads rows to Raw every 5 seconds (or when the queue size reaches 1.000)
using (var queue = destination.CreateRawUploadQueue<ColumnsDto>("myDb", "myTable", TimeSpan.FromSeconds(5), 1_000,
    result => { // handle result of upload here }))
{
    // Task to generate rows at regular intervals
    var enqueueTask = Task.Run(async () => {
        while (index < 2_000)
        {
            queue.EnqueueRow($"r{index}", new ColumnsDto {Name = "Test", Number = index});
            await Task.Delay(50, cancellationToken);
            index++;
        }
    });
    
    // Task to start the upload queue
    var uploadTask = queue.Start(cancellationToken);

    // wait for either the enqueue task to finish or the upload task to fail
    var t = Task.WhenAny(uploadTask, enqueueTask);
    await t;
    logger.LogInformation("Enqueueing task completed. Disposing of the upload queue");
} // disposing the queue will upload any rows left and stop the upload loop

```

## Using the State Store
```c#
services.AddStateStore();

using (var provider = services.BuildServiceProvider()) {
    var stateStore = provider.GetRequiredService<IExtractionStateStore>();
    var destination = provider.GetRequiredService<CogniteDestination>();
    
    // Create a state for a node
    var myState = new BaseExtractionState("myState");
    var states = new [] { myState };
    var stateDict = states.ToDictionary(state => state.Id);
    
    await stateStore.RestoreExtarctionState(stateDict, "someTableorcollection", cancellationToken)
   
    // After uploading points cdf, update the ranges in your state
    var now = DateTime.UtcNow;
    var datapoints = new Dictionary<Identity, IEnumerable<Datapoint>>() {
        { new Identity("myState"), new Datapoint[] { new Datapoint(now - TimeSpan.FromHours(2), "B"), new Datapoint(now, "A")}}}

    await destination.InsertDataPointsIgnoreErrorsAsync(datapoints, cancellationToken);
    
    myState.UpdateDestinationRanges(now - TimeSpan.FromHours(2), now);
    
    await stateStore.StoreExtractionState(states, "someTableorcollection", cancellationToken);
    // If the extractor stops here, the state is saved and can be restored after restart.
}
```

# Code of Conduct

This project follows https://www.contributor-covenant.org

## License

Apache v2, see [LICENSE](https://github.com/cognitedata/dotnet-extractor-utils/blob/master/LICENSE).
