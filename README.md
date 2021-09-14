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

The easiest way to use the library utilities is by using the ```BaseExtractor``` class. The following is a working implementation of an extractor writing a sine wave to CDF.

```c#
using Microsoft.Extensions.DependencyInjection;
using Cognite.Extractor.Utils;
using Cognite.Extensions;
using CogniteSdk;

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
			return Task.FromResult<IEnumerable<(Identity, Datapoint)>>(new [] { dp });
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

## Contributing

Due to restrictions on integration tests, PRs from external forks should be merged into the `integration` branch.

The project requires test coverage, if your change adds code, make sure to create a test for it.

External commits are _merged_. PRs should have clean commit history with descriptive commit messages.

### Style

 - Newline before braces.
 - Private member names start with underscore.
 - Use PascalCase for methods and public members, and camelCase for locals.
 - Public members must be `const` or properties.
 - Methods must have inline XML documentation.
 - Make sure to fix any warnings generated by code analysis during build.
