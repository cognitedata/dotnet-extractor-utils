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
  api-key: ${COGNITE_API_KEY}
```

Set the ```COGNITE_PROJECT``` and ```COGNITE_API_KEY``` environment variables. Set the ```metrics``` tag, only if collecting metrics is required by the extractor. If using a [Prometheus pushgateway](https://prometheus.io/docs/practices/pushing/), set ```host```to a valid endpoint.

The library is intended to be used with dependency injection, In ```Program.cs```:

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

Using the ```destination``` you can now push data from your source system to CDF.

For example, inserting datapoints:

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

The utils handles CDF limits on datapoint values and timestamps.