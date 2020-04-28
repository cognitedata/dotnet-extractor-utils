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

To create a console application and add the **1.0.0-alpha-007** version of library:

Using .NET CLI:
```sh
mkdir NewExtractor
cd NewExtractor
dotnet new console
dotnet add package Cognite.ExtractorUtils -v 1.0.0-alpha-007
```
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

Set the ```COGNITE_PROJECT``` and ```COGNITE_API_KEY``` environment variables. Set the ```metrics``` tag, only if collecting metrics is required by the extractor. If using a [Prometheus pushgateway](https://prometheus.io/docs/practices/pushing/), set ```host```to a valid endpoint.

The easiest way to use the library utilities is through **dependency injection**. Open ```Program.cs``` and use the library as follows:

```c#
using ExtractorUtils;
using Microsoft.Extensions.DependencyInjection;

// Then, in the Main() method:
var services = new ServiceCollection();
services.AddConfig<BaseConfig>("./config.yml", 1);
services.AddLogger();
services.AddMetrics();
services.AddCogniteClient("MyExtractor", true, true);

// Create a service provider and resolve the required services
using (var provider = services.BuildServiceProvider()) {
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

# Code of Conduct

This project follows https://www.contributor-covenant.org

## License

Apache v2, see [LICENSE](https://github.com/cognitedata/dotnet-extractor-utils/blob/master/LICENSE).
