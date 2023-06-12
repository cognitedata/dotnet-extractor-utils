<a href="https://cognite.com/">
    <img src="https://images.squarespace-cdn.com/content/5bd167cf65a707203855d3c0/1540463676940-6USHZRRF36KCAZLUPM2P/Logo-H.jpg?format=300w&content-type=image%2Fjpeg" alt="Cognite logo" title="Cognite" align="right" height="40" />
</a>

.Net Utilities for Building Cognite Extractors
=======================
![Build and Test](https://github.com/cognitedata/dotnet-extractor-utils/workflows/Build%20and%20Test/badge.svg?branch=master&event=push)
![Release](https://github.com/cognitedata/dotnet-extractor-utils/workflows/Create%20Release/badge.svg)
[![codecov](https://codecov.io/gh/cognitedata/dotnet-extractor-utils/branch/master/graph/badge.svg?token=2IX9UN9ING)](https://codecov.io/gh/cognitedata/dotnet-extractor-utils)
[![Nuget](https://img.shields.io/nuget/vpre/Cognite.ExtractorUtils)](https://www.nuget.org/packages/Cognite.ExtractorUtils/)

A library containing utilities for building extractors in .Net.

[Documentation](https://cognitedata.github.io/dotnet-extractor-utils/index.html)

## Installation

The Cognite Extractor Utils can be downloaded from [NuGet](https://www.nuget.org/packages/Cognite.ExtractorUtils). 

To create a console application and add the library:

Using .NET CLI:
```sh
mkdir NewExtractor
cd NewExtractor
dotnet new console
dotnet add package Cognite.ExtractorUtils
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

See the [example configuration](ExtractorUtils/config/config.example.yml) for a full example with all available options.

Set the ```COGNITE_PROJECT```, ```COGNITE_TENANT_ID```, ```COGNITE_CLIENT_ID```, ```COGNITE_CLIENT_SECRET```, and ```COGNITE_SCOPE``` environment variables. Set the ```metrics``` tag, only if collecting metrics is required by the extractor. If using a [Prometheus pushgateway](https://prometheus.io/docs/practices/pushing/), set ```host```to a valid endpoint.

The easiest way to use the library utilities is by using the ```BaseExtractor``` class. The following is a working implementation of an extractor writing a sine wave to CDF.

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
    static async Task Main()
    {
        await ExtractorRunner.Run<BaseConfig, MyExtractor>(
            "config.yml",
            new[] { 1 },
            "my-extractor",
            "myextractor/1.0.0",
            addStateStore: false,
            addLogger: true,
            addMetrics: true,
            restart: true,
            CancellationToken.None);
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

# Using the installer template

The installer template is mostly configured from the command live and the config .json file, but more advanced changes may require modifying the project files. Feel free to copy the installer project and modifying it to your needs. The following instructions produce a single file executable for a .NET core project. It assumes that you have a config file named `config.example.yml` somewhere.

 - Update the `setup-config.json` file to suit your needs. The default fields are described below. All config fields except for `setup_project` is injected as a build property with msbuild into the `.wixproj` file.
 - Modify the `InstBanner.bmp` file in `Resources`, adding your extractor name to the blue field.
 - Modify or replace the `License.rtf` with a license adapted to your project.
 - (Optional) Add any new additional config files at the bottom of the `Product.wxs` file, by adding new `Component` blocks after the one for `config.example.yml`
 - (Optional) Add a cognite icon to your extractor by adding `Resources/black32x32.ico` to the folder for your actual extractor, and adding `<ApplicationIcon>black32x32.ico</ApplicationIcon>` to its .csproj file.
 - Compile the installer in a windows environment with Wix utils and msbuild installed, using something like the following command:
 
`build.ps1 -b Path\To\MSBuild.exe -v 1.0.0 -d "some description" -c Path\To\setup-config.json`

 - `-v` or `-version` is embedded as InformationalVersion when compiling, and can be retrieved at runtime. It is also used for the installer version, so it is required. A good way to retrieve this in a build environment is using a git tag, this way a github release can also be created based on tags.
 - `-b` or `-build` is the path to your MSBuild.exe.
 - `-d` or `-description` is embedded as Description when compiling. It must be set, either to something static, or something like the current git commit + git commit time.
 - `-c` or `-config` is the path to the json configuration file.

The following configuration parameters are used by default:

 - `target_project` is the csproj file for your extractor. Note that all paths are relative to the .wixproj file.
 - `target_configuration` is generally Release.
 - `target_runtime` is most likely either win-x64 or win-x86
 - `product_name` full display name of your product. This is used both in the installer and in the installed programs registry on the target computer.
 - `product_short_name` a short version of the product name without spaces. Should not contain "Cognite". It is used for registry keys and folder names.
 - `exe_name` the name of the final executable.
 - `config_dir` the path to the config.example.yml folder. This is also relative to the .wixproj folder.
 - `service` can be left out. If `true`, the installer will add and configure a windows service for the extractor.
 - `service-args` can be left out. Arguments to specify to the extractor when running it as a service. This is useful if the standalone and service versions use the same executable, but with different command line parameters.
 - `upgrade-guid` is a unique upper case GUID which you must generate yourself. It identifies the project when upgrading.
 - `setup-project` is the path to the .wixproj file used to build.
 - `output-name` is the name of the output msi, like `MyExtractorSetup`.
 
See the ExampleExtractorSetup project in this repository for a full example.

In general the setup template assumes that this is a cognite product, but changing this is no more difficult than replacing instances of `Cognite` in `Product.wxs` with whatever suits your purposes.
 
## Modifying the installer template

Adding to the installer template is relatively easy. New builds can be added in the `<Target Name="BeforeBuild">` block in `SetupTemplate.wixproj`, these should output to new folders. New files going in the `bin/` folder can be added to `<ComponentGroup Id="ExecutableComponentGroup">`. Note that the executable is duplicated here due to conditionals on `service`. New components can be added after `<?endif?>`.

New folders can be added by adding new `Directory` tags in the first `Fragment`, a new `ComponentGroupRef` at the bottom of `Product`, and a new `ComponentGroup` somewhere in the last `Fragment`.

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
