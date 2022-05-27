# Enabling Extraction Pipelines

In order to enable using extraction pipelines with the `BaseExtractor`, you need to provide it with an `ExtractionRun`, which is the class representing an extraction pipeline.

By default this is provided through dependency injection, so you can just do:

```c#

public MyExtractor(
    BaseConfig config,
    IServiceProvider provider,
    CogniteDestination destination,
    ExtractionRun? run) : base(config, provider, destination, run)
{
    // If your extractor is continuously running
    if (run != null) run.Continuous = true;
}
```

and it will work automatically if `extraction-pipeline: pipeline-id: ` is set to the extraction pipeline external id in the config file.

If you set the `run.Continuous` option, you indicate that your extractor runs continuously, and does not halt after finishing. This makes it behave slightly differently.

The run will only start to report `Seen` once `Start` has terminated, so if you have long-running tasks, those should be placed in the Scheduler. `Start` is typically used for one-time actions during extractor startup, like connecting to the source system, or creating timeseries.

If _not_ configured `Continuous`, the run will report `Seen` until it is called with `Report(..., final: true)`, which automatically happens if `Start` or the scheduler throws an exception, or when the extractor stops normally.

If configured as `Continuous`, it will also report `Success` once `Start` is finished, to indicate that it has started successfully. It will still report `Success` once it terminates, but that should only happen if the extractor is manually stopped.