# Using timeseries upload queues

Timeseries upload queues buffer datapoints and sends them to CDF at a fixed interval, or whenever the queue reaches a certain size. Automatic uploads can be disabled entirely.

If inserting into CDF fails with a timeout or 5xx response code, datapoints can optionally be written to a local file, then read once the queue is able to push again. The path to a file is passed to `CreateTimeSeriesUploadQueue`. Multiple queues can not use the same buffer file.

```c#
// Creates a queue that uploads datapoints to CDF every 5 seconds (or when the queue size reaches 1.000)
await using (var queue = destination.CreateTimeSeriesUploadQueue<ColumnsDto>(TimeSpan.FromSeconds(5), 1_000,
    result => { handle result of upload here }, "some-path.bin"))
{
    // Task to generate datapoints at regular intervals
    var enqueueTask = Task.Run(async () => {
        while (index < 2_000)
        {
            queue.Enqueue("my_timeseries", new Datapoint(DateTime.UtcNow, index));
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
} // disposing the queue will upload any datapoints left and stop the upload loop
```

## State storage

Call [AddStateStorage](xref:Cognite.Extractor.Utils.TimeSeriesUploadQueue) with a dictionary of states in order to enable automatically updating extracted ranges after pushing.

```c#
var myStateMap = new Dictionary<Identity, BaseExtractionState>(new IdentityComparer());

queue.AddStateStorage(states, provider.GetRequiredService<IExtractionStateStore>(), "some-timeseries-collection-name");
```

Adding a state store implementation as well makes the queue store states there after uploading.