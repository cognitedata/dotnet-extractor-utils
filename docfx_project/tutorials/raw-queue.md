# Using Raw upload queues.

Raw upload queues buffer POCOs and stores them in Raw at a fixed interval or whenever the queue reaches a specified max size.

```c#
// Data transfer object representing raw columns
private class ColumnsDto
{
    public string Name { get; set; }
    public int Number { get; set; }
}

// Creates an queue that uploads rows to Raw every 5 seconds (or when the queue size reaches 1.000)
await using (var queue = destination.CreateRawUploadQueue<ColumnsDto>("myDb", "myTable", TimeSpan.FromSeconds(5), 1_000,
    result => { handle result of upload here }))
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