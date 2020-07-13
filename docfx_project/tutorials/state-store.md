# Using the state store.

The state store is used to store the ranges of points/events/others extracted from source systems. It is intended to be used when the source system has significant amounts of historical data, and reading first/last timestamps from CDF or other destination systems is infeasible.

## Concepts

When reading data from historical source systems there are three typical sources:

 - *Backfill*, meaning old historical data that should be synchronized to destination systems. Backfill typically only runs once for each source object, but there may be a lot of it, and it may take days or weeks to read it all. Backfill reads backwards through history.
 - *Frontfill*, meaning historical data that has been generated while the extractor was offline. This is usually a much smaller amount than for Backfill.
 - *Streaming*, meaning live updates to data. Usually this represents live data from source systems, but it can also be updates to old data, so streamed data can come from anywhere in history.

Any point of data can exist in three different states:
 
 - Unextracted, so sitting in source systems. This will usually be outside of frontfill/backfill ranges.
 - Read, but not pushed to destinations. These points are in the extractor, but for whatever reason are not pushed to CDF or other destinations. Typically this will happen if the extractor does some kind of buffering of streamed data.
 - Extracted, these points exist in destination systems and are considered to be safe.
 
The state store is exclusively interested in points that have been fully extracted. Read points can be lost if the extractor goes down.

## Initialization

When starting the extractor, it first needs to read ranges from state-store/destination systems.

```c#
// Somehow generate states from source system, typically some method to list source system extractable objects.
IEnumerable<BaseExtractionState> states = ObtainStatesFromSourceSystem();
services.AddStateStore();

using (var provider = services.BuildServiceProvider())
{
    var stateStore = provider.GetRequiredService<IExtractionStateStore>();
    var stateDict = states.ToDictionary(state => state.Id);

    await stateStore.RestoreExtractionState(stateDict, "someTable", cancellationToken);
}
```

Alternatively, using CDF timeseries as a kind of store

```c#
IEnumerable<BaseExtractionState> states = ObtainStatesFromSourceSystem();

var destination = provider.GetRequiredService<CogniteDestination>();

var ids = states.Select(state => Identity.Create(state.Id))
var ranges = await destination.GetExtractedRanges(ids, cancellationToken);

foreach (var state in states) {
    if (ranges.TryGetValue(Identity.Create(state.Id), out TimeRange range)) {
        state.InitExtractedRange(range.First, range.Last);
    }
}
```

## Usage

When the extractor is running, states must be informed of points pushed to destinations.

```c#
var destination = provider.GetRequiredService<CogniteDestination>();
var stateStore = provider.GetRequiredService<IExtractionStateStore>();

IDictionary<string, Datapoint> points = ReadPointsFromSource();

var pointsByIdentity = points.ToDictionary(kvp => Identity.Create(kvp.Key), kvp => kvp.Value, new IdentityComparer());

await destination.InsertDataPointsIgnoreErrors(points, cancellationToken);

// For each successfully pushed list of points, report the first/last timestamp to states.
foreach (var kvp in points)
{
    if (stateDict.TryGetValue(kvp.Key, out var state)) {
        DateTime (min, max) = kvp.Value.MinMax(dp => dp.Timestamp);
        state.UpdateDestinationRange(min, max);
    }
}
// Finally store states in state store, this only stores modified states.
await stateStore.StoreExtractionState(states, "someTable", cancellationToken);
```