# Using HistoryExtractionState

HistoryExtractionState is a more advanced abstraction of the history extraction process. Basic usage is fairly similar to BaseExtractionState, but using it requires a few extra steps when extracting data. It handles some more of the logic behind synchronization of data between source and destination systems.

Most importantly it introduces a new TimeRange "SourceExtractedRange", which describes the range of data present in the source system. This is separate from the data present in destination systems and handles the "Read, but not pushed" case described [here](state-store.md).

Using the state consists of the source range separately from the destination range, in different ways depending on which operation is being performed.

## Initialization

First of all, when creating HistoryExtractionState you must specify whether frontfill/backfill is enabled for the given state. This determines how the state is synchronized, and what default values for ranges are.

It also allows using multiple source systems for initialization. After creating the state, it is possible to call `InitExtractedRange` for each source system. Then, once that is done, call `FinalizeRangeInit` to set un-initialized states to default values.

```c#

IEnumerable<HistoryExtractionState> states = ObtainStatesFromSourceSystem();
var stateDict = states.ToDictionary(state => state.Id);

var stateStore = provider.GetRequiredService<IExtractionStateStore>();

await stateStore.RestoreExtractionState(stateDict, "someTable", cancellationToken);

var destination = provider.GetRequiredService<CogniteDestination>();

var ids = states.Select(state => Identity.Create(state.Id))
var ranges = await destination.GetExtractedRanges(ids, cancellationToken);

foreach (var state in states)
{
    if (ranges.TryGetValue(Identity.Create(state.Id), out TimeRange range)) {
        state.InitExtractedRange(range.First, range.Last);
    }
}

foreach (var state in states)
{
    state.FinalizeRangeInit();
}
```

## Usage

When running, after each block of data obtained from the source system, the state should be updated.

```c#

Dictionary<string, IEnumerable<Datapoints>> points = DoFrontfill();

foreach (var kvp in points)
{
    var max = kvp.Value.Select(dp => dp.Timestamp).Max();
    // True to indicate that this is the final block of data
    stateDict[kvp.Key].UpdateFromFrontfill(max, true);
}

points = DoBackfill();

foreach (var kvp in points)
{
    var min = kvp.Value.Select(dp => dp.Timestamp).Min();
    // True to indicate that this is the final block of data
    stateDict[kvp.Key].UpdateFromBackfill(min, true);
}

points = DoStreaming();

foreach (var kvp in points)
{
    var (min, max) = kvp.Value.MinMax(dp => dp.Timestamp);
    stateDict[kvp.Key].UpdateFromStream(min, max);
}
```

Then after pushing to destinations, call `UpdateDestinationRange` as normal.
