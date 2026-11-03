# Charon CDF-writer (unstable)

> **Unstable / POC.** Charon is an in-development CDF-writer service with a changing contract.
> Everything described here lives under `Cognite.Extractor.Utils.Unstable.Charon` and may change
> without notice. It is **opt-in** and disabled by default.

Instead of writing time series straight to CDF, an extractor can register its tasks with Charon
once at startup (`/cdfwriter/setup`) and then stream data batches (`/cdfwriter/payload`). Charon
decorates, routes and shapes items (Kuiper expressions, regex space routing) and forwards them to
CDF using the same bearer token the library already obtains for CDF.

## Configuration

Add a `cdf-writer` section to your `connection.yml`:

```yaml
cdf-writer:
  enabled: true
  base-url: "https://charon.example.com"
```

When `enabled` is `false` (the default) or the section is absent, writes go directly to CDF and
behavior is unchanged. When enabled, the runtime registers the Charon client and the
`CharonWriter` façade. An integration external id (`integration.external-id`) is required.

## Registering tasks

Resolve `CharonWriter` from the service provider and register your tasks in `InitTasks` (which runs
before startup). Build task registrations with `CharonTask`:

```csharp
var writer = Provider.GetRequiredService<CharonWriter>();

// space_routing: items are already-shaped data-modeling bodies; Charon injects `space`.
// Mapping order is significant (first regex match wins).
writer.RegisterTask(CharonTask.SpaceRouting(
    "route-ts", CharonDestinations.TimeSeriesCreate,
    new[]
    {
        new KeyValuePair<string, string>("abcd.*", "space-1"),
        new KeyValuePair<string, string>("def.*", "space-2"),
    },
    defaultSpace: "space-default"));

// custom: items are raw source-shaped; Charon evaluates Kuiper expressions per item.
writer.RegisterTask(CharonTask.Custom(
    "shape-ts", CharonDestinations.TimeSeriesUpdate,
    new[]
    {
        new KeyValuePair<string, string>("externalId", "input.id"),
        new KeyValuePair<string, string>("name", "input.name"),
    }));

await writer.SetupAsync(GetExtractorVersion(), token);
```

`SetupAsync` is a full replace: the registered set is the complete task set for the integration.
A validation failure throws `CharonSetupException` listing every `itemIndex`/`taskName`/`reason`.

## Writing data

Create an upload queue per task:

```csharp
// timeseries create/update
var queue = writer.CreateQueue("route-ts", TimeSpan.FromSeconds(5), maxSize: 1000, logger);
queue.Enqueue(jsonElement); // flat DM body (no `space`) or raw source item

// datapoints: one reading = one payload item (Charon does not pivot)
var dps = writer.CreateDatapointsQueue("dps", TimeSpan.FromSeconds(5), maxSize: 10000, logger);
dps.Enqueue("ts-external-id", new Datapoint(DateTime.UtcNow, 42.0));
```

The queues chunk each task's items to CDF limits before calling Charon (`/timeseries` 1000 items,
`/timeseries/data` 100000 datapoints / 10000 series) — Charon itself does no chunking. Results map
to the usual `QueueUploadResult<T>`: a per-task non-2xx marks that chunk failed; a Charon
`5xx`/timeout surfaces as a fatal `QueueUploadResult.Exception`.

## Limits and caveats

- **Fallback is config-only.** On Charon `5xx`/unreachable the upload fails like any CDF error;
  switch `enabled: false` to fall back to the direct-to-CDF path.
- **Setup runs once at startup.** Charon keeps its task store in memory, so a Charon restart
  mid-run causes payload failures until the extractor restarts.
- **Datapoints throughput.** One reading per payload item is larger on the wire than CDF's pivoted
  datapoints format; for very high-frequency data this is less efficient.
- **Regex anchoring for space routing is not yet frozen** on the Charon side (full-match vs
  substring). Patterns are sent verbatim.
