using Xunit;

// Integration tests make real, retrying network calls to CDF and run for seconds at a time;
// left to xunit's default cross-collection parallelism, they compete for the thread pool with
// timing-sensitive unit tests (e.g. BaseExtractorTest's Task.Run-per-action dispatch tests,
// which poll against a fixed several-second budget) on CI's constrained runners, causing
// otherwise-deterministic unit tests to flake under contention. Disabling parallelization across
// the whole assembly trades some CI wall-clock time for eliminating that contention.
[assembly: CollectionBehavior(DisableTestParallelization = true)]
