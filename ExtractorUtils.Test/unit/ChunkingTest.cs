using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Cognite.Extractor.Common;
using Cognite.Extractor.Utils;
using Xunit.Sdk;
using Cognite.Extensions;
using CogniteSdk;

namespace ExtractorUtils.Test.Unit
{
    public static class ChunkingTest {

        [Fact]
        public static async Task RunThrottledOK()
        {
            var completed = new List<int>();
            var token = CancellationToken.None;
            var taskNum = 0;
            var generators = Enumerable.Range(1, 5).Select<int, Func<Task>>(
                i => async () => {
                    Console.Out.WriteLine($"Starting {i}");
                    await Task.Delay(i * 100, token);
                    Console.Out.WriteLine($"Completed {i}");
                    completed.Add(i);
                });

            Action<Task> taskDone = (task) => { Console.Out.WriteLine($"Task completed {++taskNum}: {task.Id} - {task.Status}"); };
            await generators.RunThrottled(2, taskDone, token);
            Assert.Equal(5, completed.Count);
            Assert.Equal(15, completed.Sum());
        }

        [Fact]
        public static async Task RunThrottledException()
        {
            var completed = new List<int>();
            var token = CancellationToken.None;
            var generators = Enumerable.Range(1, 5).Select<int, Func<Task>>(
                i => async () => {
                    Console.Out.WriteLine($"Starting {i}");
                    await Task.Delay(i * 100, token);
                    if (i == 3)
                    {
                        throw new Exception("Failed on 3!!!");
                    }
                    Console.Out.WriteLine($"Completed {i}");
                    completed.Add(i);
                });

            await Assert.ThrowsAsync<Exception>(
                async () => {
                    await generators.RunThrottled(2, token);
                    await Task.Delay(2000);
                }
            );
            Assert.Contains(1, completed);
            // may or may not contain 2
            Assert.DoesNotContain(3, completed);
            // may or may not contain 4
            Assert.DoesNotContain(5, completed);
        }

        [Theory]
        [InlineData(1, 1)]
        [InlineData(10_000, 100_000)]
        [InlineData(10_000, 123_456)]
        [InlineData(999, 1_000_000)]
        public static void TestEnumerableChunkBy(int chunkSize, int datapoints)
        {
            var left = datapoints % chunkSize;
            var numChunks = datapoints/chunkSize + (left > 0 ? 1 : 0);
            Datapoint[] dps = new Datapoint[datapoints];
            for (int i = 0; i < datapoints; i++)
            {
                dps[i] = new Datapoint(DateTime.UtcNow, i * 0.01);
            }

            var chunks = dps.ChunkBy(chunkSize);
            var count = chunks.Count();
            var sum = chunks.Select(c => c.Count()).Sum();
            var last = chunks.Last().Count();

            Assert.Equal(numChunks, count);
            Assert.Equal(datapoints, sum);
            if (left > 0)
            {
                Assert.Equal(left, last);
            }
        }

        [Theory]
        [InlineData(100_000, 10_000, 20000, 100, 20, 1000, 100000)]
        [InlineData(100_000, 10_000, 200, 10000, 20, 10, 100000)]
        [InlineData(100_000, 10_000, 20000, 5, 2, 10000, 50000)]
        [InlineData(100, 10_000, 1, 10_000, 100, 1, 100)]
        public static void TestDictionaryChunking(
            int dpChunk, int tsChunk,
            int timeseries, int datapoints,
            int expChunks, int expTimeseriesMax, int expDatapointsMax)
        {
            var dict = new List<(string, IEnumerable<int>)>();
            for (int i = 0; i < timeseries; i++)
            {
                var points = new List<int>();
                for (int j = 0; j < datapoints; j++)
                {
                    points.Add(i * datapoints + j);
                }
                dict.Add(($"id{i}", points));
            }
            var results = dict.ChunkBy(dpChunk, tsChunk);
            var min = results.Min(c => c.Min(e => e.Values.Count()));
            Assert.True(min > 0);
            var max = results.Max(c => c.Select(e => e.Values.Count()).Sum());
            var maxTs = results.Max(x => x.Count());
            Assert.Equal(expDatapointsMax, max);
            Assert.Equal(expTimeseriesMax, maxTs);
            Assert.Equal(expChunks, results.Count());
            //var total = results.Sum(dct => dct.Values.Sum(val => val.Count()));
            var total = results.SelectMany(x => x.Select(y => y.Values.Count())).Sum();
            var totalTs = results.SelectMany(x => x.Select(y => y.Key)).ToHashSet().Count;
            Assert.Equal(timeseries, totalTs);
            Assert.Equal(datapoints * timeseries, total);

            var exists = new bool[timeseries * datapoints];
            foreach (var chunk in results)
            {
                foreach (var (id, values) in chunk)
                {
                    foreach (var value in values)
                    {
                        exists[value] = true;
                    }
                }
            }
            Assert.True(exists.All(val => val));
        }

        [Theory]
        [InlineData(0, 2, 50)]
        [InlineData(5, 0, 0)]
        [InlineData(2, 2, 50)]
        public static async Task TestTaskThrottler(int maxParallelism, int maxPerUnit, int timespanMs)
        {
            // Running this test in github actions is pretty unreliable...
            for (int i = 0; i < 5; i++)
            {
                try
                {
                    using var throttler = new TaskThrottler(maxParallelism, true, maxPerUnit, TimeSpan.FromMilliseconds(timespanMs));
                    var generators = Enumerable.Range(0, 20)
                        .Select<int, Func<Task>>(_ => () => Task.Delay(100))
                        .ToList();

                    var start = DateTime.UtcNow;
                    foreach (var generator in generators) throttler.EnqueueTask(generator);

                    await throttler.WaitForCompletion();
                    var end = DateTime.UtcNow;

                    double minElapsedTimeParallel = maxParallelism == 0
                        ? 0
                        : 2000 / maxParallelism;
                    double minElapsedTimeLimit = maxPerUnit == 0 || timespanMs == 0
                        ? 0
                        : 20 / maxPerUnit * timespanMs;

                    var minElapsedTime = Math.Max(minElapsedTimeParallel, minElapsedTimeLimit);

                    var realMs = (end - start).TotalMilliseconds;

                    // Task overhead
                    var flex = 100 + realMs * 0.1;
                    Assert.True(realMs > minElapsedTime - flex && realMs < minElapsedTime + flex,
                        $"Execution took {realMs}ms but should be between {minElapsedTime - flex} and {minElapsedTime + flex}");
                    break;
                }
                catch (TrueException)
                {
                    if (i == 4) throw;
                }
            }
        }
        [Fact]
        public static async Task TestTaskThrottlerResults()
        {
            using var throttler = new TaskThrottler(0);
            static Task okGenerator() => Task.Delay(100);
            static async Task badGenerator()
            {
                await Task.Delay(100);
                Assert.True(false);
            }
            var result = await throttler.EnqueueAndWait(okGenerator);
            Assert.Equal(0, result.Index);
            Assert.Null(result.Exception);
            Assert.True(result.IsCompleted);
            // Turns out system clock resolution isn't actually precise enough to guarantee that delay takes as long as it says...
            Assert.True((result.CompletionTime - result.StartTime).Value.TotalMilliseconds >= 80,
                $"Expected task to take at least 100ms, but it took {(result.CompletionTime - result.StartTime).Value.TotalMilliseconds}ms");

            var badResult = await throttler.EnqueueAndWait(badGenerator);
            Assert.Equal(1, badResult.Index);
            Assert.NotNull(badResult.Exception);
            Assert.True(badResult.Exception is AggregateException exc && exc.InnerExceptions.First() is TrueException);

            using var throttler2 = new TaskThrottler(0, true);

            result = await throttler2.EnqueueAndWait(okGenerator);
            Assert.Equal(0, result.Index);
            Assert.Null(result.Exception);
            Assert.True(result.IsCompleted);
            Assert.True((result.CompletionTime - result.StartTime).Value.TotalMilliseconds >= 80);

            await Assert.ThrowsAsync<AggregateException>(() => throttler2.EnqueueAndWait(badGenerator));
        }
        [Theory]
        [InlineData(1, new[] { 3, 2, 1, 1 })]
        [InlineData(2, new[] { 3, 2, 2 })]
        [InlineData(3, new[] { 3, 3, 1 })]
        [InlineData(4, new[] { 3, 4 })]
        [InlineData(5, new[] { 5, 2 })]
        [InlineData(6, new[] { 6, 1 })]
        [InlineData(7, new[] { 7 })]
        public static void TestChunkByHierarchy(int maxSize, int[] lengths)
        {
            var assets = new List<AssetCreate>()
            {
                new AssetCreate { ExternalId = "1" },
                new AssetCreate { ExternalId = "6", ParentExternalId = "5" },
                new AssetCreate { ExternalId = "5", ParentExternalId = "4" },
                new AssetCreate { ExternalId = "3", ParentExternalId = "2" },
                new AssetCreate { ExternalId = "2" },
                new AssetCreate { ExternalId = "4", ParentExternalId = "2" },
                new AssetCreate { ExternalId = "7", ParentExternalId = "0" }
            };
            var result = assets.ChunkByHierarchy(maxSize, create => create.ExternalId, create => create.ParentExternalId).ToList();
            Assert.Equal(7, result.Aggregate(0, (seed, res) => seed + res.Count()));
            Assert.Equal(lengths, result.Select(res => res.Count()));
        }

        private static async Task RunWithTimeout(Task task, int timeoutMs)
        {
            await Task.WhenAny(task, Task.Delay(timeoutMs));
            Assert.True(task.IsCompleted);
        }

        [Fact(Timeout = 10000)]
        public static async Task TestPeriodicScheduler()
        {
            using var source = new CancellationTokenSource();
            using var scheduler = new PeriodicScheduler(source.Token);

            int periodicRuns = 0;

            // Schedule periodic
            scheduler.SchedulePeriodicTask("periodic", TimeSpan.FromMilliseconds(100), token =>
            {
                periodicRuns++;
                return Task.CompletedTask;
            });
            Assert.Throws<InvalidOperationException>(() => scheduler.SchedulePeriodicTask("periodic", TimeSpan.Zero, null));

            int singleRuns = 0;
            // Schedule single
            scheduler.ScheduleTask("single", token =>
            {
                singleRuns++;
                return Task.CompletedTask;
            });

            // Schedule interally looping task
            bool shouldLoop = true;
            scheduler.ScheduleTask("intLoop", async token =>
            {
                while (shouldLoop)
                {
                    await Task.Delay(100);
                }
            });
            Assert.Throws<InvalidOperationException>(() => scheduler.ScheduleTask("intLoop", null));

            // Wait for single to terminate
            await RunWithTimeout(scheduler.WaitForTermination("single"), 1000);
            Assert.Equal(1, singleRuns);

            // Wait for internally looping to terminate
            var intTask = scheduler.WaitForTermination("intLoop");
            shouldLoop = false;
            await RunWithTimeout(intTask, 1000);


            // pause periodic
            await Task.Delay(500);
            Assert.True(periodicRuns > 1);
            scheduler.PauseTask("periodic", true);
            int numRuns = periodicRuns;
            await Task.Delay(500);
            // It might run once more, if it was already scheduled to run
            Assert.True(periodicRuns <= numRuns + 1);


            // Exit periodic and wait
            await RunWithTimeout(scheduler.ExitAndWaitForTermination("periodic"), 1000);
            source.Cancel();

            await RunWithTimeout(scheduler.WaitForAll(), 1000);
        }
    }
}