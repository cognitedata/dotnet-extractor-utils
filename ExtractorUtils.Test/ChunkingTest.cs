using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Cognite.Extractor.Utils;

namespace ExtractorUtils.Test {

    public static class ChunkingTest {

        [Fact]
        public static void RunThrottledOK()
        {
            var completed = new List<int>();
            var token = CancellationToken.None;
            var generators = Enumerable.Range(1, 5).Select<int, Func<Task>>(
                i => async () => {
                    Console.Out.WriteLine($"Starting {i}");
                    await Task.Delay(i * 100, token);
                    Console.Out.WriteLine($"Completed {i}");
                    completed.Add(i);
                });
            Task.WhenAll(generators.RunThrottled(2, token)).GetAwaiter().GetResult();
            Assert.Equal(5, completed.Count);
            Assert.Equal(15, completed.Sum());
        }

        [Fact]
        public static void RunThrottledException()
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

            Assert.Throws<Exception>(
                delegate {
                    Task.WhenAll(generators.RunThrottled(2, token)).GetAwaiter().GetResult();
                    Thread.Sleep(2000);
                }
            );
            Assert.Equal(2, completed.Count);
            Assert.Equal(3, completed.Sum());
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

    }
}