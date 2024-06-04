using Cognite.Extractor.Common;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace ExtractorUtils.Test.Unit
{
    class SchedulerItem
    {
        public int NumRemaining { get; set; }
        public int NumChildren { get; set; }
        public int DepthRemaining { get; set; }
        public int MaxRemaining { get; set; }
        public int FoundChildren { get; set; }
    }

    class SchedulerChunk : IChunk<SchedulerItem>
    {
        public IEnumerable<SchedulerItem> Items { get; set; }

        public Exception Exception { get; set; }

        public bool Completed(SchedulerItem item)
        {
            return item.NumRemaining == 0;
        }
    }

    class TestScheduler : SharedResourceScheduler<SchedulerItem>
    {
        public int Aborted { get; private set; }
        public int Pending { get; set; }
        public int Operations { get; set; }
        public int Finished { get; set; }
        public int Total { get; set; }
        public int CountItems { get; set; }
        public int CountChunks { get; set; }

        public TestScheduler(
            IEnumerable<SchedulerItem> initialItems,
            int chunkSize,
            TaskThrottler throttler,
            IResourceCounter resource,
            CancellationToken token) : base(initialItems, throttler, chunkSize, resource, token)
        {
        }


        protected override Task AbortChunk(IChunk<SchedulerItem> chunk, CancellationToken token)
        {
            Aborted++;
            return Task.CompletedTask;
        }

        protected override async Task ConsumeChunk(IChunk<SchedulerItem> chunk, CancellationToken token)
        {
            await Task.Delay(10);
        }

        protected override IChunk<SchedulerItem> GetChunk(IEnumerable<SchedulerItem> items)
        {
            return new SchedulerChunk
            {
                Items = items
            };
        }

        protected override Task<IEnumerable<SchedulerItem>> HandleTaskResult(IChunk<SchedulerItem> chunk, CancellationToken token)
        {
            CountChunks++;
            var res = new List<SchedulerItem>();
            foreach (var item in chunk.Items)
            {
                item.NumRemaining--;
                if (item.DepthRemaining > 0)
                {
                    for (int i = 0; i < item.NumChildren; i++)
                    {
                        item.FoundChildren++;
                        CountItems++;
                        res.Add(new SchedulerItem
                        {
                            DepthRemaining = item.DepthRemaining - 1,
                            NumChildren = item.NumChildren,
                            NumRemaining = item.MaxRemaining,
                            MaxRemaining = item.MaxRemaining
                        });
                    }
                }
            }
            return Task.FromResult(res as IEnumerable<SchedulerItem>);
        }

        protected override void OnIteration(int pending, int operations, int finished, int total)
        {
            Pending = pending;
            Operations = operations;
            Finished = finished;
            Total = total;
        }
    }

    public class SchedulerTest
    {
        [Fact]
        public async Task TestScheduler()
        {
            using var throttler = new TaskThrottler(2);
            var items = new[]
            {
                // 6 children with 6 children each = 1 + 6 + 6 * 6 = 43
                new SchedulerItem
                {
                    DepthRemaining = 2,
                    NumChildren = 2,
                    NumRemaining = 3,
                    MaxRemaining = 3
                },
                // Just 1 item
                new SchedulerItem
                {
                    DepthRemaining = 0,
                    NumChildren = 2,
                    NumRemaining = 1,
                    MaxRemaining = 1
                },
                // 2 children three times = 1 + 2 + 2 * 2 + 2 * 2 * 2 = 15
                new SchedulerItem
                {
                    DepthRemaining = 3,
                    NumChildren = 1,
                    NumRemaining = 2,
                    MaxRemaining = 2
                }
            };

            int totalItems = 43 + 1 + 15;
            int totalReads = 43 * 3 + 1 + 15 * 2;

            var resource = new BlockingResourceCounter(6);
            using var scheduler = new TestScheduler(items, 3, throttler, resource, CancellationToken.None);

            await scheduler.RunAsync();

            Assert.True(scheduler.CountChunks >= totalReads / 3);
            Assert.Equal(0, scheduler.Pending);
            Assert.Equal(6, resource.Count);
            Assert.Equal(totalReads, scheduler.Operations);
            Assert.Equal(totalItems, scheduler.CountItems + 3);
            Assert.Equal(totalItems, scheduler.Total);
            Assert.Equal(totalItems, scheduler.Finished);
        }
        [Fact]
        public async Task TestBlockingResource()
        {
            var resource = new BlockingResourceCounter(5);
            Assert.Equal(3, await resource.Take(3, true));
            Assert.Equal(2, await resource.Take(3, true));
            Assert.Equal(0, await resource.Take(2, false));

            var task = resource.Take(3, true);

            Assert.NotEqual(task, await Task.WhenAny(task, Task.Delay(100)));

            resource.Free(2);

            Assert.Equal(task, await Task.WhenAny(task, Task.Delay(1000)));
            var res = await task;
            Assert.Equal(2, res);

            Assert.Equal(0, await resource.Take(0, true));
            resource.Free(0);
            Assert.Throws<ArgumentException>(() => resource.Free(-1));
        }

        [Fact]
        public async Task TestBlockingResourceIncCapacity()
        {
            var resource = new BlockingResourceCounter(5);
            Assert.Equal(5, await resource.Take(5, true));

            var task = resource.Take(3, true);

            Assert.NotEqual(task, await Task.WhenAny(task, Task.Delay(100)));

            resource.SetCapacity(3);

            Assert.NotEqual(task, await Task.WhenAny(task, Task.Delay(100)));
            Assert.Equal(-2, resource.Count);

            resource.SetCapacity(10);
            Assert.Equal(3, await task);
            Assert.Equal(2, resource.Count);

            resource.SetCapacity(2);
            Assert.Equal(-6, resource.Count);

            var task2 = resource.Take(3, true);

            Assert.NotEqual(task2, await Task.WhenAny(task2, Task.Delay(100)));

            resource.SetCapacity(9);

            Assert.Equal(1, await task2);
        }
    }
}
