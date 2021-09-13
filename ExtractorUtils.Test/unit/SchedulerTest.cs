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

    class TestScheduler : OperationScheduler<SchedulerItem>
    {
        public int Aborted { get; private set; }
        public int Capacity { get; set; }
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
            CancellationToken token) : base(initialItems, throttler, chunkSize, token)
        {

        }


        protected override void AbortChunk(IChunk<SchedulerItem> chunk, CancellationToken token)
        {
            Aborted++;
        }

        protected override async Task ConsumeChunk(IChunk<SchedulerItem> chunk, CancellationToken token)
        {
            await Task.Delay(10);
        }

        protected override void FreeCapacity(int freed)
        {
            Capacity += freed;
        }

        protected override Task<int> GetCapacity(int requested)
        {
            int toAlloc = Math.Min(Capacity, requested);
            Capacity -= toAlloc;
            return Task.FromResult(toAlloc);
        }

        protected override IChunk<SchedulerItem> GetChunk(IEnumerable<SchedulerItem> items)
        {
            return new SchedulerChunk
            {
                Items = items
            };
        }

        protected override IEnumerable<SchedulerItem> HandleTaskResult(IChunk<SchedulerItem> chunk, CancellationToken token)
        {
            CountChunks++;
            foreach (var item in chunk.Items)
            {
                item.NumRemaining--;
                if (item.DepthRemaining > 0)
                {
                    for (int i = 0; i < item.NumChildren; i++)
                    {
                        item.FoundChildren++;
                        CountItems++;
                        yield return new SchedulerItem
                        {
                            DepthRemaining = item.DepthRemaining - 1,
                            NumChildren = item.NumChildren,
                            NumRemaining = item.MaxRemaining,
                            MaxRemaining = item.MaxRemaining
                        };
                    }
                }
            }
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

            using var scheduler = new TestScheduler(items, 3, throttler, CancellationToken.None);
            scheduler.Capacity = 6;

            await scheduler.RunAsync();

            Assert.True(scheduler.CountChunks >= totalReads / 3);
            Assert.Equal(0, scheduler.Pending);
            Assert.Equal(6, scheduler.Capacity);
            Assert.Equal(totalReads, scheduler.Operations);
            Assert.Equal(totalItems, scheduler.CountItems + 3);
            Assert.Equal(totalItems, scheduler.Total);
            Assert.Equal(totalItems, scheduler.Finished);
        }
    }
}
