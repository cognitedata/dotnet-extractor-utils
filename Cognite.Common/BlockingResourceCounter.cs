using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.Extractor.Common
{
    /// <summary>
    /// Interface for generic common resource counter, which allows instant freeing, and asynchronous taking.
    /// </summary>
    public interface IResourceCounter
    {
        /// <summary>
        /// Take <paramref name="count"/> instances of the resource.
        /// Returns at least 1, if there are zero instances available, blocks until there are.
        /// </summary>
        /// <param name="count">Maximum number to take</param>
        /// <returns>The number of resources granted. At least 1, but no more than <paramref name="count"/></returns>
        Task<int> Take(int count);
        /// <summary>
        /// Releases <paramref name="count"/> resources.
        /// Behavior is not defined if this number was not taken before.
        /// </summary>
        /// <param name="count">Number of resources to free.</param>
        void Free(int count);
    }



    /// <summary>
    /// Simple construct to count number of available instances of some shared resource.
    /// </summary>
    public class BlockingResourceCounter : IResourceCounter
    {
        /// <summary>
        /// Unsafe reference to the remaining resources.
        /// Do not use this for thread safe logic.
        /// </summary>
        public int Count { get; private set; }

        private readonly object _lock = new object();
        /// <summary>
        /// Resource counter constructor.
        /// </summary>
        /// <param name="initial">Number of resources initially available. Can safely be increased by calling Free</param>
        public BlockingResourceCounter(int initial)
        {
            Count = initial;
        }

        /// <inheritdocs />
        public Task<int> Take(int count)
        {
            return Task.Run(() =>
            {
                lock (_lock)
                {
                    while (Count == 0)
                    {
                        Monitor.Wait(_lock);
                    }

                    int toTake = Math.Min(Count, count);
                    Count -= toTake;
                    return toTake;
                }
            });
        }

        /// <inheritdocs />
        public void Free(int count)
        {
            lock (_lock)
            {
                Count += count;
                Monitor.Pulse(_lock);
            }
        }
    }
}
