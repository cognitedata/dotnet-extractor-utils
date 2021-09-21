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
        /// Take up to <paramref name="count"/> instances of the resource.
        /// If <paramref name="shouldBlock"/> is true, returns at least 1 and blocks until
        /// this is possible.
        /// </summary>
        /// <param name="count">Maximum number to take</param>
        /// <param name="shouldBlock">True if this request should block if no resources are available</param>
        /// <returns>The number of resources granted.
        /// If <paramref name="shouldBlock"/> is true then at least 1,
        /// always no more than <paramref name="count"/></returns>
        Task<int> Take(int count, bool shouldBlock);
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
        public Task<int> Take(int count, bool shouldBlock)
        {
            return Task.Run(() =>
            {
                lock (_lock)
                {
                    while (Count == 0 && shouldBlock)
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
