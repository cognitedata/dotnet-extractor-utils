using System;
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

        /// <summary>
        /// Reference to the total capacity of the counter.
        /// </summary>
        public int Capacity { get; private set; }

        private readonly object _lock = new object();
        /// <summary>
        /// Resource counter constructor.
        /// </summary>
        /// <param name="initial">Number of resources initially available. Can safely be increased by calling Free</param>
        public BlockingResourceCounter(int initial)
        {
            Count = initial;
            Capacity = initial;
        }

        /// <inheritdocs />
        public Task<int> Take(int count, bool shouldBlock)
        {
            if (count < 0) throw new ArgumentException("count must be greater than or equal to 0");
            if (count == 0) return Task.FromResult(0);
            return Task.Run(() =>
            {
                lock (_lock)
                {
                    while (Count <= 0 && shouldBlock)
                    {
                        Monitor.Wait(_lock);
                    }

                    int toTake = Math.Min(Count, count);
                    Count -= toTake;
                    return toTake;
                }
            });
        }

        /// <summary>
        /// Set the capacity of the counter, may set the current count to less than 0.
        /// </summary>
        /// <param name="newCapacity">New capacity</param>
        /// <exception cref="ArgumentException">If <paramref name="newCapacity"/> is less than 0</exception>
        public void SetCapacity(int newCapacity)
        {
            if (newCapacity < 0) throw new ArgumentException("newCapacity must be greater than or equal to 0");
            lock (_lock)
            {
                // Modify the count by the diff in capacity. If the capacity increases,
                // increment the number of available resources.
                var diff = newCapacity - Capacity;
                Capacity = newCapacity;
                Count += diff;

                Monitor.PulseAll(_lock);
            }
        }

        /// <inheritdocs />
        public void Free(int count)
        {
            if (count < 0) throw new ArgumentException("count must be greater than or equal to 0");
            lock (_lock)
            {
                Count += count;
                Monitor.PulseAll(_lock);
            }
        }
    }
}
