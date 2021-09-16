using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.Extractor.Common
{
    /// <summary>
    /// Abstraction for handling some kind of recursive exploration.
    /// Simply a small extension to <see cref="OperationScheduler{T}"/> that
    /// uses an instance of a <see cref="IResourceCounter"/> for shared capacity.
    /// </summary>
    /// <typeparam name="T">Type of item to explore</typeparam>
    public abstract class SharedResourceScheduler<T> : OperationScheduler<T>
    {
        private readonly IResourceCounter _resource;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="initialItems">List of initial items, must be non-empty</param>
        /// <param name="throttler">TaskThrottler to use. Can be shared with other schedulers</param>
        /// <param name="chunkSize">Maximum number of items per chunk</param>
        /// <param name="resource">Shared resource to limit parallel requests based on</param>
        /// <param name="token">Cancellation token</param>
        public SharedResourceScheduler(
            IEnumerable<T> initialItems,
            TaskThrottler throttler,
            int chunkSize,
            IResourceCounter resource,
            CancellationToken token)
            : base(initialItems, throttler, chunkSize, token)
        {
            if (resource == null) throw new ArgumentNullException(nameof(resource));
            _resource = resource;
        }

        /// <summary>
        /// Get <paramref name="requested"/> capacity from the shared resource manager.
        /// Returns a number between 1 and <paramref name="requested" />, blocks if
        /// there are zero resources available unless <paramref name="shouldBlock"/> is false,
        /// then returns immediately.
        /// </summary>
        /// <param name="requested">Amount of capacity to request</param>
        /// <param name="shouldBlock">True if this should block</param>
        /// <returns>The number of resources allocated</returns>
        protected override Task<int> GetCapacity(int requested, bool shouldBlock)
        {
            return _resource.Take(requested, shouldBlock);
        }

        /// <summary>
        /// Free <paramref name="freed"/> capacity from the shared resource manager.
        /// </summary>
        /// <param name="freed">Amount of capacity to free</param>
        protected override void FreeCapacity(int freed)
        {
            _resource.Free(freed);
        }
    }
}
