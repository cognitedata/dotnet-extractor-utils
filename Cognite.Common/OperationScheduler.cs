using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.Extractor.Common
{
    /// <summary>
    /// Interface representing a chunk of items in TaskScheduler
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public interface IChunk<T>
    {
        /// <summary>
        /// Items in chunk
        /// </summary>
        IEnumerable<T> Items { get; }
        /// <summary>
        /// Exception if chunk operation failed
        /// </summary>
        Exception? Exception { get; set; }
        /// <summary>
        /// Return true if the passed item (which is a member of this chunk) is completed
        /// </summary>
        /// <param name="item">Item to verify</param>
        /// <returns>True if T is complete and can be removed</returns>
        bool Completed(T item);
    }

    /// <summary>
    /// Abstraction for handling some kind of recursive exploration.
    /// Option for shared, asynchronous limits, and using a shared TaskThrottler.
    /// </summary>
    /// <typeparam name="T">Type of item to explore</typeparam>
    public abstract class OperationScheduler<T> : IDisposable
    {
        private IEnumerable<T> _activeItems;
        private int _numPending;
        private readonly TaskThrottler _throttler;
        private readonly BlockingCollection<IChunk<T>> _finishedOps = new BlockingCollection<IChunk<T>>();
        private readonly CancellationTokenSource _source;

        private readonly int _chunkSize;

        private int _numRuns;
        private int _numFinished;
        private int _numTotal;
        private bool _disposed;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="initialItems">List of initial items, must be non-empty</param>
        /// <param name="throttler">TaskThrottler to use. Can be shared with other schedulers</param>
        /// <param name="chunkSize">Maximum number of items per chunk</param>
        /// <param name="token">Cancellation token</param>
        public OperationScheduler(IEnumerable<T> initialItems, TaskThrottler throttler, int chunkSize, CancellationToken token)
        {
            _activeItems = initialItems;
            _throttler = throttler;
            _numTotal = _activeItems.Count();
            _chunkSize = chunkSize;
            _source = CancellationTokenSource.CreateLinkedTokenSource(token);
        }
        #region abstract methods
        /// <summary>
        /// Request to start reading <paramref name="requested"/> nodes.
        /// Must return a number greater than 0 and less than <paramref name="requested"/>.
        /// If <paramref name="shouldBlock"/> is true it may block to wait for resources to be freed elsewhere.
        /// </summary>
        /// <param name="requested">Maximum to request</param>
        /// <param name="shouldBlock">True if this should block</param>
        /// <returns>Number greater than 0 and less than <paramref name="requested"/></returns>
        protected abstract Task<int> GetCapacity(int requested, bool shouldBlock);
        /// <summary>
        /// Return used capacity allocated using GetCapacity.
        /// </summary>
        /// <param name="freed">Amount of capacity to free</param>
        protected abstract void FreeCapacity(int freed);

        /// <summary>
        /// Construct a chunk object from a list of items.
        /// </summary>
        /// <param name="items">Items to construct chunk from</param>
        /// <returns></returns>
        protected abstract IChunk<T> GetChunk(IEnumerable<T> items);

        /// <summary>
        /// Method being called from TaskThrottler, operate on chunk and store the result so that it
        /// can be retrieved from the chunk later.
        /// Can safely throw exceptions, they are stored and can be handled in HandleTaskResult later.
        /// </summary>
        /// <param name="chunk">Chunk to consume</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>Task</returns>
        protected abstract Task ConsumeChunk(IChunk<T> chunk, CancellationToken token);

        /// <summary>
        /// Handle the result of a chunk operation. Called from the main loop after completion is reported.
        /// Returns a list of newly discovered items that should be scheduled.
        /// </summary>
        /// <param name="chunk">Chunk to handle</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>New elements</returns>
        protected abstract IEnumerable<T> HandleTaskResult(IChunk<T> chunk, CancellationToken token);

        /// <summary>
        /// Called if the scheduler is aborted before it finishes running.
        /// Handle any cleanup of resources related to the passed chunks.
        /// FreeCapacity is called outside of this method.
        /// </summary>
        /// <param name="chunk">Chunk to free</param>
        /// <param name="token">Cancellation token</param>
        protected abstract void AbortChunk(IChunk<T> chunk, CancellationToken token);

        /// <summary>
        /// Called on each iteration of the scheduler loop, for reporting.
        /// </summary>
        /// <param name="pending">Number of items currently pending</param>
        /// <param name="operations">Number of operations that have been completed thus far</param>
        /// <param name="finished">Number of items that have been finished</param>
        /// <param name="total">Number of items that have been discovered in total</param>
        protected abstract void OnIteration(int pending, int operations, int finished, int total);
        #endregion

        /// <summary>
        /// Get a single chunk from list.
        /// Should not return more items than <paramref name="capacity"/>.
        /// Default implementation chunks by chunkSize and capacity.
        /// </summary>
        /// <param name="items">Items to take from</param>
        /// <param name="capacity">Maximum number to return</param>
        /// <param name="newItems">New list after iterating. Can be a linq expression,
        /// does not need to maintain the same order, but should ensure that started items
        /// are placed first.</param>
        /// <returns>Items in a single new chunk</returns>
        protected virtual IEnumerable<T> GetNextChunk(IEnumerable<T> items, int capacity, out IEnumerable<T> newItems)
        {
            int toTake = _chunkSize > 0 ? Math.Min(capacity, _chunkSize) : capacity;
            var chunk = items.Take(toTake);
            newItems = items.Skip(toTake);
            return chunk;
        }

        /// <summary>
        /// Get next chunks by consuming items from list.
        /// Should not get more items than <paramref name="capacity"/>
        /// Default implementation simply calls GetNextChunk until it returns nothing or
        /// remaining capacity is 0.
        /// </summary>
        /// <param name="items">Items to take from</param>
        /// <param name="capacity">Maximum number to return</param>
        /// <param name="newItems">New list after iterating. Can be a linq expression,
        /// does not need to maintain the same order, but should ensure that started items
        /// are placed first.</param>
        /// <returns>List of new chunks.</returns>
        protected virtual IEnumerable<IChunk<T>> GetNextChunks(IEnumerable<T> items, int capacity, out IEnumerable<T> newItems)
        {
            var chunks = new List<IChunk<T>>();
            IEnumerable<T> chunk;
            do
            {
                chunk = GetNextChunk(items, capacity, out items);
                capacity -= chunk.Count();
                if (chunk.Any())
                {
                    chunks.Add(GetChunk(chunk));
                }
            } while (chunk.Any() && items.Any() && capacity > 0);
            newItems = items;
            return chunks;
        }

        /// <summary>
        /// Read cancellation token source.
        /// </summary>
        /// <returns>Cancellation token source</returns>
        protected virtual CancellationTokenSource GetCancellationTokenSource()
        {
            return _source;
        }

        private async Task ConsumeChunkInternal(IChunk<T> chunk, CancellationToken token)
        {
            try
            {
                await ConsumeChunk(chunk, token).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                chunk.Exception = ex;
            }
            _finishedOps.Add(chunk, token);
        }

        /// <summary>
        /// Start the scheduler loop.
        /// </summary>
        /// <returns> Task which terminates when the scheduler is finished</returns>
        public Task RunAsync()
        {
            return Task.Run(() => Run(), CancellationToken.None);
        }



        /// <summary>
        /// Runs in a single thread, so that logic in the ThreadScheduler is thread safe.
        /// </summary>
        private void Run()
        {
            var capacity = GetCapacity(_activeItems.Count(), true).Result;
            var chunks = GetNextChunks(_activeItems, capacity, out _activeItems).ToList();
            foreach (var chunk in chunks)
            {
                _numPending += chunk.Items.Count();
            }

            // Number of items in the pending list that have not been freed.
            int numContinued = 0;

            while ((_numPending > 0 || chunks.Any()) && !_source.Token.IsCancellationRequested)
            {
                var generators = chunks.Select<IChunk<T>, Func<Task>>(chunk => () => ConsumeChunkInternal(chunk, _source.Token));

                foreach (var generator in generators)
                {
                    _throttler.EnqueueTask(generator);
                }
                chunks.Clear();

                var finished = new List<IChunk<T>>();
                try
                {
                    finished.Add(_finishedOps.Take(_source.Token));
                }
                catch (OperationCanceledException)
                {
                    break;
                }

                while (_finishedOps.TryTake(out var chunk))
                {
                    finished.Add(chunk);
                }

                var toContinue = new List<T>();
                var newItems = new List<T>();
                foreach (var chunk in finished)
                {
                    int numFinished = 0;

                    var next = HandleTaskResult(chunk, _source.Token);
                    foreach (var newItem in next)
                    {
                        newItems.Add(newItem);
                        _numTotal++;
                    }

                    foreach (var item in chunk.Items)
                    {
                        _numRuns++;

                        if (!chunk.Completed(item))
                        {
                            numContinued++;
                            toContinue.Add(item);
                        }
                        else
                        {
                            numFinished++;
                        }
                    }

                    // Free any finished items here
                    _numPending -= numFinished;
                    _numFinished += numFinished;
                    FreeCapacity(numFinished);
                }

                OnIteration(_numPending, _numRuns, _numFinished, _numTotal);

                // Call ToList regularly to not get too many chained LINQ expressions.
                _activeItems = toContinue.Concat(_activeItems).Concat(newItems).ToList();

                // Do not request capacity for items which have not yet been freed.
                int toRequest = _activeItems.Count() - numContinued;
                if (toRequest > 0)
                {
                    // If numContinued is not zero we don't need to block.
                    capacity = GetCapacity(toRequest, numContinued == 0).Result;
                }
                else
                {
                    capacity = 0;
                }

                // We still have capacity for any that have not been freed.
                var nextChunks = GetNextChunks(_activeItems, capacity + numContinued, out _activeItems);
                foreach (var chunk in nextChunks)
                {
                    int count = chunk.Items.Count();
                    if (numContinued > 0)
                    {
                        int contConsumed = Math.Min(numContinued, count);
                        numContinued -= contConsumed;
                        count -= contConsumed;
                    }
                    _numPending += count;

                    chunks.Add(chunk);
                }
            }

            if (chunks.Any())
            {
                foreach (var chunk in chunks)
                {
                    AbortChunk(chunk, _source.Token);
                }
            }
            if (_numPending > 0)
            {
                FreeCapacity(_numPending);
            }
        }

        /// <summary>
        /// Dispose method.
        /// </summary>
        /// <param name="disposing"></param>
        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    _source.Cancel();
                    _source.Dispose();
                }

                _disposed = true;
            }
        }

        void IDisposable.Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }
    }
}
