using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.Extractor.Common
{
    /// <summary>
    /// Support for splitting sequences into sequences of sequences subject to various constraints, as well as running 
    /// a sequence of tasks in parallel.
    /// </summary>
    static public class Chunking
    {
        /// <summary>
        /// Chunk the input [(key, [values])] sequence into a sequence subject to constraints on the number
        /// of keys and total number of values per chunk.
        /// Keys should be unique.
        /// </summary>
        /// <typeparam name="TKey">Type of keys to group by, should be unique</typeparam>
        /// <typeparam name="TVal">Value type</typeparam>
        /// <param name="points">List of (key, [values]) that will be chunked</param>
        /// <param name="maxPerList">Maximum number of entries per list of values in the output</param>
        /// <param name="maxKeys">Maximum number of keys per list of groups in the output</param>
        /// <returns>A chunking of the output subject to <paramref name="maxKeys"/> and <paramref name="maxPerList"/>.</returns>
        public static IEnumerable<IEnumerable<(TKey Key, IEnumerable<TVal> Values)>> ChunkBy<TKey, TVal>(
            this IEnumerable<(TKey, IEnumerable<TVal>)> points, int maxPerList, int maxKeys)
        {
            var current = new List<(TKey, IEnumerable<TVal>)>();
            int count = 0;

            foreach (var (key, values) in points)
            {
                if (!values.Any())
                    continue;

                if (current.Count >= maxKeys)
                {
                    yield return current;
                    current = new List<(TKey, IEnumerable<TVal>)>();
                    count = 0;
                }

                int pcount = values.Count();
                if (count + pcount <= maxPerList)
                {
                    current.Add((key, values));
                    count += pcount;
                    continue;
                }

                // fill up the current batch to max_datapoints data points and keep the remaining data points in current.
                var inCurrent = values.Take(Math.Min(maxPerList - count, pcount)).ToList();
                if (inCurrent.Count > 0)
                {
                    current.Add((key, inCurrent));
                }
                yield return current;
                current = null;

                // inNext can have too many datapoints
                var inNext = values.Skip(inCurrent.Count);
                if (inNext.Any())
                {
                    var chunks = ChunkBy(inNext, maxPerList).Select(chunk => (Key: key, Values: chunk));
                    foreach (var chunk in chunks)
                    {
                        if (chunk.Values.Count() == maxPerList)
                        {
                            yield return new List<(TKey, IEnumerable<TVal>)> { chunk };
                            continue;
                        }
                        if (chunk.Values.Any())
                        {
                            count = chunk.Values.Count();
                            current = new List<(TKey, IEnumerable<TVal>)> { chunk };
                        }
                        break;
                    }
                }
                if (current == null)
                {
                    current = new List<(TKey, IEnumerable<TVal>)>();
                    count = 0;
                }
            }

            if (current.Any())
            {
                yield return current;
            }
        }

        /// <summary>
        /// Chunk the input sequence into a sequence of sequences subject to a constraint on the maximum
        /// number of elements per inner sequence.
        /// </summary>
        /// <typeparam name="T">Input type</typeparam>
        /// <param name="input">List of objects to be chunked</param>
        /// <param name="maxSize">Maximum number of entires per list in the output</param>
        /// <returns>A list of lists where each list is subject to <paramref name="maxSize"/></returns>
        public static IEnumerable<IEnumerable<T>> ChunkBy<T>(this IEnumerable<T> input, int maxSize)
        {
            var list = new List<T>(maxSize);
            foreach (var thing in input)
            {
                list.Add(thing);
                if (list.Count == maxSize)
                {
                    yield return list;
                    list = new List<T>(maxSize);
                }
            }

            if (list.Any())
            {
                yield return list;
            }
        }

        /// <summary>
        /// Chunk a list of entries with some sort of parent-child hierarchy into an ordered list where each element
        /// contains all the parents of the next. The first list has no parents, or the parents are not in the list of given elements.
        /// If <paramref name="maxSize"/> is set larger than 1, the lists are merged so that the first list may contain both
        /// the first and second layers, etc. respecting the maximum number of elements in each chunk.
        /// The final lists may be larger than <paramref name="maxSize"/>, it is only used to merge layers.
        /// This is useful when pushing large numbers of assets to CDF.
        /// </summary>
        /// <typeparam name="T">Element to be grouped</typeparam>
        /// <typeparam name="K">Type used for ids</typeparam>
        /// <param name="input">Input list</param>
        /// <param name="maxSize">Maximum number of elements when merging lists</param>
        /// <param name="idSelector">Function to get id from each element</param>
        /// <param name="parentIdSelector">Function to get parentId from each element</param>
        /// <param name="comparer">Optional equalitycomparer</param>
        /// <returns>A list of lists so that if they are consumed in order, all parents will be pushed ahead of children</returns>
        public static IEnumerable<IEnumerable<T>> ChunkByHierarchy<T, K>(
            this IEnumerable<T> input,
            int maxSize,
            Func<T, K> idSelector,
            Func<T, K> parentIdSelector,
            IEqualityComparer<K> comparer = null)
        {
            if (idSelector == null)
            {
                throw new ArgumentNullException(nameof(idSelector));
            }
            if (parentIdSelector == null)
            {
                throw new ArgumentNullException(nameof(parentIdSelector));
            }

            var eqComparer = comparer ?? EqualityComparer<K>.Default;
            if (!input.Any()) return Enumerable.Empty<IEnumerable<T>>();

            // We need a set of all node-ids
            var nodeSet = new HashSet<K>(input.Select(idSelector));

            // Find all parent-ids, and also identify all root level nodes.
            var layer = new List<T>();
            var children = new Dictionary<K, IList<T>>();
            foreach (var el in input)
            {
                var parentId = parentIdSelector(el);
                if (eqComparer.Equals(parentId, default) || !nodeSet.Contains(parentId))
                {
                    // This is the first layer, we can recursively traverse the tree using this later.
                    layer.Add(el);
                    continue;
                }
                if (!children.TryGetValue(parentId, out var nodes))
                {
                    children[parentId] = nodes = new List<T>();
                }
                nodes.Add(el);
            }
            var levels = new List<List<T>>();
            // For each set of nodes, get their children, then assign them to the iteration number.
            while (layer.Any())
            {
                levels.Add(layer);
                var nextLayer = new List<T>();
                foreach (var el in layer)
                {
                    var id = idSelector(el);
                    if (!nodeSet.Remove(id)) throw new InvalidOperationException("Input is not a tree");
                    if (children.TryGetValue(id, out var next))
                    {
                        nextLayer.AddRange(next);
                    }
                }
                layer = nextLayer;
            }

            return levels.ConservativeMerge(maxSize);
        }
        /// <summary>
        /// Conservatively merge a list of lists, ensuring that no sub-list is split, and no created list is longer than <paramref name="maxSize"/>.
        /// Only subsequent lists are merged.
        /// Example: maxSize = 10, list lengths are 1, 2, 6, 15, 17, 9, 4, 4 yields output lengths 9, 15, 17, 9, 8
        /// </summary>
        /// <typeparam name="T">Type of element</typeparam>
        /// <param name="input">List of lists to merge</param>
        /// <param name="maxSize">Maximum number of entries</param>
        /// <returns>A list of lists, each list may </returns>
        private static IEnumerable<IEnumerable<T>> ConservativeMerge<T>(this IEnumerable<IEnumerable<T>> input, int maxSize)
        {
            if (maxSize <= 1)
            {
                foreach (var chunk in input) yield return chunk;
                yield break;
            }
            var current = new List<T>();
            foreach (var chunk in input)
            {
                if (current.Count + chunk.Count() <= maxSize)
                {
                    current.AddRange(chunk);
                }
                else if (!current.Any())
                {
                    yield return chunk;
                }
                else
                {
                    yield return current;
                    current = new List<T>(chunk);
                }
            }
            if (current.Any())
            {
                yield return current;
            }
        }

        private static List<T> Dequeue<T>(this Queue<T> queue, int numToDequeue)
        {
            List<T> ret = new List<T>();
            while (queue.Any() && ret.Count < numToDequeue)
            {
                ret.Add(queue.Dequeue());
            }
            return ret;
        }

        /// <summary>
        /// Runs the generated tasks in parallell subject to the maximum parallelism.
        /// </summary>
        /// <param name="generators">Tasks to perform</param>
        /// <param name="parallelism">Number of tasks to run in parallel</param>
        /// <param name="token">Cancellation token</param>
        public static async Task RunThrottled(
            this IEnumerable<Func<Task>> generators,
            int parallelism,
            CancellationToken token)
        {
            await RunThrottled(generators, parallelism, null, token).ConfigureAwait(false);
        }

        /// <summary>
        /// Runs the generated tasks in parallell subject to the maximum parallelism.
        /// Call the <paramref name="taskCompletedCallback"/> action for each completed task
        /// </summary>
        /// <param name="generators">Tasks to perform</param>
        /// <param name="parallelism">Number of tasks to run in parallel</param>
        /// <param name="taskCompletedCallback">Action to call on task completion</param>
        /// <param name="token">Cancellation token</param>
        /// <returns></returns>
        public static async Task RunThrottled(
            this IEnumerable<Func<Task>> generators,
            int parallelism,
            Action<Task> taskCompletedCallback,
            CancellationToken token)
        {
            List<Task> tasks = new List<Task>();
            var generatorQueue = new Queue<Func<Task>>(generators);
            int totalTasks = generatorQueue.Count;
            int completedTasks = 0;
            while (generatorQueue.Any() || tasks.Any())
            {
                token.ThrowIfCancellationRequested();
                if (tasks.Any())
                {
                    var task = await Task.WhenAny(tasks).ConfigureAwait(false);
                    // will throw exception if the task failed, returns immediately
                    await task.ConfigureAwait(false);
                    completedTasks++;
                    Debug.Assert(completedTasks <= totalTasks);
                    tasks.Remove(task);
                    if (taskCompletedCallback != null)
                    {
                        taskCompletedCallback(task);
                    }
                }

                Debug.Assert(tasks.Count < parallelism);
                if (generatorQueue.Any())
                {
                    int toInsert = parallelism - tasks.Count;
                    tasks.AddRange(generatorQueue.Dequeue(toInsert).Select(gen => gen()));
                }
            }
        }
    }
}
