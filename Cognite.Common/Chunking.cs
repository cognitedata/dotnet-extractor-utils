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
            await RunThrottled(generators, parallelism, null, token);
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
                    var task = await Task.WhenAny(tasks);
                    // will throw exception if the task failed, returns immediately
                    await task;
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
