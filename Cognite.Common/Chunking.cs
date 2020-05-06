using System;
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
        /// </summary>
        /// <typeparam name="TKey"></typeparam>
        /// <typeparam name="TVal"></typeparam>
        /// <param name="points"></param>
        /// <param name="maxPerList"></param>
        /// <param name="maxKeys"></param>
        /// <returns></returns>
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
        /// <typeparam name="T"></typeparam>
        /// <param name="input"></param>
        /// <param name="maxSize"></param>
        /// <returns></returns>
        public static IEnumerable<IEnumerable<T>> ChunkBy<T>(this IEnumerable<T> input, int maxSize)
        {
            return input
                .Select((x, i) => new { Index = i, Value = x })
                .GroupBy(x => x.Index / maxSize)
                .Select(x => x.Select(v => v.Value));
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
        /// <param name="generators"></param>
        /// <param name="parallelism"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        public static async Task RunThrottled(
            this IEnumerable<Func<Task>> generators,
            int parallelism,
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
