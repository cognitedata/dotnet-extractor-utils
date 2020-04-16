using System;
using System.Collections.Generic;
using System.Linq;
using System.Diagnostics;
using System.Threading.Tasks;
using System.Threading;

namespace ExtractorUtils
{
    static public class Chunking
    {
        public static IEnumerable<IEnumerable<T>> Chunked<T>(this IEnumerable<T> things, int itemsPerChunk)
        {
            var list = new List<T>(itemsPerChunk);
            foreach (var thing in things)
            {
                list.Add(thing);
                if (list.Count == itemsPerChunk)
                {
                    yield return list;
                    list = new List<T>(itemsPerChunk);
                }
            }

            if (list.Any())
            {
                yield return list;
            }
        }

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

        public static IEnumerable<IEnumerable<T>> ChunkBy<T>(IEnumerable<T> input, int maxSize)
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

        public static async Task RunThrottled(
            this IEnumerable<Func<Task>> generators,
            int limit,
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

                Debug.Assert(tasks.Count < limit);
                if (generatorQueue.Any())
                {
                    int toInsert = limit - tasks.Count;
                    tasks.AddRange(generatorQueue.Dequeue(toInsert).Select(gen => gen()));
                }
            }
        }

    }
}
