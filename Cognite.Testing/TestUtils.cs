using System;
using System.Linq;
using System.Threading.Tasks;
using Cognite.Extractor.Common;
using Xunit;
using Xunit.Sdk;

namespace Cognite.Extractor.Testing
{
    /// <summary>
    /// Collection of static utility methods for testing
    /// </summary>
    public static class TestUtils
    {
        /// <summary>
        /// Returns a task that polls an asynchronous method every 200 milliseconds and returns once it
        /// is true. This throws an exception if the condition does not become true within <paramref name="seconds"/>.
        /// </summary>
        /// <param name="valueMethod">Method to call</param>
        /// <param name="condition">Method to check condition</param>
        /// <param name="seconds">Number of seconds to wait before failing</param>
        /// <param name="assertion">Method returning a string to include in the thrown exception</param>
        /// <exception cref="ArgumentNullException">if condition or assertion are null</exception>
        /// <exception cref="TrueException">
        /// If <paramref name="condition"/> does not become true within <paramref name="seconds"/>
        /// </exception>
        public static async Task<T> WaitForCondition<T>(Func<Task<T>> valueMethod, Func<T, bool> condition, int seconds, Func<string> assertion)
        {
            if (valueMethod == null) throw new ArgumentNullException(nameof(valueMethod));
            if (condition == null) throw new ArgumentNullException(nameof(condition));
            if (assertion == null) throw new ArgumentNullException(nameof(assertion));
            bool triggered = false;
            int i;
            T val = default;
            for (i = 0; i < seconds * 5; i++)
            {
                val = await valueMethod().ConfigureAwait(false);
                if (condition(val))
                {
                    triggered = true;
                    break;
                }

                await Task.Delay(200).ConfigureAwait(false);
            }

            Assert.True(triggered, assertion());
            return val!;
        }
        /// <summary>
        /// Returns a task that polls an asynchronous method every 200 milliseconds and returns once it
        /// is true. This throws an exception if the condition does not become true within <paramref name="seconds"/>.
        /// </summary>
        /// <param name="valueMethod">Method to call</param>
        /// <param name="condition">Method to check condition</param>
        /// <param name="seconds">Number of seconds to wait before failing</param>
        /// <param name="assertion">String to include in the thrown exception</param>
        /// <exception cref="ArgumentNullException">if condition or assertion are null</exception>
        /// <exception cref="TrueException">
        /// If <paramref name="condition"/> does not become true within <paramref name="seconds"/>
        /// </exception>
        public static async Task<T> WaitForCondition<T>(Func<Task<T>> valueMethod, Func<T, bool> condition, int seconds, string assertion = "Expected condition to trigger")
        {
            return await WaitForCondition<T>(valueMethod, condition, seconds, () => assertion).ConfigureAwait(false);
        }
        /// <summary>
        /// Returns a task that polls an asynchronous method every 200 milliseconds and returns once it
        /// is true. This throws an exception if the condition does not become true within <paramref name="seconds"/>.
        /// </summary>
        /// <param name="condition">Method to poll</param>
        /// <param name="seconds">Number of seconds to wait before failing</param>
        /// <param name="assertion">Method returning a string to include in the thrown exception</param>
        /// <exception cref="ArgumentNullException">if condition or assertion are null</exception>
        /// <exception cref="TrueException">
        /// If <paramref name="condition"/> does not become true within <paramref name="seconds"/>
        /// </exception>
        public static async Task WaitForCondition(Func<Task<bool>> condition, int seconds, Func<string> assertion)
        {
            await WaitForCondition(condition, (bool x) => x == true, seconds, assertion).ConfigureAwait(false);
        }

        /// <summary>
        /// Returns a task that polls a method every 200 milliseconds and returns once it
        /// is true. This throws an exception if the condition does not become true within <paramref name="seconds"/>.
        /// </summary>
        /// <param name="condition">Method to poll</param>
        /// <param name="seconds">Number of seconds to wait before failing</param>
        /// <param name="assertion">String to include in the thrown exception</param>
        /// <exception cref="ArgumentNullException">if condition or assertion are null</exception>
        /// <exception cref="TrueException">
        /// If <paramref name="condition"/> does not become true within <paramref name="seconds"/>
        /// </exception>
        public static Task WaitForCondition(Func<bool> condition, int seconds,
            string assertion = "Expected condition to trigger")
        {
            return WaitForCondition(() => Task.FromResult(condition()), seconds, () => assertion);
        }

        /// <summary>
        /// Returns a task that polls a method every 200 milliseconds and returns once it
        /// is true. This throws an exception if the condition does not become true within <paramref name="seconds"/>.
        /// </summary>
        /// <param name="condition">Method to poll</param>
        /// <param name="seconds">Number of seconds to wait before failing</param>
        /// <param name="assertion">Method returning a string to include in the thrown exception</param>
        /// <exception cref="ArgumentNullException">if condition or assertion are null</exception>
        /// <exception cref="TrueException">
        /// If <paramref name="condition"/> does not become true within <paramref name="seconds"/>
        /// </exception>
        public static Task WaitForCondition(Func<bool> condition, int seconds,
            Func<string> assertion)
        {
            return WaitForCondition(() => Task.FromResult(condition()), seconds, assertion);
        }

        /// <summary>
        /// Returns a task that polls an asynchronous method every 200 milliseconds and returns once it
        /// is true. This throws an exception if the condition does not become true within <paramref name="seconds"/>.
        /// </summary>
        /// <param name="condition">Method to poll</param>
        /// <param name="seconds">Number of seconds to wait before failing</param>
        /// <param name="assertion">String to include in the thrown exception</param>
        /// <exception cref="ArgumentNullException">if condition or assertion are null</exception>
        /// <exception cref="TrueException">
        /// If <paramref name="condition"/> does not become true within <paramref name="seconds"/>
        /// </exception>
        public static Task WaitForCondition(Func<Task<bool>> condition, int seconds,
            string assertion = "Expected condition to trigger")
        {
            return WaitForCondition(condition, seconds, () => assertion);
        }

        /// <summary>
        /// Wait for task to complete or <paramref name="seconds"/>.
        /// Throws an exception if the task did not complete in time.
        ///
        /// Will re-throw any exception from the task, after simplifying it if it is an AggregateException.
        /// </summary>
        /// <param name="task">Task to wait for.</param>
        /// <param name="seconds"></param>
        /// <exception cref="TrueException">If the task does not complete in time</exception>
        public static async Task RunWithTimeout(Task task, int seconds)
        {
            if (task == null) throw new ArgumentNullException(nameof(task));
            await Task.WhenAny(task, Task.Delay(TimeSpan.FromSeconds(seconds))).ConfigureAwait(false);
            Assert.True(task.IsCompleted, "Task did not complete in time");
            if (task.IsFaulted)
            {
                // Rethrow exception preserving stack trace
                var exc = CommonUtils.SimplifyException(task.Exception!);
                System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(exc).Throw();
            }
        }

        /// <summary>
        /// Wait for task to complete or <paramref name="seconds"/>.
        /// Throws an exception if the task did not complete in time.
        /// </summary>
        /// <param name="action">Method returning task to wait for.</param>
        /// <param name="seconds"></param>
        /// <exception cref="TrueException">If the task does not complete in time</exception>
        public static Task RunWithTimeout(Func<Task> action, int seconds)
        {
            if (action == null) throw new ArgumentNullException(nameof(action));
            return RunWithTimeout(action(), seconds);
        }

        /// <summary>
        /// Generate a random string, for testing.
        /// </summary>
        /// <param name="prefix">Constant prefix, should describe which application
        /// created the data.</param>
        /// <param name="numChars">Number of characters in the random string,
        /// 5 is plenty unless this is generated millions of times per test run.</param>
        /// <returns></returns>
        public static string AlphaNumericPrefix(string prefix, int numChars = 5)
        {
            const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
            Random random = new Random();
            return prefix + new string(Enumerable.Repeat(chars, numChars)
              .Select(s => s[random.Next(s.Length)]).ToArray());
        }
    }
}
