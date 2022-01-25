using System;
using System.Threading.Tasks;
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
        /// <param name="condition">Method to poll</param>
        /// <param name="seconds">Number of seconds to wait before failing</param>
        /// <param name="assertion">Method returning a string to include in the thrown exception</param>
        /// <exception cref="ArgumentNullException">if condition or assertion are null</exception>
        /// <exception cref="TrueException">
        /// If <paramref name="condition"/> does not become true within <paramref name="seconds"/>
        /// </exception>
        public static async Task WaitForCondition(Func<Task<bool>> condition, int seconds, Func<string> assertion)
        {
            if (condition == null) throw new ArgumentNullException(nameof(condition));
            if (assertion == null) throw new ArgumentNullException(nameof(assertion));
            bool triggered = false;
            int i;
            for (i = 0; i < seconds * 5; i++)
            {
                if (await condition().ConfigureAwait(false))
                {
                    triggered = true;
                    break;
                }

                await Task.Delay(200).ConfigureAwait(false);
            }

            Assert.True(triggered, assertion());
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
        /// </summary>
        /// <param name="task">Task to wait for.</param>
        /// <param name="seconds"></param>
        /// <exception cref="TrueException">If the task does not complete in time</exception>
        public static async Task RunWithTimeout(Task task, int seconds)
        {
            if (task == null) throw new ArgumentNullException(nameof(task));
            await Task.WhenAny(task, Task.Delay(TimeSpan.FromSeconds(seconds))).ConfigureAwait(false);
            Assert.True(task.IsCompleted, "Task did not complete in time");
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
    }
}
