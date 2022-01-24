using Cognite.Extractor.Logging;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;
using Xunit.Sdk;

namespace Cognite.ExtractorUtils.Testing
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
    }
}
