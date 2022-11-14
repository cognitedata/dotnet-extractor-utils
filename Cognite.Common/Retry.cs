using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.Extractor.Common
{
    /// <summary>
    /// Configuration for utility methods in the RetryUtil class. This can be used in config files for user-configured retries.
    /// </summary>
    public class RetryUtilConfig
    {
        /// <summary>
        /// Value of the timeout parameter.
        /// </summary>
        public TimeSpanWrapper TimeoutValue { get; } = new TimeSpanWrapper(false, "s", "0");
        /// <summary>
        /// Global timeout. After this much time has passed, new retries will not be started.
        /// </summary>
        public string Timeout
        {
            get => TimeoutValue.RawValue;
            set => TimeoutValue.RawValue = value;
        }

        /// <summary>
        /// Maximum number of attempts. 0 or less retries forever, 1 does not retry.
        /// </summary>
        public int MaxTries { get; set; } = 5;

        /// <summary>
        /// Value of the MaxDelay parameter
        /// </summary>
        public TimeSpanWrapper MaxDelayValue { get; } = new TimeSpanWrapper(false, "s", "0");

        /// <summary>
        /// Maximum delay between attempts using exponential backoff.
        /// </summary>
        public string MaxDelay
        {
            get => MaxDelayValue.RawValue;
            set => MaxDelayValue.RawValue = value;
        }

        /// <summary>
        /// Value of the initial delay parameter.
        /// </summary>
        public TimeSpanWrapper InitialDelayValue { get; } = new TimeSpanWrapper(true, "ms", "500ms");

        /// <summary>
        /// Initial delay used for exponential backoff. Time between each retry is calculated as
        /// `min(max-delay, initial-delay * 2 ^ retry)`, where 0 is treated as infinite for `max-delay`.
        /// The maximum delay is about 10 minutes (13 retries).
        /// </summary>
        public string InitialDelay
        {
            get => InitialDelayValue.RawValue;
            set => InitialDelayValue.RawValue = value;
        }
    }

    /// <summary>
    /// Utilities for retries.
    /// </summary>
    public static class RetryUtil
    {
        /// <summary>
        /// Retry the given method based on <paramref name="config"/>.
        /// </summary>
        /// <param name="name">Name of the task being retried, for logging.</param>
        /// <param name="generator">Method returning a task, called once per retry.</param>
        /// <param name="config">Retry config</param>
        /// <param name="shouldRetry">A method returning true if the method should continue after the given exception was thrown.</param>
        /// <param name="logger">Logger logging information about retries.</param>
        /// <param name="token">Cancellation token</param>
        /// <exception cref="ArgumentNullException"></exception>
        public static async Task RetryAsync(
            string name,
            Func<Task> generator,
            RetryUtilConfig config,
            Func<Exception, bool> shouldRetry,
            ILogger logger,
            CancellationToken token)
        {
            if (config == null) throw new ArgumentNullException(nameof(config));
            if (generator == null) throw new ArgumentNullException(nameof(generator));
            if (shouldRetry == null) throw new ArgumentNullException(nameof(shouldRetry));
            int tries = 0;
            DateTime start = DateTime.UtcNow;

            while (true)
            {
                logger.LogInformation("Run task {Task} {Tries}/{Max}. Elapsed: {Time}", name, tries, config.MaxTries, DateTime.UtcNow - start);
                try
                {
                    await generator().ConfigureAwait(false);
                    return;
                }
                catch (Exception ex)
                {
                    if (shouldRetry(ex) && (tries < config.MaxTries || config.MaxTries == 0) && ((DateTime.UtcNow - start) < config.TimeoutValue.Value || config.TimeoutValue.Value == Timeout.InfiniteTimeSpan))
                    {
                        var delay = CogniteTime.Min(config.MaxDelayValue.Value, TimeSpan.FromTicks(config.InitialDelayValue.Value.Ticks * (int)Math.Pow(2, Math.Min(tries, 13))));
                        logger.LogTrace(ex, "Operation {Op} failed with error {Message}", name, ex.Message);
                        logger.LogDebug("Operation {Op} failed with error {Message}. Retrying after {Time}", name, ex.Message, delay);
                        await Task.Delay(delay, token).ConfigureAwait(false);
                    }
                    else
                    {
                        throw;
                    }
                }
                tries++;
                token.ThrowIfCancellationRequested();
            }
        }

        /// <summary>
        /// Retry the given method based on <paramref name="config"/>.
        /// </summary>
        /// <param name="name">Name of the task being retried, for logging.</param>
        /// <param name="generator">Method returning a task, called once per retry.</param>
        /// <param name="config">Retry config</param>
        /// <param name="token">Cancellation token</param>
        public static Task RetryAsync(string name, Func<Task> generator, RetryUtilConfig config, CancellationToken token)
        {
            return RetryAsync(name, generator, config, _ => true, NullLogger.Instance, token);
        }

        /// <summary>
        /// Retry the given method based on <paramref name="config"/>.
        /// </summary>
        /// <param name="name">Name of the task being retried, for logging.</param>
        /// <param name="generator">Method called once per retry.</param>
        /// <param name="config">Retry config</param>
        /// <param name="shouldRetry">A method returning true if the method should continue after the given exception was thrown.</param>
        /// <param name="logger">Logger logging information about retries.</param>
        /// <param name="token">Cancellation token</param>
        /// <exception cref="ArgumentNullException"></exception>
        public static Task RetryAsync(string name, Action generator, RetryUtilConfig config, Func<Exception, bool> shouldRetry, ILogger logger, CancellationToken token)
        {
            return RetryAsync(name, () => Task.Run(generator), config, shouldRetry, logger, token);
        }

        /// <summary>
        /// Retry the given method based on <paramref name="config"/>.
        /// </summary>
        /// <param name="name">Name of the task being retried, for logging.</param>
        /// <param name="generator">Method called once per retry.</param>
        /// <param name="config">Retry config</param>
        /// <param name="token">Cancellation token</param>
        public static Task RetryAsync(string name, Action generator, RetryUtilConfig config, CancellationToken token)
        {
            return RetryAsync(name, () => Task.Run(generator), config, token);
        }

        /// <summary>
        /// Retry the given method based on <paramref name="config"/>, return a result of type <typeparamref name="T"/>.
        /// </summary>
        /// <param name="name">Name of the task being retried, for logging.</param>
        /// <param name="generator">Method returning a task, called once per retry.</param>
        /// <param name="config">Retry config</param>
        /// <param name="shouldRetry">A method returning true if the method should continue after the given exception was thrown.</param>
        /// <param name="logger">Logger logging information about retries.</param>
        /// <param name="token">Cancellation token</param>
        /// <exception cref="ArgumentNullException"></exception>
        public static async Task<T> RetryResultAsync<T>(
            string name,
            Func<Task<T>> generator,
            RetryUtilConfig config,
            Func<Exception, bool> shouldRetry,
            ILogger logger,
            CancellationToken token)
        {
            if (config == null) throw new ArgumentNullException(nameof(config));
            if (generator == null) throw new ArgumentNullException(nameof(generator));
            if (shouldRetry == null) throw new ArgumentNullException(nameof(shouldRetry));
            int tries = 0;
            DateTime start = DateTime.UtcNow;

            while (true)
            {
                try
                {
                    return await generator().ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    if (shouldRetry(ex) && (tries < config.MaxTries || config.MaxTries == 0) && ((DateTime.UtcNow - start) < config.TimeoutValue.Value || config.TimeoutValue.Value == Timeout.InfiniteTimeSpan))
                    {
                        var delay = CogniteTime.Min(config.MaxDelayValue.Value, TimeSpan.FromTicks(config.InitialDelayValue.Value.Ticks * (int)Math.Pow(2, Math.Min(tries, 13))));
                        logger.LogTrace(ex, "Operation {Op} failed with error {Message}", name, ex.Message);
                        logger.LogDebug("Operation {Op} failed with error {Message}. Retrying after {Time}", name, ex.Message, delay);
                        await Task.Delay(delay, token).ConfigureAwait(false);
                    }
                    else
                    {
                        throw;
                    }
                }
                tries++;
                token.ThrowIfCancellationRequested();
            }
        }

        /// <summary>
        /// Retry the given method based on <paramref name="config"/>, return a result of type <typeparamref name="T"/>.
        /// </summary>
        /// <param name="name">Name of the task being retried, for logging.</param>
        /// <param name="generator">Method returning a task, called once per retry.</param>
        /// <param name="config">Retry config</param>
        /// <param name="token">Cancellation token</param>
        public static Task<T> RetryResultAsync<T>(string name, Func<Task<T>> generator, RetryUtilConfig config, CancellationToken token)
        {
            return RetryResultAsync(name, generator, config, _ => true, NullLogger.Instance, token);
        }

        /// <summary>
        /// Retry the given method based on <paramref name="config"/>, return a result of type <typeparamref name="T"/>.
        /// </summary>
        /// <param name="name">Name of the task being retried, for logging.</param>
        /// <param name="generator">Method called once per retry.</param>
        /// <param name="config">Retry config</param>
        /// <param name="shouldRetry">A method returning true if the method should continue after the given exception was thrown.</param>
        /// <param name="logger">Logger logging information about retries.</param>
        /// <param name="token">Cancellation token</param>
        /// <exception cref="ArgumentNullException"></exception>
        public static Task<T> RetryResultAsync<T>(string name, Func<T> generator, RetryUtilConfig config, Func<Exception, bool> shouldRetry, ILogger logger, CancellationToken token)
        {
            return RetryResultAsync(name, () => Task.Run(generator), config, shouldRetry, logger, token);
        }

        /// <summary>
        /// Retry the given method based on <paramref name="config"/>, return a result of type <typeparamref name="T"/>.
        /// </summary>
        /// <param name="name">Name of the task being retried, for logging.</param>
        /// <param name="generator">Method called once per retry.</param>
        /// <param name="config">Retry config</param>
        /// <param name="token">Cancellation token</param>
        public static Task<T> RetryResultAsync<T>(string name, Func<T> generator, RetryUtilConfig config, CancellationToken token)
        {
            return RetryResultAsync(name, () => Task.Run(generator), config, token);
        }
    }
}
