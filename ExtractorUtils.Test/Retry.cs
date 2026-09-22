using System;
using System.Threading;
using System.Threading.Tasks;

namespace ExtractorUtils.Test
{
    /// <summary>
    /// Retries eventually-consistent CDF calls (EDG-891). Mirrors cognite-sdk-dotnet's
    /// Retry.RunAsync, plus an optional <c>isDone</c> for calls that fail by returning an
    /// incomplete result rather than throwing.
    /// </summary>
    public static class Retry
    {
        private const int DefaultMaxAttempts = 6;
        private static readonly TimeSpan BaseDelay = TimeSpan.FromMilliseconds(300);
        private static readonly TimeSpan MaxDelay = TimeSpan.FromSeconds(5);

        public static async Task<T> RunAsync<T>(
            Func<Task<T>> action,
            Func<Exception, bool> shouldRetry = null,
            Func<T, bool> isDone = null,
            int maxAttempts = DefaultMaxAttempts,
            CancellationToken cancellationToken = default)
        {
            if (action is null) throw new ArgumentNullException(nameof(action));
            if (maxAttempts < 1) throw new ArgumentOutOfRangeException(nameof(maxAttempts));

            for (var attempt = 1; ; attempt++)
            {
                try
                {
                    var result = await action().ConfigureAwait(false);
                    if (isDone == null || isDone(result) || attempt >= maxAttempts)
                    {
                        return result;
                    }
                }
                catch (Exception ex) when (ex is not OperationCanceledException && attempt < maxAttempts && (shouldRetry?.Invoke(ex) ?? true))
                {
                }
                await Task.Delay(BackoffDelay(attempt), cancellationToken).ConfigureAwait(false);
            }
        }

        /// <summary>Non-generic overload for actions with no return value.</summary>
        public static Task RunAsync(
            Func<Task> action,
            Func<Exception, bool> shouldRetry = null,
            int maxAttempts = DefaultMaxAttempts,
            CancellationToken cancellationToken = default)
        {
            if (action is null) throw new ArgumentNullException(nameof(action));

            return RunAsync<bool>(async () =>
            {
                await action().ConfigureAwait(false);
                return true;
            }, shouldRetry, isDone: null, maxAttempts, cancellationToken);
        }

        /// <summary>Exponential backoff with equal jitter.</summary>
        private static TimeSpan BackoffDelay(int attempt)
        {
            var exponential = Math.Min(MaxDelay.TotalMilliseconds, BaseDelay.TotalMilliseconds * Math.Pow(2, attempt - 1));
            var half = exponential / 2;
            return TimeSpan.FromMilliseconds(half + Random.Shared.NextDouble() * half);
        }
    }
}
