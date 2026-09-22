using System;
using System.Threading;
using System.Threading.Tasks;

namespace ExtractorUtils.Test
{
    /// <summary>
    /// Shared retry-with-backoff helper for integration tests riding out CDF's eventual
    /// consistency (EDG-891). Mirrors cognite-sdk-dotnet's own Retry.RunAsync (same
    /// action/shouldRetry/maxAttempts shape), extended with an optional <c>isDone</c> predicate
    /// for cases where "not ready yet" comes back as a successful-but-incomplete result rather
    /// than a thrown exception -- e.g. a read that returns an empty list for a just-written
    /// resource, as opposed to a delete that throws while the resource is still in use.
    /// </summary>
    public static class Retry
    {
        private const int DefaultMaxAttempts = 6;
        private static readonly TimeSpan BaseDelay = TimeSpan.FromMilliseconds(300);
        private static readonly TimeSpan MaxDelay = TimeSpan.FromSeconds(5);

        /// <summary>
        /// Runs <paramref name="action"/>, retrying with exponential backoff and jitter until its
        /// result satisfies <paramref name="isDone"/> (if given; otherwise any non-throwing
        /// result counts as done), or until <paramref name="maxAttempts"/> attempts have been
        /// made. Exponential-with-jitter backoff (mirroring cognite-sdk-dotnet's own
        /// Retry.RunAsync) means many tests polling in parallel don't synchronize into a
        /// repeating load spike against the same fixed interval.
        ///
        /// An exception from <paramref name="action"/> (other than cancellation) is treated the
        /// same as "not done yet" and retried on the same schedule, as long as
        /// <paramref name="shouldRetry"/> accepts it (defaulting to retrying on anything). Once
        /// attempts are exhausted, the last exception is left to propagate, or the last-seen
        /// (possibly still-failing) result is returned, so the caller's own assertion or
        /// exception is what actually surfaces -- not a generic "gave up retrying".
        /// </summary>
        /// <param name="action">Operation to run.</param>
        /// <param name="shouldRetry">Optional predicate deciding whether an exception is retryable. Defaults to retrying on any exception (except cancellation).</param>
        /// <param name="isDone">Optional predicate deciding whether a successful result is acceptable. Defaults to accepting any non-throwing result.</param>
        /// <param name="maxAttempts">Maximum number of attempts, including the first one.</param>
        /// <param name="cancellationToken">Token observed between attempts; cancellation is never retried.</param>
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

        /// <summary>
        /// Non-generic overload of <see cref="RunAsync{T}"/> for operations that return no
        /// value -- retries purely on exception, since there is no result to check with
        /// <c>isDone</c>.
        /// </summary>
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

        /// <summary>
        /// Exponential backoff with "equal jitter": half of the exponential delay is fixed, the
        /// other half is randomised, so concurrent retries spread out instead of stampeding.
        /// </summary>
        private static TimeSpan BackoffDelay(int attempt)
        {
            var exponential = Math.Min(MaxDelay.TotalMilliseconds, BaseDelay.TotalMilliseconds * Math.Pow(2, attempt - 1));
            var half = exponential / 2;
            return TimeSpan.FromMilliseconds(half + Random.Shared.NextDouble() * half);
        }
    }
}
