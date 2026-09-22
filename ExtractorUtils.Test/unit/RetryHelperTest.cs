using System;
using System.Threading.Tasks;
using ExtractorUtils.Test;
using Xunit;

namespace ExtractorUtils.Test.Unit
{
    // Unit tests for ExtractorUtils.Test.Retry, the shared retry-with-backoff helper
    // integration tests use to ride out CDF's eventual consistency (EDG-891). Kept separate
    // from RetryTest.cs, which covers the unrelated production Cognite.Extractor.Common retry
    // utility.
    public class RetryHelperTest
    {
        [Fact]
        public async Task TestRunAsyncReturnsImmediatelyWhenDoneOnFirstTry()
        {
            var attempts = 0;
            var result = await Retry.RunAsync(
                () => { attempts++; return Task.FromResult(42); },
                isDone: r => r == 42);

            Assert.Equal(42, result);
            Assert.Equal(1, attempts);
        }

        [Fact]
        public async Task TestRunAsyncRetriesUntilIsDoneAccepts()
        {
            var attempts = 0;
            var result = await Retry.RunAsync(
                () => { attempts++; return Task.FromResult(attempts); },
                isDone: r => r >= 3,
                maxAttempts: 5);

            Assert.Equal(3, result);
            Assert.Equal(3, attempts);
        }

        [Fact]
        public async Task TestRunAsyncReturnsLastResultWhenAttemptsExhaustedWithoutThrowing()
        {
            // isDone never accepts -- RunAsync must give up gracefully and return the last-seen
            // result (so the caller's own assertion fails normally) rather than throwing itself.
            var attempts = 0;
            var result = await Retry.RunAsync(
                () => { attempts++; return Task.FromResult(attempts); },
                isDone: r => false,
                maxAttempts: 3);

            Assert.Equal(3, result);
            Assert.Equal(3, attempts);
        }

        [Fact]
        public async Task TestRunAsyncRetriesOnExceptionByDefault()
        {
            var attempts = 0;
            var result = await Retry.RunAsync<int>(() =>
            {
                attempts++;
                if (attempts < 3) throw new InvalidOperationException("not ready yet");
                return Task.FromResult(99);
            }, maxAttempts: 5);

            Assert.Equal(99, result);
            Assert.Equal(3, attempts);
        }

        [Fact]
        public async Task TestRunAsyncPropagatesExceptionOnceAttemptsExhausted()
        {
            var attempts = 0;
            await Assert.ThrowsAsync<InvalidOperationException>(() => Retry.RunAsync<int>(() =>
            {
                attempts++;
                throw new InvalidOperationException("still not ready");
            }, maxAttempts: 2));

            Assert.Equal(2, attempts);
        }

        [Fact]
        public async Task TestRunAsyncDoesNotRetryWhenShouldRetryRejects()
        {
            // shouldRetry rejecting the exception means it must propagate on the very first
            // attempt, without waiting for maxAttempts to be exhausted.
            var attempts = 0;
            await Assert.ThrowsAsync<ArgumentException>(() => Retry.RunAsync<int>(() =>
            {
                attempts++;
                throw new ArgumentException("not retryable");
            }, shouldRetry: ex => ex is InvalidOperationException, maxAttempts: 5));

            Assert.Equal(1, attempts);
        }

        [Fact]
        public async Task TestRunAsyncNeverRetriesCancellation()
        {
            // Cancellation must always propagate immediately, regardless of shouldRetry --
            // extending a poll past the caller's own cancellation would be wrong.
            var attempts = 0;
            await Assert.ThrowsAsync<OperationCanceledException>(() => Retry.RunAsync<int>(() =>
            {
                attempts++;
                throw new OperationCanceledException();
            }, shouldRetry: _ => true, maxAttempts: 5));

            Assert.Equal(1, attempts);
        }

        [Fact]
        public async Task TestRunAsyncNonGenericOverloadRetriesOnException()
        {
            var attempts = 0;
            await Retry.RunAsync(() =>
            {
                attempts++;
                if (attempts < 2) throw new InvalidOperationException("not ready yet");
                return Task.CompletedTask;
            }, maxAttempts: 3);

            Assert.Equal(2, attempts);
        }
    }
}
