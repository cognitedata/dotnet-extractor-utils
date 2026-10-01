using Cognite.Extractor.Common;
using Cognite.Extractor.Testing;
using Microsoft.Extensions.Logging;
using System;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace ExtractorUtils.Test.Unit
{
    public class RetryTest
    {
        private ILogger _logger;
        public RetryTest(ITestOutputHelper output)
        {
            _logger = TestLogging.GetTestLogger<RetryTest>(output);
        }

        [Fact(Timeout = 20000)]
        public async Task TestRetry()
        {
            int counter = 0;
            void Test()
            {
                if (counter++ < 2) throw new Exception();
            }
            await RetryUtil.RetryAsync("test", Test, new RetryUtilConfig { InitialDelay = "10ms", MaxDelay = "1s", MaxTries = 3 }, _ => true, _logger, CancellationToken.None);
        }

        [Fact(Timeout = 20000)]
        public async Task TestMaxRetry()
        {
            int counter = 0;
            void Test()
            {
                if (counter++ < 2) throw new Exception();
            }
            await Assert.ThrowsAsync<Exception>(async () => await RetryUtil.RetryAsync("test", Test, new RetryUtilConfig { InitialDelay = "10ms", MaxDelay = "1s", MaxTries = 2 }, _ => true, _logger, CancellationToken.None));
        }

        [Fact(Timeout = 20000)]
        public async Task TestTimeout()
        {
            int counter = 0;
            async Task Test()
            {
                await Task.Delay(1000);
                if (counter++ < 2) throw new Exception();
            }
            await Assert.ThrowsAsync<Exception>(async () => await RetryUtil.RetryAsync("test", Test, new RetryUtilConfig { InitialDelay = "10ms", MaxDelay = "1s", MaxTries = 3, Timeout = "500ms" }, _ => true, _logger, CancellationToken.None));
        }

        [Fact(Timeout = 20000)]
        public async Task TestRetryResult()
        {
            int counter = 0;
            int Test()
            {
                if (counter++ < 2) throw new Exception();
                return counter;
            }
            Assert.Equal(3, await RetryUtil.RetryResultAsync("test", Test, new RetryUtilConfig { InitialDelay = "10ms", MaxDelay = "1s", MaxTries = 3 }, _ => true, _logger, CancellationToken.None));
        }

        [Fact(Timeout = 20000)]
        public async Task TestRetryWithJitterStillSucceeds()
        {
            // Jitter only randomises the delay -- retry count and eventual success must be
            // unaffected.
            int counter = 0;
            void Test()
            {
                if (counter++ < 2) throw new Exception();
            }
            await RetryUtil.RetryAsync("test", Test, new RetryUtilConfig { InitialDelay = "10ms", MaxDelay = "50ms", MaxTries = 3, UseJitter = true }, _ => true, _logger, CancellationToken.None);
            Assert.Equal(3, counter);
        }

        [Fact(Timeout = 20000)]
        public async Task TestRetryUntilReturnsImmediatelyWhenDoneOnFirstTry()
        {
            var attempts = 0;
            var result = await RetryUtil.RetryUntilAsync(
                "test",
                () => { attempts++; return Task.FromResult(42); },
                new RetryUtilConfig { InitialDelay = "10ms", MaxDelay = "1s", MaxTries = 5 },
                r => r == 42,
                CancellationToken.None);

            Assert.Equal(42, result);
            Assert.Equal(1, attempts);
        }

        [Fact(Timeout = 20000)]
        public async Task TestRetryUntilRetriesOnIncompleteResult()
        {
            var attempts = 0;
            var result = await RetryUtil.RetryUntilAsync(
                "test",
                () => { attempts++; return Task.FromResult(attempts); },
                new RetryUtilConfig { InitialDelay = "10ms", MaxDelay = "1s", MaxTries = 5 },
                r => r >= 3,
                CancellationToken.None);

            Assert.Equal(3, result);
            Assert.Equal(3, attempts);
        }

        [Fact(Timeout = 20000)]
        public async Task TestRetryUntilReturnsLastResultWhenAttemptsExhaustedWithoutThrowing()
        {
            // isDone never accepts -- must give up gracefully and return the last-seen result
            // (so the caller's own check fails normally) instead of throwing.
            var attempts = 0;
            var result = await RetryUtil.RetryUntilAsync(
                "test",
                () => { attempts++; return Task.FromResult(attempts); },
                new RetryUtilConfig { InitialDelay = "10ms", MaxDelay = "1s", MaxTries = 3 },
                r => false,
                CancellationToken.None);

            Assert.Equal(3, result);
            Assert.Equal(3, attempts);
        }

        [Fact(Timeout = 20000)]
        public async Task TestRetryUntilRetriesOnExceptionByDefault()
        {
            var attempts = 0;
            var result = await RetryUtil.RetryUntilAsync<int>(
                "test",
                () =>
                {
                    attempts++;
                    if (attempts < 3) throw new InvalidOperationException("not ready yet");
                    return Task.FromResult(99);
                },
                new RetryUtilConfig { InitialDelay = "10ms", MaxDelay = "1s", MaxTries = 5 },
                r => true,
                CancellationToken.None);

            Assert.Equal(99, result);
            Assert.Equal(3, attempts);
        }

        [Fact(Timeout = 20000)]
        public async Task TestRetryUntilDoesNotRetryCancellation()
        {
            // Cancellation must always propagate immediately -- extending a poll past the
            // caller's own cancellation would be wrong.
            var attempts = 0;
            await Assert.ThrowsAsync<OperationCanceledException>(() => RetryUtil.RetryUntilAsync<int>(
                "test",
                () =>
                {
                    attempts++;
                    throw new OperationCanceledException();
                },
                new RetryUtilConfig { InitialDelay = "10ms", MaxDelay = "1s", MaxTries = 5 },
                r => true,
                CancellationToken.None));

            Assert.Equal(1, attempts);
        }
    }
}
