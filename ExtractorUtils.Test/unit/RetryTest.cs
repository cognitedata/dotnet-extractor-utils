using System;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extractor.Common;
using Cognite.Extractor.Testing;
using Microsoft.Extensions.Logging;
using Xunit;

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
    }
}
