using System;
using Cognite.Extensions;
using Cognite.Extensions.DataModels.CogniteExtractorExtensions;
using Xunit;

namespace ExtractorUtils.Test.Unit
{
    public class BetaResourceParamsTest
    {
        [Fact]
        public void Constructor_WithValidParams_CreatesInstance()
        {
            var chunkSize = 100;
            var throttleSize = 10;
            var retryMode = RetryMode.None;
            var sanitationMode = SanitationMode.None;

            var param = new BetaResourceParams(chunkSize, throttleSize, retryMode, sanitationMode);

            Assert.Equal(chunkSize, param.ChunkSize);
            Assert.Equal(throttleSize, param.ThrottleSize);
            Assert.Equal(retryMode, param.RetryMode);
            Assert.Equal(sanitationMode, param.SanitationMode);
        }

        [Fact]
        public void Constructor_WithZeroChunkSize_ThrowsException()
        {
            var ex = Assert.Throws<ArgumentOutOfRangeException>(
                () => new BetaResourceParams(0, 10, RetryMode.None, SanitationMode.None));
            Assert.Equal("chunkSize", ex.ParamName);
            Assert.Contains("greater than zero", ex.Message);
        }

        [Fact]
        public void Constructor_WithNegativeChunkSize_ThrowsException()
        {
            var ex = Assert.Throws<ArgumentOutOfRangeException>(
                () => new BetaResourceParams(-1, 10, RetryMode.None, SanitationMode.None));
            Assert.Equal("chunkSize", ex.ParamName);
        }

        [Fact]
        public void Constructor_WithZeroThrottleSize_ThrowsException()
        {
            var ex = Assert.Throws<ArgumentOutOfRangeException>(
                () => new BetaResourceParams(100, 0, RetryMode.None, SanitationMode.None));
            Assert.Equal("throttleSize", ex.ParamName);
            Assert.Contains("greater than zero", ex.Message);
        }

        [Fact]
        public void Constructor_WithNegativeThrottleSize_ThrowsException()
        {
            var ex = Assert.Throws<ArgumentOutOfRangeException>(
                () => new BetaResourceParams(100, -5, RetryMode.None, SanitationMode.None));
            Assert.Equal("throttleSize", ex.ParamName);
        }

        [Theory]
        [InlineData(RetryMode.None)]
        [InlineData(RetryMode.OnError)]
        [InlineData(RetryMode.OnErrorKeepDuplicates)]
        [InlineData(RetryMode.OnFatal)]
        [InlineData(RetryMode.OnFatalKeepDuplicates)]
        public void Constructor_WithDifferentRetryModes_StoresCorrectly(RetryMode retryMode)
        {
            var param = new BetaResourceParams(100, 10, retryMode, SanitationMode.None);
            Assert.Equal(retryMode, param.RetryMode);
        }

        [Theory]
        [InlineData(SanitationMode.None)]
        [InlineData(SanitationMode.Clean)]
        public void Constructor_WithDifferentSanitationModes_StoresCorrectly(SanitationMode sanitationMode)
        {
            var param = new BetaResourceParams(100, 10, RetryMode.None, sanitationMode);
            Assert.Equal(sanitationMode, param.SanitationMode);
        }

        [Fact]
        public void Constructor_WithLargeValues_CreatesInstance()
        {
            var param = new BetaResourceParams(10000, 1000, RetryMode.OnFatal, SanitationMode.Clean);
            Assert.Equal(10000, param.ChunkSize);
            Assert.Equal(1000, param.ThrottleSize);
        }

        [Fact]
        public void Constructor_WithMinimumValidValues_CreatesInstance()
        {
            var param = new BetaResourceParams(1, 1, RetryMode.None, SanitationMode.None);
            Assert.Equal(1, param.ChunkSize);
            Assert.Equal(1, param.ThrottleSize);
        }

        [Fact]
        public void Properties_AreAccessible()
        {
            var param = new BetaResourceParams(100, 10, RetryMode.None, SanitationMode.None);

            Assert.Equal(100, param.ChunkSize);
            Assert.Equal(10, param.ThrottleSize);
            Assert.Equal(RetryMode.None, param.RetryMode);
            Assert.Equal(SanitationMode.None, param.SanitationMode);
        }
    }
}
