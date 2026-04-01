#nullable enable
using System;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extractor.Utils;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using Xunit;

namespace ExtractorUtils.Test.Unit
{
    /// <summary>
    /// Test double for CogniteDestination that doesn't require CogniteSdk.Client
    /// </summary>
    internal class TestCogniteDestination : CogniteDestination
    {
        public TestCogniteDestination()
            : base(null!, new NullLogger<CogniteDestination>(), new CogniteConfig { Project = "test-project" })
        {
        }
    }

    public class CallbackUtilTest
    {
        private static CogniteDestination GetCogniteDestination()
        {
            return new TestCogniteDestination();
        }

        [Fact]
        public async Task TryCallWithMissingConfiguration_ReturnsFalseAndLogsWarning()
        {
            // Arrange
            var destination = GetCogniteDestination();
            var mockLogger = new Mock<ILogger>();
            var config = new FunctionCallConfig(); // No ExternalId or Id
            var wrapper = new FunctionCallWrapper<string>(destination, config, mockLogger.Object);

            // Act
            var result = await wrapper.TryCall("test-argument", CancellationToken.None);

            // Assert
            Assert.False(result);
            mockLogger.Verify(
                x => x.Log(
                    LogLevel.Warning,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains("Missing function configuration")),
                    It.IsAny<Exception>(),
                    It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
                Times.Once);
        }

        [Theory]
        [InlineData("test-function-id", null)]
        [InlineData(null, 12345L)]
        [InlineData("test-function-id", 12345L)]
        public async Task TryCallWithValidConfiguration_ReturnsTrue(string? externalId, long? id)
        {
            // Arrange
            var destination = GetCogniteDestination();
            var config = new FunctionCallConfig { ExternalId = externalId, Id = id };
            var wrapper = new FunctionCallWrapper<string>(destination, config, null);

            // Act
            // Note: The stub destination always returns true for function calls.
            // If the underlying function's behavior changes, this test will need to be updated.
            var result = await wrapper.TryCall("test-argument", CancellationToken.None);

            // Assert
            Assert.True(result);
        }
    }
}
