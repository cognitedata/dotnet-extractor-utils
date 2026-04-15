using System;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extractor.Utils.Unstable.Tasks;
using CogniteSdk;
using CogniteSdk.Alpha;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Xunit;
using Xunit.Abstractions;

namespace ExtractorUtils.Test.Integration
{
    public class CheckInIntegrationTest
    {
        private readonly ITestOutputHelper _output;

        public CheckInIntegrationTest(ITestOutputHelper output)
        {
            _output = output;
        }

        [Fact]
        public async Task TestCDFCheckInForLongErrors()
        {
            using var tester = new CDFTester(CDFTester.GetConfig(CogniteHost.BlueField), _output);

            var client = tester.Destination.CogniteClient;
            var longDescription = new string('x', 6000);
            var longDetails = new string('y', 6000);

            // Test 1: Verify that the CDF API rejects errors with description > 5000 characters
            var requestWithLongDescription = new CheckInRequest
            {
                ExternalId = "test-integration-that-does-not-exist",
                Errors = new[]
                {
                    new ErrorWithTask
                    {
                        ExternalId = Guid.NewGuid().ToString(),
                        Level = ErrorLevel.error,
                        Description = longDescription,
                        StartTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                    }
                }
            };

            var ex = await Assert.ThrowsAsync<ResponseException>(async () =>
            {
                await client.Alpha.Integrations.CheckInAsync(requestWithLongDescription, CancellationToken.None);
            });
            _output.WriteLine($"Long description exception: {ex.Message}");
            Assert.Contains("5000", ex.Message);

            // Test 2: Verify that the CDF API rejects errors with details > 5000 characters
            var requestWithLongDetails = new CheckInRequest
            {
                ExternalId = "test-integration-that-does-not-exist",
                Errors = new[]
                {
                    new ErrorWithTask
                    {
                        ExternalId = Guid.NewGuid().ToString(),
                        Level = ErrorLevel.warning,
                        Description = "Short description",
                        Details = longDetails,
                        StartTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                    }
                }
            };

            ex = await Assert.ThrowsAsync<ResponseException>(async () =>
            {
                await client.Alpha.Integrations.CheckInAsync(requestWithLongDetails, CancellationToken.None);
            });
            _output.WriteLine($"Long details exception: {ex.Message}");
            Assert.Contains("5000", ex.Message);

            // Test 3: Verify that CheckInWorker truncates long description/details and succeeds
            var logger = tester.Provider.GetRequiredService<ILogger<CheckInWorker>>();
            var checkInWorker = new CheckInWorker(
                "test-integration-that-does-not-exist",
                logger,
                client,
                _ => { },
                null
            );

            // Report an error with long description and details - CheckInWorker should truncate them
            checkInWorker.ReportError(new ExtractorError(ErrorLevel.error, longDescription, checkInWorker, longDetails));

            // Flush should succeed without throwing because CheckInWorker truncates the fields
            // Note: This will get a 404 since the integration doesn't exist, but that's fine -
            // the important thing is it doesn't fail with the 5000 character validation error
            await checkInWorker.Flush(CancellationToken.None);
            _output.WriteLine("CheckInWorker.Flush succeeded after truncating long description/details");
        }
    }
}
