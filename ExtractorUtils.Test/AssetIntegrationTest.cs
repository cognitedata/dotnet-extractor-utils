using Cognite.Extensions;
using CogniteSdk;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace ExtractorUtils.Test
{
    public class AssetIntegrationTest
    {
        private string[] lines = { 
            "version: 2",
            "logger:",
            "  console:",
            "    level: verbose",
            "cognite:",
            "  project: ${TEST_PROJECT}",
            "  api-key: ${TEST_API_KEY}",
            "  host: ${TEST_HOST}",
            "  cdf-chunking:",
            "    assets: 2",
            "  cdf-throttling:",
            "    assets: 2" };
        // Basic usage of ensure and GetOrCreate
        [Fact]
        public async Task TestCreateAssets()
        {
            using var tester = new CDFTester(lines);
            var ids = new[] {
                $"{tester.Prefix} asset-1",
                $"{tester.Prefix} asset-2",
                $"{tester.Prefix} asset-3",
                $"{tester.Prefix} asset-4",
                $"{tester.Prefix} asset-5",
            };
            try
            {
                var assets1 = new[]
                {
                    new AssetCreate
                    {
                        Name = $"{tester.Prefix} asset-1",
                        ExternalId = $"{tester.Prefix} asset-1"
                    },
                    new AssetCreate
                    {
                        Name = $"{tester.Prefix} asset-2",
                        ExternalId = $"{tester.Prefix} asset-2"
                    },
                    new AssetCreate
                    {
                        Name = $"{tester.Prefix} asset-3",
                        ExternalId = $"{tester.Prefix} asset-3"
                    }
                };

                var result = await tester.Destination.EnsureAssetsExistsAsync(assets1, RetryMode.None, SanitationMode.None, tester.Source.Token);
                Assert.True(result.IsAllGood);
                Assert.Equal(3, result.Results.Count());

                var assets2 = new[]
                {
                    assets1[0],
                    new AssetCreate
                    {
                        Name = $"{tester.Prefix} asset-4",
                        ExternalId = $"{tester.Prefix} asset-4"
                    }
                };

                result = await tester.Destination.EnsureAssetsExistsAsync(assets2, RetryMode.OnError, SanitationMode.None, tester.Source.Token);

                Assert.Single(result.Errors);
                var error = result.Errors.First();
                Assert.Equal(ErrorType.ItemExists, error.Type);
                Assert.Equal(ResourceType.ExternalId, error.Resource);

                result = await tester.Destination.GetOrCreateAssetsAsync(ids, toCreate =>
                {
                    Assert.Single(toCreate);
                    Assert.Equal($"{tester.Prefix} asset-5", toCreate.First());
                    return new[]
                    {
                        new AssetCreate
                        {
                            Name = $"{tester.Prefix} asset-5",
                            ExternalId = $"{tester.Prefix} asset-5"
                        }
                    };
                }, RetryMode.OnError, SanitationMode.None, tester.Source.Token);

                Assert.Equal(5, result.Results.Count());
                Assert.Equal(Enumerable.Range('1', 5), result.Results.Select(res => (int)res.ExternalId.Last()));
            }
            finally
            {
                await tester.Destination.CogniteClient.Assets.DeleteAsync(new AssetDelete
                {
                    IgnoreUnknownIds = true,
                    Items = ids.Select(Identity.Create)
                }, tester.Source.Token);
            }
        }
    }
}
