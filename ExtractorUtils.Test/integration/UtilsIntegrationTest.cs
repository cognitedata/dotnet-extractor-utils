using System.Threading.Tasks;
using Cognite.Extensions;
using CogniteSdk;
using Xunit;

namespace ExtractorUtils.Test.Integration
{
    public class UtilsIntegrationTest
    {
        private readonly ITestOutputHelper _output;
        public UtilsIntegrationTest(ITestOutputHelper output)
        {
            _output = output;
        }

        [Fact]
        public async Task TestGetDataSet()
        {
            using var tester = new CDFTester(CDFTester.GetConfig(CogniteHost.BlueField), _output);

            var dataSetId = await tester.GetDataSetId();
            var dataSetExternalId = "test-dataset";
            var config = new DataSetConfig();

            // No configured data set
            Assert.Null(await tester.Destination.CogniteClient.DataSets.Get(config, tester.Source.Token));
            Assert.Null(await tester.Destination.CogniteClient.DataSets.GetId(config, tester.Source.Token));

            // Configured with correct ID
            config.Id = dataSetId;
            Assert.Equal(dataSetId, (await tester.Destination.CogniteClient.DataSets.Get(config, tester.Source.Token)).Id);
            Assert.Equal(dataSetId, await tester.Destination.CogniteClient.DataSets.GetId(config, tester.Source.Token));

            // Configured with correct externalId
            config.Id = null;
            config.ExternalId = dataSetExternalId;
            Assert.Equal(dataSetId, (await tester.Destination.CogniteClient.DataSets.Get(config, tester.Source.Token)).Id);
            Assert.Equal(dataSetId, await tester.Destination.CogniteClient.DataSets.GetId(config, tester.Source.Token));

            // Configured with incorrect ID
            config.Id = 123;
            config.ExternalId = null;
            await Assert.ThrowsAsync<ResponseException>(async () => await tester.Destination.CogniteClient.DataSets.Get(config, tester.Source.Token));
            // This doesn't actually test the data set, needed for backwards compatibility in some extractors.
            Assert.Equal(123, await tester.Destination.CogniteClient.DataSets.GetId(config, tester.Source.Token));

            // Configured with incorrect external ID
            config.Id = null;
            config.ExternalId = "some-dataset-that-doesnt-exist";
            await Assert.ThrowsAsync<ResponseException>(async () => await tester.Destination.CogniteClient.DataSets.Get(config, tester.Source.Token));
            await Assert.ThrowsAsync<ResponseException>(async () => await tester.Destination.CogniteClient.DataSets.GetId(config, tester.Source.Token));
        }
    }
}
