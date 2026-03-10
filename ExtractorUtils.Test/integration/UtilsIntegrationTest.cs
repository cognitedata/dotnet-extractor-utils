using Cognite.Extensions;
using CogniteSdk;
using System;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

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

        [Fact]
        public async Task TestGetDataSets()
        {
            using var tester = new CDFTester(CDFTester.GetConfig(CogniteHost.BlueField), _output);

            var dataSetId = await tester.GetDataSetId();
            var dataSetExternalId = "test-dataset";

            // No ids provided
            await Assert.ThrowsAsync<ArgumentNullException>(async () => await tester.Destination.CogniteClient.DataSets.GetIds(null, tester.Source.Token));

            // Retrieve external ID
            var id = Identity.Create(dataSetExternalId);
            var datasets = await tester.Destination.CogniteClient.DataSets.GetIds(new[] { id }, tester.Source.Token);
            Assert.Single(datasets);
            Assert.Equal(dataSetId, datasets.FirstOrDefault()?.Id);

            // Retrieve incorrect external ID
            var id2 = Identity.Create("some-dataset-that-doesnt-exist");
            var ids = new[] { id, id2 };
            await Assert.ThrowsAsync<ResponseException>(async () => await tester.Destination.CogniteClient.DataSets.GetIds(ids, tester.Source.Token));


            // Retrieve multiple datasets
            var dataSetId2 = await tester.GetDataSetId("test-dataset-2");
            var dataSetExternalId2 = "test-dataset-2";
            var existing_ids = new[] { id, Identity.Create(dataSetExternalId2) };
            var datasets2 = await tester.Destination.CogniteClient.DataSets.GetIds(existing_ids, tester.Source.Token);
            Assert.Equal(2, datasets2.Count());
            Assert.Contains(datasets2, ds => ds.Id == dataSetId);
            Assert.Contains(datasets2, ds => ds.Id == dataSetId2);
        }
    }
}
