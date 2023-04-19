using Cognite.Extractor.Utils;
using CogniteSdk;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace ExtractorUtils.Test.integration
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
            Assert.Null(await config.GetDataSet(tester.Destination.CogniteClient, tester.Source.Token));
            Assert.Null(await config.GetDataSetId(tester.Destination.CogniteClient, tester.Source.Token));

            // Configured with correct ID
            config.Id = dataSetId;
            Assert.Equal(dataSetId, (await config.GetDataSet(tester.Destination.CogniteClient, tester.Source.Token)).Id);
            Assert.Equal(dataSetId, await config.GetDataSetId(tester.Destination.CogniteClient, tester.Source.Token));

            // Configured with correct externalId
            config.Id = null;
            config.ExternalId = dataSetExternalId;
            Assert.Equal(dataSetId, (await config.GetDataSet(tester.Destination.CogniteClient, tester.Source.Token)).Id);
            Assert.Equal(dataSetId, await config.GetDataSetId(tester.Destination.CogniteClient, tester.Source.Token));

            // Configured with incorrect ID
            config.Id = 123;
            config.ExternalId = null;
            await Assert.ThrowsAsync<ResponseException>(async () => await config.GetDataSet(tester.Destination.CogniteClient, tester.Source.Token));
            // This doesn't actually test the data set, needed for backwards compatibility in some extractors.
            Assert.Equal(123, await config.GetDataSetId(tester.Destination.CogniteClient, tester.Source.Token));

            // Configured with incorrect external ID
            config.Id = null;
            config.ExternalId = "some-dataset-that-doesnt-exist";
            await Assert.ThrowsAsync<ResponseException>(async () => await config.GetDataSet(tester.Destination.CogniteClient, tester.Source.Token));
            await Assert.ThrowsAsync<ResponseException>(async () => await config.GetDataSetId(tester.Destination.CogniteClient, tester.Source.Token));

            // No client
            await Assert.ThrowsAsync<ArgumentNullException>(async () => await config.GetDataSet(null, tester.Source.Token));
            await Assert.ThrowsAsync<ArgumentNullException>(async () => await config.GetDataSetId(null, tester.Source.Token));
        }
    }
}
