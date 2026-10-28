using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extensions;
using Cognite.Extensions.DataModels.CogniteExtractorExtensions;
using CogniteSdk.DataModels;
using CogniteSdk.DataModels.Core;
using Xunit;

namespace ExtractorUtils.Test.Unit
{
    /// <summary>
    /// Unit tests for <see cref="BetaTimeSeriesExtensions"/> - tests parameter validation
    /// and error handling. Integration tests cover actual functionality with real SDK clients.
    /// </summary>
    public class BetaTimeSeriesExtensionsTest
    {
        private InstanceIdentifier CreateInstanceId(string space = "test-space", string externalId = "test-id")
        {
            return new InstanceIdentifier { Space = space, ExternalId = externalId };
        }

        private SourcedNodeWrite<T> CreateSourcedNodeWrite<T>(string space = "test-space", string externalId = "test-id", T properties = null) where T : class
        {
            return new SourcedNodeWrite<T>
            {
                Space = space,
                ExternalId = externalId,
                Properties = properties
            };
        }

        [Fact]
        public async Task GetOrCreateTimeSeriesAsync_WithNullTimeSeries_ThrowsArgumentNullException()
        {
            var instanceIds = new[] { CreateInstanceId() };
            Func<IEnumerable<InstanceIdentifier>, IEnumerable<SourcedNodeWrite<CogniteTimeSeriesBase>>> buildTimeSeries =
                ids => new List<SourcedNodeWrite<CogniteTimeSeriesBase>>();
            var options = new BetaResourceParams(100, 10, RetryMode.None, SanitationMode.None);

            var ex = await Assert.ThrowsAsync<ArgumentNullException>(
                () => ((CogniteSdk.Resources.Beta.TimeSeriesResource)null).GetOrCreateTimeSeriesAsync(instanceIds, buildTimeSeries, options, CancellationToken.None));

            Assert.Equal("timeSeries", ex.ParamName);
        }

        [Fact]
        public async Task GetOrCreateTimeSeriesAsync_WithNullBuildTimeSeries_ThrowsArgumentNullException()
        {
            var instanceIds = new[] { CreateInstanceId() };
            var options = new BetaResourceParams(100, 10, RetryMode.None, SanitationMode.None);

            await Assert.ThrowsAsync<ArgumentNullException>(
                () => ((CogniteSdk.Resources.Beta.TimeSeriesResource)null).GetOrCreateTimeSeriesAsync(
                    instanceIds,
                    (Func<IEnumerable<InstanceIdentifier>, IEnumerable<SourcedNodeWrite<CogniteTimeSeriesBase>>>)null,
                    options,
                    CancellationToken.None));
        }

        [Fact]
        public async Task GetOrCreateTimeSeriesAsync_WithNullOptions_ThrowsArgumentNullException()
        {
            var instanceIds = new[] { CreateInstanceId() };
            Func<IEnumerable<InstanceIdentifier>, IEnumerable<SourcedNodeWrite<CogniteTimeSeriesBase>>> buildTimeSeries =
                ids => new List<SourcedNodeWrite<CogniteTimeSeriesBase>>();

            await Assert.ThrowsAsync<ArgumentNullException>(
                () => ((CogniteSdk.Resources.Beta.TimeSeriesResource)null).GetOrCreateTimeSeriesAsync(
                    instanceIds, buildTimeSeries, null, CancellationToken.None));
        }

        [Fact]
        public async Task GetOrCreateTimeSeriesAsync_AsyncOverload_WithNullTimeSeries_ThrowsArgumentNullException()
        {
            var instanceIds = new[] { CreateInstanceId() };
            Func<IEnumerable<InstanceIdentifier>, Task<IEnumerable<SourcedNodeWrite<CogniteTimeSeriesBase>>>> buildTimeSeries =
                ids => Task.FromResult<IEnumerable<SourcedNodeWrite<CogniteTimeSeriesBase>>>(new List<SourcedNodeWrite<CogniteTimeSeriesBase>>());
            var options = new BetaResourceParams(100, 10, RetryMode.None, SanitationMode.None);

            await Assert.ThrowsAsync<ArgumentNullException>(
                () => ((CogniteSdk.Resources.Beta.TimeSeriesResource)null).GetOrCreateTimeSeriesAsync(instanceIds, buildTimeSeries, options, CancellationToken.None));
        }

        [Fact]
        public async Task GetOrCreateTimeSeriesAsync_AsyncOverload_WithNullBuildTimeSeries_ThrowsArgumentNullException()
        {
            var instanceIds = new[] { CreateInstanceId() };
            var options = new BetaResourceParams(100, 10, RetryMode.None, SanitationMode.None);

            await Assert.ThrowsAsync<ArgumentNullException>(
                () => ((CogniteSdk.Resources.Beta.TimeSeriesResource)null).GetOrCreateTimeSeriesAsync(
                    instanceIds,
                    (Func<IEnumerable<InstanceIdentifier>, Task<IEnumerable<SourcedNodeWrite<CogniteTimeSeriesBase>>>>)null,
                    options,
                    CancellationToken.None));
        }

        [Fact]
        public async Task GetOrCreateTimeSeriesAsync_AsyncOverload_WithNullOptions_ThrowsArgumentNullException()
        {
            var instanceIds = new[] { CreateInstanceId() };
            Func<IEnumerable<InstanceIdentifier>, Task<IEnumerable<SourcedNodeWrite<CogniteTimeSeriesBase>>>> buildTimeSeries =
                ids => Task.FromResult<IEnumerable<SourcedNodeWrite<CogniteTimeSeriesBase>>>(new List<SourcedNodeWrite<CogniteTimeSeriesBase>>());

            await Assert.ThrowsAsync<ArgumentNullException>(
                () => ((CogniteSdk.Resources.Beta.TimeSeriesResource)null).GetOrCreateTimeSeriesAsync(instanceIds, buildTimeSeries, null, CancellationToken.None));
        }

        [Fact]
        public async Task EnsureTimeSeriesExistsAsync_WithNullTimeSeries_ThrowsArgumentNullException()
        {
            var timeSeries = CreateSourcedNodeWrite("space1", "ts1", new CogniteTimeSeriesBase());
            var options = new BetaResourceParams(100, 10, RetryMode.None, SanitationMode.None);

            await Assert.ThrowsAsync<ArgumentNullException>(
                () => ((CogniteSdk.Resources.Beta.TimeSeriesResource)null).EnsureTimeSeriesExistsAsync(new[] { timeSeries }, options, CancellationToken.None));
        }

        [Fact]
        public async Task EnsureTimeSeriesExistsAsync_WithNullOptions_ThrowsArgumentNullException()
        {
            var ts = CreateSourcedNodeWrite("space1", "ts1", new CogniteTimeSeriesBase());

            await Assert.ThrowsAsync<ArgumentNullException>(
                () => ((CogniteSdk.Resources.Beta.TimeSeriesResource)null).EnsureTimeSeriesExistsAsync(new[] { ts }, null, CancellationToken.None));
        }

        [Fact]
        public async Task UpsertAsync_WithNullTimeSeriesResource_ThrowsArgumentNullException()
        {
            var ts = CreateSourcedNodeWrite("space1", "ts1", new CogniteTimeSeriesBase());
            var options = new BetaResourceParams(100, 10, RetryMode.None, SanitationMode.None);

            await Assert.ThrowsAsync<ArgumentNullException>(
                () => ((CogniteSdk.Resources.Beta.TimeSeriesResource)null).UpsertAsync(new[] { ts }, options, CancellationToken.None));
        }

        [Fact]
        public async Task UpsertAsync_WithNullOptions_ThrowsArgumentNullException()
        {
            var ts = CreateSourcedNodeWrite("space1", "ts1", new CogniteTimeSeriesBase());

            await Assert.ThrowsAsync<ArgumentNullException>(
                () => ((CogniteSdk.Resources.Beta.TimeSeriesResource)null).UpsertAsync(new[] { ts }, null, CancellationToken.None));
        }
    }
}
