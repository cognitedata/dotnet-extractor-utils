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
    /// Unit tests for <see cref="BetaStateSetsExtensions"/> - tests parameter validation
    /// and error handling. Integration tests cover actual functionality with real SDK clients.
    /// </summary>
    public class BetaStateSetsExtensionsTest
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
        public async Task GetOrCreateStateSetsAsync_WithNullStateSets_ThrowsArgumentNullException()
        {
            var instanceIds = new[] { CreateInstanceId() };
            Func<IEnumerable<InstanceIdentifier>, IEnumerable<SourcedNodeWrite<CogniteStateSet>>> buildStateSets =
                ids => new List<SourcedNodeWrite<CogniteStateSet>>();
            var options = new BetaResourceParams(100, 10, RetryMode.None, SanitationMode.None);

            var ex = await Assert.ThrowsAsync<ArgumentNullException>(
                () => ((CogniteSdk.Resources.Beta.StateSetsResource)null).GetOrCreateStateSetsAsync(instanceIds, buildStateSets, options, CancellationToken.None));

            Assert.Equal("stateSets", ex.ParamName);
        }

        [Fact]
        public async Task GetOrCreateStateSetsAsync_WithNullBuildStateSets_ThrowsArgumentNullException()
        {
            var instanceIds = new[] { CreateInstanceId() };
            var options = new BetaResourceParams(100, 10, RetryMode.None, SanitationMode.None);

            await Assert.ThrowsAsync<ArgumentNullException>(
                () => ((CogniteSdk.Resources.Beta.StateSetsResource)null).GetOrCreateStateSetsAsync(
                    instanceIds,
                    (Func<IEnumerable<InstanceIdentifier>, IEnumerable<SourcedNodeWrite<CogniteStateSet>>>)null,
                    options,
                    CancellationToken.None));
        }

        [Fact]
        public async Task GetOrCreateStateSetsAsync_WithNullOptions_ThrowsArgumentNullException()
        {
            var instanceIds = new[] { CreateInstanceId() };
            Func<IEnumerable<InstanceIdentifier>, IEnumerable<SourcedNodeWrite<CogniteStateSet>>> buildStateSets =
                ids => new List<SourcedNodeWrite<CogniteStateSet>>();

            await Assert.ThrowsAsync<ArgumentNullException>(
                () => ((CogniteSdk.Resources.Beta.StateSetsResource)null).GetOrCreateStateSetsAsync(
                    instanceIds, buildStateSets, null, CancellationToken.None));
        }

        [Fact]
        public async Task GetOrCreateStateSetsAsync_AsyncOverload_WithNullStateSets_ThrowsArgumentNullException()
        {
            var instanceIds = new[] { CreateInstanceId() };
            Func<IEnumerable<InstanceIdentifier>, Task<IEnumerable<SourcedNodeWrite<CogniteStateSet>>>> buildStateSets =
                ids => Task.FromResult<IEnumerable<SourcedNodeWrite<CogniteStateSet>>>(new List<SourcedNodeWrite<CogniteStateSet>>());
            var options = new BetaResourceParams(100, 10, RetryMode.None, SanitationMode.None);

            await Assert.ThrowsAsync<ArgumentNullException>(
                () => ((CogniteSdk.Resources.Beta.StateSetsResource)null).GetOrCreateStateSetsAsync(instanceIds, buildStateSets, options, CancellationToken.None));
        }

        [Fact]
        public async Task GetOrCreateStateSetsAsync_AsyncOverload_WithNullBuildStateSets_ThrowsArgumentNullException()
        {
            var instanceIds = new[] { CreateInstanceId() };
            var options = new BetaResourceParams(100, 10, RetryMode.None, SanitationMode.None);

            await Assert.ThrowsAsync<ArgumentNullException>(
                () => ((CogniteSdk.Resources.Beta.StateSetsResource)null).GetOrCreateStateSetsAsync(
                    instanceIds,
                    (Func<IEnumerable<InstanceIdentifier>, Task<IEnumerable<SourcedNodeWrite<CogniteStateSet>>>>)null,
                    options,
                    CancellationToken.None));
        }

        [Fact]
        public async Task GetOrCreateStateSetsAsync_AsyncOverload_WithNullOptions_ThrowsArgumentNullException()
        {
            var instanceIds = new[] { CreateInstanceId() };
            Func<IEnumerable<InstanceIdentifier>, Task<IEnumerable<SourcedNodeWrite<CogniteStateSet>>>> buildStateSets =
                ids => Task.FromResult<IEnumerable<SourcedNodeWrite<CogniteStateSet>>>(new List<SourcedNodeWrite<CogniteStateSet>>());

            await Assert.ThrowsAsync<ArgumentNullException>(
                () => ((CogniteSdk.Resources.Beta.StateSetsResource)null).GetOrCreateStateSetsAsync(instanceIds, buildStateSets, null, CancellationToken.None));
        }

        [Fact]
        public async Task EnsureStateSetsExistAsync_WithNullStateSets_ThrowsArgumentNullException()
        {
            var stateSet = CreateSourcedNodeWrite("space1", "id1", new CogniteStateSet());
            var options = new BetaResourceParams(100, 10, RetryMode.None, SanitationMode.None);

            await Assert.ThrowsAsync<ArgumentNullException>(
                () => ((CogniteSdk.Resources.Beta.StateSetsResource)null).EnsureStateSetsExistAsync(new[] { stateSet }, options, CancellationToken.None));
        }

        [Fact]
        public async Task EnsureStateSetsExistAsync_WithNullOptions_ThrowsArgumentNullException()
        {
            var stateSet = CreateSourcedNodeWrite("space1", "id1", new CogniteStateSet());

            await Assert.ThrowsAsync<ArgumentNullException>(
                () => ((CogniteSdk.Resources.Beta.StateSetsResource)null).EnsureStateSetsExistAsync(new[] { stateSet }, null, CancellationToken.None));
        }

        [Fact]
        public async Task GetStateSetsByIdsIgnoreErrors_WithNullStateSets_ThrowsArgumentNullException()
        {
            var ids = new[] { CogniteSdk.Identity.Create(CreateInstanceId()) };

            await Assert.ThrowsAsync<ArgumentNullException>(
                () => ((CogniteSdk.Resources.Beta.StateSetsResource)null).GetStateSetsByIdsIgnoreErrors<CogniteStateSet>(ids, 100, 10, CancellationToken.None));
        }

        [Fact]
        public async Task UpsertAsync_WithNullStateSetsResource_ThrowsArgumentNullException()
        {
            var stateSet = CreateSourcedNodeWrite("space1", "id1", new CogniteStateSet());
            var options = new BetaResourceParams(100, 10, RetryMode.None, SanitationMode.None);

            await Assert.ThrowsAsync<ArgumentNullException>(
                () => ((CogniteSdk.Resources.Beta.StateSetsResource)null).UpsertAsync(new[] { stateSet }, options, CancellationToken.None));
        }

        [Fact]
        public async Task UpsertAsync_WithNullOptions_ThrowsArgumentNullException()
        {
            var stateSet = CreateSourcedNodeWrite("space1", "id1", new CogniteStateSet());

            await Assert.ThrowsAsync<ArgumentNullException>(
                () => ((CogniteSdk.Resources.Beta.StateSetsResource)null).UpsertAsync(new[] { stateSet }, null, CancellationToken.None));
        }
    }
}
