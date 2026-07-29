using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using CogniteSdk;
using CogniteSdk.DataModels;
using CogniteSdk.DataModels.Core;
using Microsoft.Extensions.DependencyInjection;
using Xunit;
using Cognite.Extractor.Utils;
using Cognite.Extensions;
using Xunit.Abstractions;
using Cognite.Extractor.Testing;
using Cognite.Extractor.Testing.Mock;
using Moq;

namespace ExtractorUtils.Test.Unit
{
    /// <summary>
    /// Unit tests for the <see cref="CogniteDestinationWithIDM"/> state-set wrapper methods
    /// (GetOrCreateStateSetsAsync/EnsureStateSetsExistAsync/GetStateSetsByIdsIgnoreErrors/UpsertStateSetsAsync),
    /// covering the two different null-argument conventions these methods use, the async-builder overload,
    /// and that config-derived chunking is actually honored -- none of which the integration tests
    /// (which only call the sync-builder overload with a single item, config defaults untouched) exercise.
    /// </summary>
    public class CogniteDestinationWithIDMStateSetTest
    {
        private const string _project = "someProject";
        private const string _host = "https://test.cognitedata.com";
        private const string _retrievePath = "^/api/v1/projects/" + _project + "/models/instances/byids$";
        private const string _upsertPath = "^/api/v1/projects/" + _project + "/models/instances$";

        private readonly ITestOutputHelper _output;

        public CogniteDestinationWithIDMStateSetTest(ITestOutputHelper output)
        {
            _output = output;
        }

        private (ServiceProvider provider, CdfMock mock) BuildProvider(string configPath, int instancesChunkSize = 1000)
        {
            // max-retries: 0 disables the SDK's own transport-level retry, so failures surface immediately
            // rather than being conflated with the SDK's own retry behavior.
            string[] lines = {
                "version: 2",
                "logger:",
                "  console:",
                "    level: verbose",
                "cognite:",
               $"  project: {_project}",
               $"  host: {_host}",
                "  cdf-retries:",
                "    max-retries: 0",
                "  cdf-chunking:",
               $"    instances: {instancesChunkSize}"
            };
            System.IO.File.WriteAllLines(configPath, lines);

            var services = new ServiceCollection();
            services.AddConfig<BaseConfig>(configPath, 2);
            services.AddTestLogging(_output);
            CdfMock.RegisterHttpClient(services);
            services.AddCogniteClient("testApp");
            var provider = services.BuildServiceProvider();
            var mock = provider.GetRequiredService<CdfMock>();
            return (provider, mock);
        }

        private static HttpResponseMessage MockRetrieveEmpty(RequestContext ctx, CancellationToken token)
        {
            return ctx.CreateJsonResponse(new InstancesRetrieveResponse<Dictionary<string, Dictionary<string, CogniteStateSet>>>
            {
                Items = Array.Empty<BaseInstance<Dictionary<string, Dictionary<string, CogniteStateSet>>>>()
            });
        }

        private static async Task<HttpResponseMessage> MockUpsertEcho(RequestContext ctx, CancellationToken token)
        {
            var body = await ctx.ReadJsonBody<InstanceWriteRequest>().ConfigureAwait(false);
            var slim = body.Items.Select(i => new SlimInstance
            {
                InstanceType = InstanceType.node,
                Space = i.Space,
                ExternalId = i.ExternalId,
                Version = 1
            });
            return ctx.CreateJsonResponse(new ItemsWithoutCursor<SlimInstance> { Items = slim });
        }

        private static SourcedNodeWrite<CogniteStateSet> BuildStateSet(InstanceIdentifier id)
        {
            return new SourcedNodeWrite<CogniteStateSet>
            {
                Space = id.Space,
                ExternalId = id.ExternalId,
                Properties = new CogniteStateSet
                {
                    Name = id.ExternalId,
                    States = new[] { new CogniteState { NumericValue = 0, StringValue = "OPEN" } }
                }
            };
        }

        public enum NullArgOp
        {
            GetOrCreateSync_InstanceIdsNull,
            GetOrCreateSync_BuildStateSetsNull,
            GetOrCreateAsync_InstanceIdsNull,
            GetOrCreateAsync_BuildStateSetsNull,
            EnsureExists_StateSetsNull,
            GetByIds_StateSetsNull,
            Upsert_UpdatesNull
        }

        [Theory]
        // instanceIds/stateSets/updates == null silently returns an empty/no-error result, while
        // buildStateSets == null and EnsureStateSetsExistAsync's stateSets == null throw -- two
        // different conventions on the same set of wrapper methods, neither exercised anywhere else.
        [InlineData(NullArgOp.GetOrCreateSync_InstanceIdsNull)]
        [InlineData(NullArgOp.GetOrCreateSync_BuildStateSetsNull)]
        [InlineData(NullArgOp.GetOrCreateAsync_InstanceIdsNull)]
        [InlineData(NullArgOp.GetOrCreateAsync_BuildStateSetsNull)]
        [InlineData(NullArgOp.EnsureExists_StateSetsNull)]
        [InlineData(NullArgOp.GetByIds_StateSetsNull)]
        [InlineData(NullArgOp.Upsert_UpdatesNull)]
        public async Task NullArguments_EitherThrowOrReturnEmpty_DependingOnMethod(NullArgOp op)
        {
            string path = $"test-destidm-nullarg-{op}-config.yml";
            var (provider, mock) = BuildProvider(path);
            using (provider)
            {
                mock.AddMatcher(new SimpleMatcher("POST", _retrievePath, (ctx, t) => Task.FromResult(MockRetrieveEmpty(ctx, t)), Times.Never()));
                mock.AddMatcher(new SimpleMatcher("POST", _upsertPath, MockUpsertEcho, Times.Never()));

                var destination = provider.GetRequiredService<CogniteDestinationWithIDM>();
                var id = new InstanceIdentifier("mySpace", "ss1");

                switch (op)
                {
                    case NullArgOp.GetOrCreateSync_InstanceIdsNull:
                        var r1 = await destination.GetOrCreateStateSetsAsync<CogniteStateSet>(
                            null, missing => missing.Select(BuildStateSet), RetryMode.None, SanitationMode.None, CancellationToken.None);
                        Assert.True(r1.Results == null || !r1.Results.Any());
                        Assert.True(r1.Errors == null || !r1.Errors.Any());
                        break;
                    case NullArgOp.GetOrCreateSync_BuildStateSetsNull:
                        await Assert.ThrowsAsync<ArgumentNullException>(() =>
                            destination.GetOrCreateStateSetsAsync<CogniteStateSet>(
                                new[] { id },
                                (Func<IEnumerable<InstanceIdentifier>, IEnumerable<SourcedNodeWrite<CogniteStateSet>>>)null,
                                RetryMode.None, SanitationMode.None, CancellationToken.None));
                        break;
                    case NullArgOp.GetOrCreateAsync_InstanceIdsNull:
                        var r2 = await destination.GetOrCreateStateSetsAsync<CogniteStateSet>(
                            null, missing => Task.FromResult(missing.Select(BuildStateSet)), RetryMode.None, SanitationMode.None, CancellationToken.None);
                        Assert.True(r2.Results == null || !r2.Results.Any());
                        break;
                    case NullArgOp.GetOrCreateAsync_BuildStateSetsNull:
                        await Assert.ThrowsAsync<ArgumentNullException>(() =>
                            destination.GetOrCreateStateSetsAsync<CogniteStateSet>(
                                new[] { id },
                                (Func<IEnumerable<InstanceIdentifier>, Task<IEnumerable<SourcedNodeWrite<CogniteStateSet>>>>)null,
                                RetryMode.None, SanitationMode.None, CancellationToken.None));
                        break;
                    case NullArgOp.EnsureExists_StateSetsNull:
                        await Assert.ThrowsAsync<ArgumentNullException>(() =>
                            destination.EnsureStateSetsExistAsync<CogniteStateSet>(null, RetryMode.None, SanitationMode.None, CancellationToken.None));
                        break;
                    case NullArgOp.GetByIds_StateSetsNull:
                        var r3 = await destination.GetStateSetsByIdsIgnoreErrors<CogniteStateSet>(null, CancellationToken.None);
                        Assert.Empty(r3);
                        break;
                    case NullArgOp.Upsert_UpdatesNull:
                        var r4 = await destination.UpsertStateSetsAsync<CogniteStateSet>(null, RetryMode.None, SanitationMode.None, CancellationToken.None);
                        Assert.True(r4.Results == null || !r4.Results.Any());
                        break;
                }
            }
            System.IO.File.Delete(path);
        }

        [Fact]
        public async Task GetOrCreateStateSetsAsync_AsyncBuilder_UsesConfiguredChunkSize()
        {
            string path = "test-destidm-chunking-config.yml";
            // Chunk size 1 with 2 missing ids should produce 2 independent retrieve+create round trips,
            // proving Chunking.Instances is actually threaded through into BetaResourceParams rather than ignored.
            var (provider, mock) = BuildProvider(path, instancesChunkSize: 1);
            using (provider)
            {
                mock.AddMatcher(new SimpleMatcher("POST", _retrievePath, (ctx, t) => Task.FromResult(MockRetrieveEmpty(ctx, t)), Times.Exactly(2)));
                mock.AddMatcher(new SimpleMatcher("POST", _upsertPath, MockUpsertEcho, Times.Exactly(2)));

                var destination = provider.GetRequiredService<CogniteDestinationWithIDM>();
                var ids = new[] { new InstanceIdentifier("mySpace", "ss1"), new InstanceIdentifier("mySpace", "ss2") };

                // Async-builder overload: the integration tests only ever call the sync-builder one.
                var result = await destination.GetOrCreateStateSetsAsync<CogniteStateSet>(
                    ids,
                    missing => Task.FromResult(missing.Select(BuildStateSet)),
                    RetryMode.None, SanitationMode.None, CancellationToken.None);

                Assert.Equal(2, result.Results.Count());
            }
            System.IO.File.Delete(path);
        }
    }
}
