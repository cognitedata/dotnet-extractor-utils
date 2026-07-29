using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using CogniteSdk;
using CogniteSdk.DataModels;
using CogniteSdk.DataModels.Core;
using CogniteSdk.Resources.Beta;
using Microsoft.Extensions.DependencyInjection;
using Xunit;
using Cognite.Extractor.Utils;
using Cognite.Extensions;
using Cognite.Extensions.DataModels.CogniteExtractorExtensions;
using Xunit.Abstractions;
using Cognite.Extractor.Testing;
using Cognite.Extractor.Testing.Mock;
using Moq;

namespace ExtractorUtils.Test.Unit
{
    /// <summary>
    /// Unit tests for the state-set methods added on top of get-or-create in this branch
    /// (<see cref="BetaStateSetsExtensions.EnsureStateSetsExistAsync{T}"/>,
    /// <see cref="BetaStateSetsExtensions.GetStateSetsByIdsIgnoreErrors{T}"/>,
    /// <see cref="BetaStateSetsExtensions.UpsertAsync{T}"/>), covering paths the integration tests
    /// never exercise: multi-chunk bookkeeping with a mixed sanitation result, GetByIdsIgnoreErrors'
    /// input validation and its (surprising, given the method's name) exception propagation on a
    /// genuine retrieve failure, and the null-argument checks on all three methods.
    /// </summary>
    public class BetaStateSetsUpsertTest
    {
        private const string _project = "someProject";
        private const string _host = "https://test.cognitedata.com";
        private const string _retrievePath = "^/api/v1/projects/" + _project + "/models/instances/byids$";
        private const string _upsertPath = "^/api/v1/projects/" + _project + "/models/instances$";

        private readonly ITestOutputHelper _output;

        public BetaStateSetsUpsertTest(ITestOutputHelper output)
        {
            _output = output;
        }

        private (ServiceProvider provider, CdfMock mock) BuildProvider(string configPath)
        {
            // max-retries: 0 disables the SDK's own transport-level retry, so failures surface to
            // BetaResourceExtensions immediately and its own retry/error-handling logic can be tested in isolation.
            string[] lines = {
                "version: 2",
                "logger:",
                "  console:",
                "    level: verbose",
                "cognite:",
               $"  project: {_project}",
               $"  host: {_host}",
                "  cdf-retries:",
                "    max-retries: 0"
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

        [Fact]
        public async Task UpsertAsync_MultiChunkWithSanitationError_MergesResultsAndErrorsCorrectly()
        {
            string path = "test-statesets-upsert-multichunk-config.yml";
            var (provider, mock) = BuildProvider(path);
            using (provider)
            {
                mock.AddMatcher(new SimpleMatcher("POST", _upsertPath, MockUpsertEcho, Times.Exactly(2)));

                var client = provider.GetRequiredService<Client>();
                // ChunkSize 1: the 2 valid items (once the oversized one is sanitized out) land in
                // separate chunks, exercising WriteChunked's multi-result merge alongside a sanitize error.
                var options = new BetaResourceParams(1, 10, RetryMode.None, SanitationMode.Remove);

                var validA = BuildStateSet(new InstanceIdentifier("mySpace", "validA"));
                var validB = BuildStateSet(new InstanceIdentifier("mySpace", "validB"));
                var invalid = BuildStateSet(new InstanceIdentifier("mySpace", new string('a', 300))); // exceeds ExternalIdMax (255)

                var result = await client.Beta.StateSets.UpsertAsync(new[] { validA, invalid, validB }, options, CancellationToken.None);

                Assert.Equal(2, result.Results.Count());
                var error = Assert.Single(result.Errors);
                Assert.Equal(ErrorType.SanitationFailed, error.Type);
            }
            System.IO.File.Delete(path);
        }

        public enum GetByIdsScenario { EmptyIds, MissingInstanceId, RetrieveThrows }

        [Theory]
        [InlineData(GetByIdsScenario.EmptyIds)]
        [InlineData(GetByIdsScenario.MissingInstanceId)]
        [InlineData(GetByIdsScenario.RetrieveThrows)]
        public async Task GetStateSetsByIdsIgnoreErrors_HandlesEdgeCases(GetByIdsScenario scenario)
        {
            string path = $"test-statesets-getbyids-{scenario}-config.yml";
            var (provider, mock) = BuildProvider(path);
            using (provider)
            {
                var retrieveTimes = scenario == GetByIdsScenario.RetrieveThrows ? Times.Once() : Times.Never();
                Func<RequestContext, CancellationToken, Task<HttpResponseMessage>> retrieveHandler = scenario == GetByIdsScenario.RetrieveThrows
                    ? (ctx, token) => Task.FromResult(ctx.CreateError(HttpStatusCode.InternalServerError, "retrieve is down"))
                    : (ctx, token) => Task.FromResult(MockRetrieveEmpty(ctx, token));
                mock.AddMatcher(new SimpleMatcher("POST", _retrievePath, retrieveHandler, retrieveTimes));

                var client = provider.GetRequiredService<Client>();

                switch (scenario)
                {
                    case GetByIdsScenario.EmptyIds:
                        var result = await client.Beta.StateSets.GetStateSetsByIdsIgnoreErrors<CogniteStateSet>(
                            Array.Empty<Identity>(), 100, 1, CancellationToken.None);
                        Assert.Empty(result);
                        break;
                    case GetByIdsScenario.MissingInstanceId:
                        await Assert.ThrowsAsync<ArgumentException>(() =>
                            client.Beta.StateSets.GetStateSetsByIdsIgnoreErrors<CogniteStateSet>(
                                new[] { Identity.Create(123L) }, 100, 1, CancellationToken.None));
                        break;
                    case GetByIdsScenario.RetrieveThrows:
                        // Despite the name, GetByIdsIgnoreErrors only ignores unknown ids -- a genuine
                        // transport/API failure propagates as an exception rather than being swallowed.
                        var id = new InstanceIdentifier("mySpace", "ss1");
                        await Assert.ThrowsAsync<ResponseException>(() =>
                            client.Beta.StateSets.GetStateSetsByIdsIgnoreErrors<CogniteStateSet>(
                                new[] { Identity.Create(id) }, 100, 1, CancellationToken.None));
                        break;
                }
            }
            System.IO.File.Delete(path);
        }

        public enum NullCheckOp { EnsureExistsOptionsNull, EnsureExistsReceiverNull, GetByIdsReceiverNull, UpsertOptionsNull, UpsertReceiverNull }

        [Theory]
        [InlineData(NullCheckOp.EnsureExistsOptionsNull)]
        [InlineData(NullCheckOp.EnsureExistsReceiverNull)]
        [InlineData(NullCheckOp.GetByIdsReceiverNull)]
        [InlineData(NullCheckOp.UpsertOptionsNull)]
        [InlineData(NullCheckOp.UpsertReceiverNull)]
        public async Task NullReceiverOrOptions_ThrowsArgumentNullException(NullCheckOp op)
        {
            string path = $"test-statesets-nullcheck-{op}-config.yml";
            var (provider, _) = BuildProvider(path);
            using (provider)
            {
                var client = provider.GetRequiredService<Client>();
                StateSetsResource nullResource = null;
                var items = new[] { BuildStateSet(new InstanceIdentifier("mySpace", "ss1")) };
                var validOptions = new BetaResourceParams(1000, 1, RetryMode.None, SanitationMode.None);

                Func<Task> action = op switch
                {
                    NullCheckOp.EnsureExistsOptionsNull => async () =>
                        await client.Beta.StateSets.EnsureStateSetsExistAsync(items, null, CancellationToken.None),
                    NullCheckOp.EnsureExistsReceiverNull => async () =>
                        await nullResource.EnsureStateSetsExistAsync(items, validOptions, CancellationToken.None),
                    NullCheckOp.GetByIdsReceiverNull => async () =>
                        await nullResource.GetStateSetsByIdsIgnoreErrors<CogniteStateSet>(Array.Empty<Identity>(), 100, 1, CancellationToken.None),
                    // Cast disambiguates from StateSetsResource's own built-in UpsertAsync(items, UpsertOptions, token)
                    // overload, which a bare `null` would otherwise resolve to (instance methods win over extension
                    // methods regardless of parameter fit, and a literal null doesn't carry a type to break the tie).
                    NullCheckOp.UpsertOptionsNull => async () =>
                        await client.Beta.StateSets.UpsertAsync(items, (BetaResourceParams)null, CancellationToken.None),
                    NullCheckOp.UpsertReceiverNull => async () =>
                        await nullResource.UpsertAsync(items, validOptions, CancellationToken.None),
                    _ => throw new ArgumentOutOfRangeException(nameof(op))
                };

                await Assert.ThrowsAsync<ArgumentNullException>(action);
            }
            System.IO.File.Delete(path);
        }
    }
}
