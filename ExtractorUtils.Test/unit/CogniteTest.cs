using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extensions;
using Cognite.Extractor.Testing;
using Cognite.Extractor.Testing.Mock;
using Cognite.Extractor.Utils;
using CogniteSdk;
using Microsoft.Extensions.DependencyInjection;
using Moq;
using Xunit;

namespace ExtractorUtils.Test.Unit
{
    class TokenMatcher : RequestMatcher
    {
        public override string Name => "TokenMatcher";
        private int _tokenCounter;

        private readonly int _expiresIn;
        public TokenMatcher(Times times, int expiresIn)
        {
            ExpectedRequestCount = times;
            _expiresIn = expiresIn;
        }

        public override Task<HttpResponseMessage> Handle(RequestContext context, CancellationToken token)
        {
            dynamic res = new ExpandoObject();
            res.token_type = "Bearer";
            res.expires_in = _expiresIn;
            res.access_token = $"token{_tokenCounter}";
            _tokenCounter++;
            return Task.FromResult(context.CreateJsonResponse(res));
        }

        public override bool Matches(HttpMethod method, string path)
        {
            return method == HttpMethod.Post && path.EndsWith("/token");
        }
    }

    public class CogniteTest
    {
        private const string _authTenant = "someTenant";
        private const string _project = "someProject";
        private const string _host = "https://test.cognitedata.com";

        private readonly ITestOutputHelper _output;
        public CogniteTest(ITestOutputHelper output)
        {
            _output = output;
        }

        private RequestMatcher MockAssetsList(Times times)
        {
            return new SimpleMatcher("POST",
                $"/api/v1/projects/{_project}/assets/list",
                (ctx, token) =>
                {
                    return ctx.CreateJsonResponse(new ItemsWithCursor<Asset>
                    {
                        Items = new List<Asset>
                        {
                            new Asset { Id = 1, Name = "Asset1" },
                        },
                    });
                },
                times
            );
        }

        [Fact]
        public async Task TestAuthenticator()
        {
            var clientId = "someId";
            string path = "test-authenticator-config.yml";
            string[] lines = {  "version: 2",
                                "logger:",
                                "  console:",
                                "    level: verbose",
                                "cognite:",
                                "  cdf-retries:",
                                "    max-retries: 1",
                                "  idp-authentication:",
                                "    implementation: Basic",
                               $"    client-id: {clientId}",
                                "    token-url: http://example.url/token",
                                "    secret: thisIsASecret",
                                "    scopes: ",
                                "      - thisIsAScope",
                                "    min-ttl: 0" };
            System.IO.File.WriteAllLines(path, lines);

            // Setup services
            var services = new ServiceCollection();
            var config = services.AddConfig<BaseConfig>(path, 2);
            services.AddTestLogging(_output);
            CdfMock.RegisterHttpClient(services);
            services.AddCogniteClient("myApp");


            using (var provider = services.BuildServiceProvider())
            {
                dynamic errorRes = new ExpandoObject();
                errorRes.error = "invalid_scope";
                var mock = provider.GetRequiredService<CdfMock>();
                mock.AddMatcher(new FailIfMatcher(
                    new TokenMatcher(Times.Exactly(3), 1),
                    (Func<RequestMatcher, bool>)(matcher => matcher.RequestCount >= 3),
                    HttpStatusCode.InternalServerError,
                    errorRes
                ));

                var auth = provider.GetRequiredService<IAuthenticator>();
                var token = await auth.GetToken(TestContext.Current.CancellationToken);
                Assert.Equal("token0", token);
                token = await auth.GetToken(TestContext.Current.CancellationToken); // same token
                Assert.Equal("token0", token);
                await Task.Delay(2000, TestContext.Current.CancellationToken); // token expired
                token = await auth.GetToken(TestContext.Current.CancellationToken); // new token
                Assert.Equal("token1", token);
                await Task.Delay(2100, TestContext.Current.CancellationToken); // token expired
                await Assert.ThrowsAsync<CogniteUtilsException>(() => auth.GetToken(TestContext.Current.CancellationToken)); // failed, returns null
            }

            System.IO.File.Delete(path);
        }

        [Fact]
        public async Task TestClientRetry()
        {
            string path = "test-cognite-retry-config.yml";
            string[] lines = {  "version: 2",
                                "logger:",
                                "  console:",
                                "    level: verbose",
                                "cognite:",
                               $"  project: {_project}",
                               $"  host: {_host}",
                                "  cdf-retries:",
                                "    max-retries: 3",
                                "    timeout: 10000" };
            System.IO.File.WriteAllLines(path, lines);

            // Setup services
            var services = new ServiceCollection();
            services.AddConfig<BaseConfig>(path, 2);
            services.AddTestLogging(_output);
            CdfMock.RegisterHttpClient(services);
            services.AddCogniteClient("testApp", setLogger: true);
            using (var provider = services.BuildServiceProvider())
            {
                var mock = provider.GetRequiredService<CdfMock>();
                mock.AddMatcher(new FailNTimesMatcher(2, MockAssetsList(Times.Once()), HttpStatusCode.InternalServerError));
                var cogniteDestination = provider.GetRequiredService<CogniteDestination>();

                var listed = await cogniteDestination.CogniteClient.Assets.ListAsync(new AssetQuery(), TestContext.Current.CancellationToken);
                Assert.Single(listed.Items);
                Assert.Equal(1, listed.Items.First().Id);
            }
        }

        [Fact]
        public async Task TestClientAuthRetry()
        {
            string path = "test-cognite-retry-auth-config.yml";
            string[] lines = {  "version: 2",
                                "logger:",
                                "  console:",
                                "    level: verbose",
                                "cognite:",
                               $"  project: {_project}",
                               $"  host: {_host}",
                                "  cdf-retries:",
                                "    max-retries: 3",
                                "    timeout: 10000",
                                "  idp-authentication:",
                                "    implementation: Basic",
                                "    client-id: someId",
                                "    token-url: http://example.url/token",
                                "    secret: thisIsASecret",
                                "    scopes: ",
                                "      - thisIsAScope"
                             };
            System.IO.File.WriteAllLines(path, lines);

            // Setup services
            var services = new ServiceCollection();
            var config = services.AddConfig<BaseConfig>(path, 2);
            services.AddSingleton(config.Cognite.IdpAuthentication);
            services.AddTestLogging(_output);
            CdfMock.RegisterHttpClient(services);
            services.AddCogniteClient("testApp", setLogger: true);
            using var provider = services.BuildServiceProvider();

            dynamic errorRes = new ExpandoObject();
            errorRes.error = "invalid_scope";
            var mock = provider.GetRequiredService<CdfMock>();
            // First we hit the auth endpoint 3 times, to get an initial valid token.
            // Next, we hit the assets endpoint once, it fails, and when retrying we need to fetch a new token.
            // This means we hit the auth endpoint 5 times in total.
            var tokenMatcher = mock.AddMatcher(new FailNTimesMatcher(
                2,
                new TokenMatcher(Times.Exactly(3), 0),
                HttpStatusCode.InternalServerError,
                errorRes
            ));
            var assetsMatcher = mock.AddMatcher(new FailNTimesMatcher(
                2,
                MockAssetsList(Times.Once()),
                HttpStatusCode.InternalServerError,
                errorRes
            ));

            var cogniteDestination = provider.GetRequiredService<CogniteDestination>();
            await cogniteDestination.CogniteClient.Assets.ListAsync(new AssetQuery(), TestContext.Current.CancellationToken);

            tokenMatcher.AssertAndReset();
            assetsMatcher.AssertAndReset();
            // Hit the token endpoint twice more.
            tokenMatcher.ExpectedRequestCount = Times.Exactly(2);
            assetsMatcher.ExpectedRequestCount = Times.Never();

            config.Cognite.CdfRetries.MaxRetries = 1; // Set max retries to 1.
            // Try again, this time it should fail.
            await Assert.ThrowsAsync<CogniteUtilsException>(() => cogniteDestination.CogniteClient.Assets.ListAsync(new AssetQuery(), TestContext.Current.CancellationToken));
        }

        [Theory]
        [InlineData("id1", "id2")]
        [InlineData("id3", "id4", "id5", "id6", "id7")]
        [InlineData("missing1", "missing2")]
        [InlineData("id8", "id9", "missing3", "id10", "missing4")]
        [InlineData("duplicated1", "duplicated2")]
        [InlineData("id11", "id12", "duplicated3", "id13", "duplicated4")]
        [InlineData("id14", "missing5", "id15", "duplicated5", "missing6", "duplicated6")]
        [InlineData("id16", "id17", "missing7", "duplicated7-2", "duplicated8-4", "duplicated9-3")]
        /// <summary>
        /// External ids starting with 'id' exist in the mocked endpoint.
        /// External ids starting with 'missing' do not exist, but can be successfully created.
        /// External ids starting with 'duplicated' do not exist, and fail during creation as duplicated.
        /// Duplicated with a suffix '-N', where N is an int will be reported by the endpoint as duplicated
        /// a total of N times.
        /// </summary>
        /// <param name="ids"></param>
        /// <returns></returns>
        public async Task TestEnsureTimeSeries(params string[] ids)
        {
            string path = "test-ensure-time-series-config.yml";
            string[] lines = {  "version: 2",
                                "logger:",
                                "  console:",
                                "    level: verbose",
                                "cognite:",
                               $"  project: {_project}",
                               $"  host: {_host}",
                                "  cdf-chunking:",
                                "    time-series: 2",
                                "  cdf-throttling:",
                                "    time-series: 2" };
            System.IO.File.WriteAllLines(path, lines);

            // Setup services
            var services = new ServiceCollection();
            services.AddConfig<BaseConfig>(path, 2);
            services.AddTestLogging(_output);
            CdfMock.RegisterHttpClient(services);
            services.AddCogniteClient("testApp");
            using (var provider = services.BuildServiceProvider())
            {
                var cogniteDestination = provider.GetRequiredService<CogniteDestination>();
                var mock = provider.GetRequiredService<CdfMock>();
                mock.AddMatcher(new SimpleMatcher("POST",
                    $"/api/v1/projects/{_project}/timeseries.*",
                    MockEnsureTimeSeriesSendAsync,
                    Times.AtLeastOnce())
                );

                Func<IEnumerable<string>, IEnumerable<TimeSeriesCreate>> createFunction =
                    (idxs) =>
                    {
                        var toCreate = new List<TimeSeriesCreate>();
                        foreach (var id in idxs)
                        {
                            toCreate.Add(new TimeSeriesCreate
                            {
                                ExternalId = id

                            });
                        }
                        return toCreate;
                    };
                var ts = await cogniteDestination.GetOrCreateTimeSeriesAsync(
                    ids,
                    createFunction,
                    RetryMode.OnErrorKeepDuplicates,
                    SanitationMode.Remove,
                    CancellationToken.None
                );
                Assert.Equal(ids?.Length, ts.Results.Where(t => ids.Contains(t.ExternalId)).Count());
                foreach (var t in ts.Results)
                {
                    _ensuredTimeSeries.TryRemove(t.ExternalId, out _);
                }

                var newTs = createFunction(ids);
                using (var source = new CancellationTokenSource(5_000))
                {
                    // a timeout would fail the test
                    await cogniteDestination.EnsureTimeSeriesExistsAsync(newTs, RetryMode.OnFatal, SanitationMode.Remove, source.Token);
                }
                Assert.Equal(ids.Length, _ensuredTimeSeries
                    .Where(kvp => ids.Contains(kvp.Key)).Count());
            }

            System.IO.File.Delete(path);
        }

        [Theory]
        [InlineData("id1", "id2")]
        [InlineData("id3", "id4", "id5", "id6", "id7")]
        [InlineData("missing1", "missing2")]
        [InlineData("id8", "id9", "missing3", "id10", "missing4")]
        [InlineData("duplicated1", "duplicated2")]
        [InlineData("id11", "id12", "duplicated3", "id13", "duplicated4")]
        [InlineData("id14", "missing5", "id15", "duplicated5", "missing6", "duplicated6")]
        [InlineData("id16", "id17", "missing7", "duplicated7-2", "duplicated8-4", "duplicated9-3")]
        /// <summary>
        /// External ids starting with 'id' exist in the mocked endpoint.
        /// External ids starting with 'missing' do not exist, but can be successfully created.
        /// External ids starting with 'duplicated' do not exist, and fail during creation as duplicated.
        /// Duplicated with a suffix '-N', where N is an int will be reported by the endpoint as duplicated
        /// a total of N times.
        /// </summary>
        /// <param name="ids"></param>
        /// <returns></returns>
        public async Task TestEnsureAssets(params string[] ids)
        {
            string path = "test-ensure-assets-config.yml";
            string[] lines = {  "version: 2",
                                "logger:",
                                "  console:",
                                "    level: verbose",
                                "cognite:",
                               $"  project: {_project}",
                               $"  host: {_host}",
                                "  cdf-chunking:",
                                "    assets: 2",
                                "  cdf-throttling:",
                                "    assets: 2" };
            System.IO.File.WriteAllLines(path, lines);
            // Setup services
            var services = new ServiceCollection();
            services.AddConfig<BaseConfig>(path, 2);
            services.AddTestLogging(_output);
            CdfMock.RegisterHttpClient(services);
            services.AddCogniteClient("testApp");
            using (var provider = services.BuildServiceProvider())
            {
                var cogniteDestination = provider.GetRequiredService<CogniteDestination>();
                var mock = provider.GetRequiredService<CdfMock>();
                mock.AddMatcher(new SimpleMatcher("POST",
                    $"/api/v1/projects/{_project}/assets.*",
                    mockEnsureAssetsSendAsync,
                    Times.AtLeastOnce())
                );

                Func<IEnumerable<string>, IEnumerable<AssetCreate>> createFunction =
                    (idx) =>
                    {
                        var toCreate = new List<AssetCreate>();
                        foreach (var id in idx)
                        {
                            toCreate.Add(new AssetCreate
                            {
                                ExternalId = id,
                                Name = id
                            });
                        }
                        return toCreate;
                    };
                var ts = await cogniteDestination.GetOrCreateAssetsAsync(
                    ids,
                    createFunction,
                    RetryMode.OnErrorKeepDuplicates,
                    SanitationMode.Remove,
                    CancellationToken.None
                );
                ts.ThrowOnFatal();
                Assert.Equal(ids?.Length, ts.Results.Where(t => ids.Contains(t.ExternalId)).Count());
                foreach (var t in ts.Results)
                {
                    _ensuredAssets.Remove(t.ExternalId, out _);
                }

                var newAssets = createFunction(ids);
                using (var source = new CancellationTokenSource(5_000))
                {
                    // a timeout would fail the test
                    await cogniteDestination.EnsureAssetsExistsAsync(newAssets, RetryMode.OnFatal, SanitationMode.Remove, source.Token);
                }
                Assert.Equal(ids.Length, _ensuredAssets
                    .Where(kvp => ids.Contains(kvp.Key)).Count());
            }

            System.IO.File.Delete(path);
        }

        #region mock
        private static ConcurrentDictionary<string, int> _ensuredTimeSeries = new ConcurrentDictionary<string, int>();

        internal static async Task<HttpResponseMessage> MockEnsureTimeSeriesSendAsync(
            RequestContext context,
            CancellationToken token)
        {
            var uri = context.RawRequest.RequestUri.ToString();

            if (uri.Contains("/timeseries/byids"))
            {
                var ids = await context.ReadJsonBody<ItemsWithIgnoreUnknownIds<CogniteExternalId>>();
                Assert.True(ids.IgnoreUnknownIds);

                dynamic result = new ExpandoObject();
                result.items = new List<ExpandoObject>();

                foreach (var item in ids.Items)
                {
                    string id = item.ExternalId;
                    var ensured = _ensuredTimeSeries.TryGetValue(id, out int countdown) && countdown <= 0;
                    if (ensured || id.StartsWith("id"))
                    {
                        dynamic tsData = new ExpandoObject();
                        tsData.externalId = id;
                        tsData.isString = id.Contains("String") ? true : false;
                        result.items.Add(tsData);
                        _ensuredTimeSeries.TryAdd(id, 0);
                    }
                }

                return context.CreateJsonResponse(result);
            }
            else
            {
                var items = await context.ReadJsonBody<ItemsWithIgnoreUnknownIds<TimeSeriesCreate>>();

                dynamic duplicateData = new ExpandoObject();
                duplicateData.error = new ExpandoObject();
                duplicateData.error.code = 409;
                duplicateData.error.message = "ExternalIds duplicated";
                duplicateData.error.duplicated = new List<ExpandoObject>();

                dynamic result = new ExpandoObject();
                result.items = new List<ExpandoObject>();

                foreach (var item in items.Items)
                {
                    string id = item.ExternalId;
                    var hasValue = _ensuredTimeSeries.TryGetValue(id, out int countdown);
                    if ((!hasValue || countdown > 0) && id.StartsWith("duplicated"))
                    {
                        var splittedId = id.Split('-');
                        var count = splittedId.Length == 2 ? int.Parse(splittedId[1]) - 1 : 0;
                        dynamic duplicatedId = new ExpandoObject();
                        duplicatedId.externalId = id;
                        duplicateData.error.duplicated.Add(duplicatedId);
                        _ensuredTimeSeries[id] = hasValue ? countdown - 1 : count;
                    }
                    else
                    {
                        dynamic tsData = new ExpandoObject();
                        tsData.externalId = id;
                        result.items.Add(tsData);
                        _ensuredTimeSeries.TryAdd(id, 0);
                    }
                }
                if (duplicateData.error.duplicated.Count > 0)
                {
                    return context.CreateJsonResponse(duplicateData, HttpStatusCode.Conflict);
                }
                else
                {
                    return context.CreateJsonResponse(result);
                }
            }
        }

        private static ConcurrentDictionary<string, int> _ensuredAssets = new ConcurrentDictionary<string, int>();

        private static async Task<HttpResponseMessage> mockEnsureAssetsSendAsync(
            RequestContext context,
            CancellationToken token)
        {
            var uri = context.RawRequest.RequestUri.ToString();

            if (uri.Contains("/assets/byids"))
            {
                var ids = await context.ReadJsonBody<ItemsWithIgnoreUnknownIds<CogniteExternalId>>();
                Assert.True(ids.IgnoreUnknownIds);

                dynamic result = new ExpandoObject();
                result.items = new List<ExpandoObject>();

                foreach (var item in ids.Items)
                {
                    string id = item.ExternalId;
                    var ensured = _ensuredAssets.TryGetValue(id, out int countdown) && countdown <= 0;
                    if (ensured || id.StartsWith("id"))
                    {
                        dynamic assetData = new ExpandoObject();
                        assetData.externalId = id;
                        result.items.Add(assetData);
                        _ensuredAssets.TryAdd(id, 0);
                    }
                }
                return context.CreateJsonResponse(result);
            }
            else
            {
                var items = await context.ReadJsonBody<ItemsWithIgnoreUnknownIds<TimeSeriesCreate>>();
                dynamic duplicateData = new ExpandoObject();
                duplicateData.error = new ExpandoObject();
                duplicateData.error.code = 409;
                duplicateData.error.message = "ExternalIds duplicated";
                duplicateData.error.duplicated = new List<ExpandoObject>();

                dynamic result = new ExpandoObject();
                result.items = new List<ExpandoObject>();

                foreach (var item in items.Items)
                {
                    string id = item.ExternalId;
                    var hasValue = _ensuredAssets.TryGetValue(id, out int countdown);
                    if ((!hasValue || countdown > 0) && id.StartsWith("duplicated"))
                    {
                        var splittedId = id.Split('-');
                        var count = splittedId.Length == 2 ? int.Parse(splittedId[1]) - 1 : 0;
                        dynamic duplicatedId = new ExpandoObject();
                        duplicatedId.externalId = id;
                        duplicateData.error.duplicated.Add(duplicatedId);
                        _ensuredAssets[id] = hasValue ? countdown - 1 : count;
                    }
                    else
                    {
                        dynamic assetData = new ExpandoObject();
                        assetData.externalId = id;
                        result.items.Add(assetData);
                        _ensuredAssets.TryAdd(id, 0);
                    }
                }
                if (duplicateData.error.duplicated.Count > 0)
                {
                    return context.CreateJsonResponse(duplicateData, HttpStatusCode.Conflict);
                }
                else
                {
                    return context.CreateJsonResponse(result);
                }
            }
        }

        #endregion
    }
}