using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extractor.Testing.Mock;
using Cognite.Extractor.Utils.Unstable.Charon;
using Cognite.Extractor.Utils.Unstable.Configuration;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using Xunit;
using Xunit.Abstractions;

namespace ExtractorUtils.Test.unit.Unstable
{
    /// <summary>
    /// Tests the Charon HTTP client against a mocked transport: setup success/422, payload
    /// 200/207/422, and transport failures mapping to exceptions.
    /// </summary>
    public class CharonClientTests
    {
        private const string BaseUrl = "https://charon.example";
        private readonly ITestOutputHelper _output;

        /// <summary>Create the test with xUnit output.</summary>
        /// <param name="output">Test output helper.</param>
        public CharonClientTests(ITestOutputHelper output)
        {
            _output = output;
        }

        /// <summary>Build a Charon client wired to the given HTTP transport.</summary>
        private static CharonClient BuildClient(HttpClient http)
        {
            return new CharonClient(http, new CdfWriterConfig { BaseUrl = BaseUrl }, new NullLogger<CharonClient>());
        }

        /// <summary>Build a JSON HTTP response with a raw body string.</summary>
        private static HttpResponseMessage Json(HttpStatusCode status, string body)
        {
            return new HttpResponseMessage(status)
            {
                Content = new StringContent(body, Encoding.UTF8, "application/json"),
            };
        }

        private static CharonSetupRequest SampleSetup() => new CharonSetupRequest
        {
            IntegrationId = "int-1",
            ExtractorType = "ext",
            ExtractorVersion = "1.0.0",
            Items = new List<CharonSetupItem>
            {
                CharonTask.Custom("task-1", CharonDestinations.TimeSeriesCreate,
                    new[] { new KeyValuePair<string, string>("externalId", "input.id") }),
            },
        };

        [Fact]
        public async Task SetupSucceedsOn200()
        {
            using var mock = new CdfMock(new NullLogger<CdfMock>());
            mock.AddMatcher(new SimpleMatcher("POST", "/cdfwriter/setup",
                (ctx, t) => Json(HttpStatusCode.OK, "{}"), Times.Once()));
            using var http = new HttpClient(mock, disposeHandler: false);
            var client = BuildClient(http);

            await client.SetupAsync(SampleSetup(), CancellationToken.None);
        }

        [Fact]
        public async Task SetupThrowsListingEveryFailureOn422()
        {
            var body = "{\"error\":{\"code\":422,\"message\":\"validation failed\",\"errors\":[" +
                "{\"itemIndex\":0,\"taskName\":\"task-1\",\"reason\":\"bad\"}," +
                "{\"itemIndex\":1,\"taskName\":\"task-2\",\"reason\":\"worse\"}]}}";
            using var mock = new CdfMock(new NullLogger<CdfMock>());
            mock.AddMatcher(new SimpleMatcher("POST", "/cdfwriter/setup",
                (ctx, t) => Json((HttpStatusCode)422, body), Times.Once()));
            using var http = new HttpClient(mock, disposeHandler: false);
            var client = BuildClient(http);

            var ex = await Assert.ThrowsAsync<CharonSetupException>(
                () => client.SetupAsync(SampleSetup(), CancellationToken.None));
            Assert.Equal(2, ex.Errors.Count);
            Assert.Equal("task-1", ex.Errors[0].TaskName);
            Assert.Equal("task-2", ex.Errors[1].TaskName);
        }

        [Fact]
        public async Task PayloadReturnsPerTaskResultsOn200()
        {
            var body = "{\"tasks\":{\"task-1\":{\"status\":200,\"body\":{}}}}";
            using var mock = new CdfMock(new NullLogger<CdfMock>());
            mock.AddMatcher(new SimpleMatcher("POST", "/cdfwriter/payload",
                (ctx, t) => Json(HttpStatusCode.OK, body), Times.Once()));
            using var http = new HttpClient(mock, disposeHandler: false);
            var client = BuildClient(http);

            var result = await client.SendPayloadAsync(
                new CharonPayloadRequest { IntegrationId = "int-1" }, CancellationToken.None);
            Assert.Equal(200, result.Tasks["task-1"].Status);
        }

        [Fact]
        public async Task PayloadReturnsMixedStatusesOn207()
        {
            var body = "{\"tasks\":{\"task-1\":{\"status\":200,\"body\":{}},\"task-2\":{\"status\":403,\"body\":{\"error\":{}}}}}";
            using var mock = new CdfMock(new NullLogger<CdfMock>());
            mock.AddMatcher(new SimpleMatcher("POST", "/cdfwriter/payload",
                (ctx, t) => Json((HttpStatusCode)207, body), Times.Once()));
            using var http = new HttpClient(mock, disposeHandler: false);
            var client = BuildClient(http);

            var result = await client.SendPayloadAsync(
                new CharonPayloadRequest { IntegrationId = "int-1" }, CancellationToken.None);
            Assert.Equal(200, result.Tasks["task-1"].Status);
            Assert.Equal(403, result.Tasks["task-2"].Status);
        }

        [Fact]
        public async Task PayloadThrowsValidationExceptionOn422()
        {
            var body = "{\"error\":{\"code\":422,\"message\":\"payload validation failed\",\"errors\":[" +
                "{\"itemIndex\":4,\"taskName\":\"task-1\",\"reason\":\"externalId evaluated to null\"}]}}";
            using var mock = new CdfMock(new NullLogger<CdfMock>());
            mock.AddMatcher(new SimpleMatcher("POST", "/cdfwriter/payload",
                (ctx, t) => Json((HttpStatusCode)422, body), Times.Once()));
            using var http = new HttpClient(mock, disposeHandler: false);
            var client = BuildClient(http);

            var ex = await Assert.ThrowsAsync<CharonPayloadValidationException>(
                () => client.SendPayloadAsync(new CharonPayloadRequest { IntegrationId = "int-1" }, CancellationToken.None));
            Assert.Single(ex.Errors);
            Assert.Equal(4, ex.Errors[0].ItemIndex);
        }

        [Fact]
        public async Task PayloadThrowsCharonExceptionOn401()
        {
            using var mock = new CdfMock(new NullLogger<CdfMock>());
            mock.AddMatcher(new SimpleMatcher("POST", "/cdfwriter/payload",
                (ctx, t) => Json(HttpStatusCode.Unauthorized, "{\"error\":{\"code\":401,\"message\":\"no token\"}}"), Times.Once()));
            using var http = new HttpClient(mock, disposeHandler: false);
            var client = BuildClient(http);

            var ex = await Assert.ThrowsAsync<CharonException>(
                () => client.SendPayloadAsync(new CharonPayloadRequest { IntegrationId = "int-1" }, CancellationToken.None));
            Assert.Equal(401, ex.Status);
        }

        [Fact]
        public async Task PayloadThrowsCharonExceptionOn5xx()
        {
            using var mock = new CdfMock(new NullLogger<CdfMock>());
            mock.AddMatcher(new SimpleMatcher("POST", "/cdfwriter/payload",
                (ctx, t) => Json(HttpStatusCode.ServiceUnavailable, "{\"error\":{\"code\":503,\"message\":\"down\"}}"), Times.Once()));
            using var http = new HttpClient(mock, disposeHandler: false);
            var client = BuildClient(http);

            var ex = await Assert.ThrowsAsync<CharonException>(
                () => client.SendPayloadAsync(new CharonPayloadRequest { IntegrationId = "int-1" }, CancellationToken.None));
            Assert.Equal(503, ex.Status);
        }
    }
}
