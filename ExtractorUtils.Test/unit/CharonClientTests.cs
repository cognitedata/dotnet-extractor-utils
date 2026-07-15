using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extensions;
using Cognite.Extractor.Testing;
using Cognite.Extractor.Testing.Mock;
using Cognite.Extractor.Utils;
using CogniteSdk;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using Moq.Protected;
using Xunit;
using Xunit.Abstractions;

namespace ExtractorUtils.Test.Unit
{
    public class CharonClientTests
    {
        private readonly ITestOutputHelper _output;

        public CharonClientTests(ITestOutputHelper output)
        {
            _output = output;
        }

        // ---------------------------------------------------------------------------
        // Helpers
        // ---------------------------------------------------------------------------

        private static (Mock<IHttpClientFactory> factory, Mock<HttpMessageHandler> handler)
            MakeFactory(Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> respond)
        {
            var handler = new Mock<HttpMessageHandler>();
            handler.Protected()
                .Setup<Task<HttpResponseMessage>>("SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .Returns<HttpRequestMessage, CancellationToken>(respond);
            var client = new HttpClient(handler.Object);
            var factory = new Mock<IHttpClientFactory>();
            factory.Setup(f => f.CreateClient(It.IsAny<string>())).Returns(client);
            return (factory, handler);
        }

        private static HttpResponseMessage Json(object body, HttpStatusCode status = HttpStatusCode.OK)
        {
            return new HttpResponseMessage(status)
            {
                Content = new StringContent(JsonSerializer.Serialize(body), Encoding.UTF8, "application/json")
            };
        }

        private static Mock<IAuthenticator> FakeAuth(string token = "test-token")
        {
            var auth = new Mock<IAuthenticator>();
            auth.Setup(a => a.GetToken(It.IsAny<CancellationToken>()))
                .ReturnsAsync(token);
            return auth;
        }

        // ---------------------------------------------------------------------------
        // SessionsResource tests
        // ---------------------------------------------------------------------------

        [Fact]
        public async Task SessionsResource_ExtractsNonce()
        {
            var sessionBody = new { items = new[] { new { id = 42, nonce = "abc-nonce", status = "READY" } } };
            var (factory, handler) = MakeFactory((_, __) =>
                Task.FromResult(Json(sessionBody)));

            var sessions = new SessionsResource(
                FakeAuth().Object,
                factory.Object,
                "https://example.cognitedata.com",
                "my-project", null, null,
                new NullLogger<SessionsResource>());

            var result = await sessions.CreateAsync(CancellationToken.None);

            Assert.Equal(42, result.Id);
            Assert.Equal("abc-nonce", result.Nonce);
            Assert.Equal("READY", result.Status);
        }

        [Fact]
        public async Task SessionsResource_NonOkStatus_Throws()
        {
            var (factory, _) = MakeFactory((_, __) =>
                Task.FromResult(Json(new { error = "Unauthorized" }, HttpStatusCode.Unauthorized)));

            var sessions = new SessionsResource(
                FakeAuth().Object,
                factory.Object,
                "https://example.cognitedata.com",
                "my-project", null, null);

            await Assert.ThrowsAsync<InvalidOperationException>(
                () => sessions.CreateAsync(CancellationToken.None));
        }

        // ---------------------------------------------------------------------------
        // CharonClient.SetupAsync tests
        // ---------------------------------------------------------------------------

        [Fact]
        public async Task SetupAsync_HappyPath_LogsJobId()
        {
            var calls = new List<string>();
            var (factory, _) = MakeFactory((req, _) =>
            {
                calls.Add(req.RequestUri!.AbsolutePath);
                if (req.RequestUri.AbsolutePath.EndsWith("/sessions"))
                    return Task.FromResult(Json(new { items = new[] { new { id = 1, nonce = "nonce-xyz", status = "READY" } } }));
                // /setup
                return Task.FromResult(Json(new { job_external_id = "charon-123-job", created = true }));
            });

            var sessions = new SessionsResource(
                FakeAuth().Object, factory.Object,
                "https://example.cognitedata.com", "my-project", null, null);

            var charon = new CharonClient(
                "https://example.cognitedata.com", "my-project",
                FakeAuth().Object, sessions, factory.Object,
                new NullLogger<CharonClient>());

            await charon.SetupAsync(CancellationToken.None);

            Assert.Contains(calls, p => p.EndsWith("/sessions"));
            Assert.Contains(calls, p => p.EndsWith("/setup"));
        }

        [Fact]
        public async Task SetupAsync_NonOkStatus_ThrowsCharonException()
        {
            var (factory, _) = MakeFactory((req, _) =>
            {
                if (req.RequestUri!.AbsolutePath.EndsWith("/sessions"))
                    return Task.FromResult(Json(new { items = new[] { new { id = 1, nonce = "nonce", status = "READY" } } }));
                return Task.FromResult(Json(new { error = "bad request" }, HttpStatusCode.BadRequest));
            });

            var sessions = new SessionsResource(
                FakeAuth().Object, factory.Object,
                "https://example.cognitedata.com", "my-project", null, null);

            var charon = new CharonClient(
                "https://example.cognitedata.com", "my-project",
                FakeAuth().Object, sessions, factory.Object);

            var ex = await Assert.ThrowsAsync<CharonException>(
                () => charon.SetupAsync(CancellationToken.None));
            Assert.Equal(400, ex.StatusCode);
        }

        // ---------------------------------------------------------------------------
        // CharonClient.InsertPayloadAsync tests
        // ---------------------------------------------------------------------------

        [Fact]
        public async Task InsertPayload_HappyPath_AcceptedTrue()
        {
            var receivedBody = "";
            var (factory, _) = MakeFactory((req, _) =>
            {
                receivedBody = req.Content!.ReadAsStringAsync(CancellationToken.None).GetAwaiter().GetResult();
                return Task.FromResult(Json(new { accepted = true }));
            });

            var sessions = new SessionsResource(
                FakeAuth().Object, factory.Object,
                "https://example.cognitedata.com", "my-project", null, null);

            var charon = new CharonClient(
                "https://example.cognitedata.com", "my-project",
                FakeAuth().Object, sessions, factory.Object);

            var items = new[]
            {
                new CharonItem { ExternalId = "ts-1", Timestamp = 1700000000000, Value = 3.14 }
            };

            await charon.InsertPayloadAsync(items, CancellationToken.None);

            // Verify camelCase field names in body
            Assert.Contains("\"externalId\"", receivedBody);
            Assert.Contains("\"timestamp\"", receivedBody);
            Assert.Contains("\"value\"", receivedBody);
            Assert.Contains("\"type\"", receivedBody);
            Assert.Contains("ts-1", receivedBody);
        }

        [Fact]
        public async Task InsertPayload_NotAccepted_ThrowsCharonException()
        {
            var (factory, _) = MakeFactory((_, __) =>
                Task.FromResult(Json(new { accepted = false })));

            var sessions = new SessionsResource(
                FakeAuth().Object, factory.Object,
                "https://example.cognitedata.com", "my-project", null, null);

            var charon = new CharonClient(
                "https://example.cognitedata.com", "my-project",
                FakeAuth().Object, sessions, factory.Object);

            await Assert.ThrowsAsync<CharonException>(
                () => charon.InsertPayloadAsync(
                    new[] { new CharonItem { ExternalId = "ts-1", Timestamp = 1, Value = 1.0 } },
                    CancellationToken.None));
        }

        [Fact]
        public async Task InsertPayload_NonOkStatus_ThrowsCharonException()
        {
            var (factory, _) = MakeFactory((_, __) =>
                Task.FromResult(Json(new { error = "internal" }, HttpStatusCode.InternalServerError)));

            var sessions = new SessionsResource(
                FakeAuth().Object, factory.Object,
                "https://example.cognitedata.com", "my-project", null, null);

            var charon = new CharonClient(
                "https://example.cognitedata.com", "my-project",
                FakeAuth().Object, sessions, factory.Object);

            var ex = await Assert.ThrowsAsync<CharonException>(
                () => charon.InsertPayloadAsync(
                    new[] { new CharonItem { ExternalId = "ts-1", Timestamp = 1, Value = 1.0 } },
                    CancellationToken.None));
            Assert.Equal(500, ex.StatusCode);
        }

        // ---------------------------------------------------------------------------
        // TimeSeriesUploadQueue Charon routing tests
        // ---------------------------------------------------------------------------

        [Fact]
        public async Task Queue_SetupCalledOnceOnFirstUpload()
        {
            var setupCount = 0;
            var insertCount = 0;

            var mockCharon = new Mock<ICharonClient>();
            mockCharon.Setup(c => c.SetupAsync(It.IsAny<CancellationToken>()))
                .Callback(() => setupCount++)
                .Returns(Task.CompletedTask);
            mockCharon.Setup(c => c.InsertPayloadAsync(
                    It.IsAny<IEnumerable<CharonItem>>(), It.IsAny<CancellationToken>()))
                .Callback(() => insertCount++)
                .Returns(Task.CompletedTask);

            var (factory, _) = MakeFactory((_, __) => Task.FromResult(Json(new { })));
            var services = new Microsoft.Extensions.DependencyInjection.ServiceCollection();
            services.AddCogniteClient("test");
            // Build a minimal destination-like object; we only call Trigger here.
            var dest = BuildMinimalDestination();
            using var queue = new TimeSeriesUploadQueue(
                dest, TimeSpan.Zero, 0,
                new NullLogger<CogniteDestination>(), null, null,
                charon: mockCharon.Object);

            var id = Identity.Create("ts-ext-1");
            queue.Enqueue((id, new Datapoint(DateTime.UtcNow, 1.0)));
            await queue.Trigger(CancellationToken.None);

            queue.Enqueue((id, new Datapoint(DateTime.UtcNow, 2.0)));
            await queue.Trigger(CancellationToken.None);

            Assert.Equal(1, setupCount);   // setup called exactly once
            Assert.Equal(2, insertCount);  // insert called on each non-empty trigger
        }

        [Fact]
        public async Task Queue_InternalIdOnlyItems_AreSkipped()
        {
            var capturedItems = new List<CharonItem>();

            var mockCharon = new Mock<ICharonClient>();
            mockCharon.Setup(c => c.SetupAsync(It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);
            mockCharon.Setup(c => c.InsertPayloadAsync(
                    It.IsAny<IEnumerable<CharonItem>>(), It.IsAny<CancellationToken>()))
                .Callback<IEnumerable<CharonItem>, CancellationToken>((items, _) =>
                    capturedItems.AddRange(items))
                .Returns(Task.CompletedTask);

            var dest = BuildMinimalDestination();
            using var queue = new TimeSeriesUploadQueue(
                dest, TimeSpan.Zero, 0,
                new NullLogger<CogniteDestination>(), null, null,
                charon: mockCharon.Object);

            // internalId-only — should be skipped
            queue.Enqueue((Identity.Create(12345L), new Datapoint(DateTime.UtcNow, 9.9)));
            // externalId — should be sent
            queue.Enqueue((Identity.Create("good-ts"), new Datapoint(DateTime.UtcNow, 1.1)));

            var result = await queue.Trigger(CancellationToken.None);

            Assert.Single(capturedItems);
            Assert.Equal("good-ts", capturedItems[0].ExternalId);
            Assert.Single(result.Failed!);
        }

        // Minimal CogniteDestination that won't hit the network in queue tests.
        // Uses CdfMock so the destination has a valid (mock-backed) Client.
        private static CogniteDestination BuildMinimalDestination()
        {
            string path = $"charon-unit-test-config-{Guid.NewGuid()}.yml";
            string[] lines = {
                "version: 2",
                "cognite:",
                "  project: test-project",
                "  host: https://example.cognitedata.com",
                "  charon:",
                "    enabled: false"   // disable Charon in the config — we inject it directly
            };
            System.IO.File.WriteAllLines(path, lines);
            try
            {
                var services = new ServiceCollection();
                services.AddConfig<BaseConfig>(path, 2);
                CdfMock.RegisterHttpClient(services);
                services.AddCogniteClient("test");
                var provider = services.BuildServiceProvider();
                return provider.GetRequiredService<CogniteDestination>();
            }
            finally
            {
                System.IO.File.Delete(path);
            }
        }
    }
}
