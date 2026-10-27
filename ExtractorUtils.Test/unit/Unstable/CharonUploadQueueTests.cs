using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extensions;
using Cognite.Extractor.Utils;
using Cognite.Extractor.Utils.Unstable;
using Cognite.Extractor.Utils.Unstable.Charon;
using Cognite.Extractor.Utils.Unstable.Configuration;
using CogniteSdk;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace ExtractorUtils.Test.unit.Unstable
{
    /// <summary>
    /// Tests the Charon upload queues: payload body shape, chunking to CDF limits, datapoint
    /// one-item-per-reading expansion, result mapping, and opt-in DI registration.
    /// </summary>
    public class CharonUploadQueueTests
    {
        /// <summary>Fake Charon client that records payloads and returns a configurable status.</summary>
        private class FakeCharonClient : ICharonClient
        {
            public List<CharonPayloadRequest> Payloads { get; } = new List<CharonPayloadRequest>();
            public int StatusToReturn { get; set; } = 200;
            public Exception? ThrowOnPayload { get; set; }

            public Task SetupAsync(CharonSetupRequest request, CancellationToken token) => Task.CompletedTask;

            public Task<CharonPayloadResponse> SendPayloadAsync(CharonPayloadRequest request, CancellationToken token)
            {
                Payloads.Add(request);
                if (ThrowOnPayload != null) throw ThrowOnPayload;
                var tasks = new Dictionary<string, CharonTaskResult>();
                foreach (var key in request.Tasks.Keys)
                {
                    tasks[key] = new CharonTaskResult { Status = StatusToReturn };
                }
                return Task.FromResult(new CharonPayloadResponse { Tasks = tasks });
            }
        }

        private static JsonElement Item(string externalId)
        {
            return JsonSerializer.Deserialize<JsonElement>($"{{\"externalId\":\"{externalId}\"}}");
        }

        [Fact]
        public async Task UploadBuildsPayloadForTaskOn200()
        {
            var fake = new FakeCharonClient();
            await using var queue = new CharonUploadQueue(fake, "int-1", "task-1",
                Timeout.InfiniteTimeSpan, 0, 1000, new NullLogger<CharonUploadQueue>());
            queue.Enqueue(Item("a"));
            queue.Enqueue(Item("b"));

            var result = await queue.Trigger(CancellationToken.None);

            Assert.Single(fake.Payloads);
            Assert.Equal("int-1", fake.Payloads[0].IntegrationId);
            Assert.Equal(2, fake.Payloads[0].Tasks["task-1"].Count);
            Assert.Equal(2, result.Uploaded!.Count());
            Assert.Empty(result.Failed!);
            Assert.False(result.IsFailed);
        }

        [Fact]
        public async Task UploadChunksToTimeSeriesLimit()
        {
            var fake = new FakeCharonClient();
            await using var queue = new CharonUploadQueue(fake, "int-1", "task-1",
                Timeout.InfiniteTimeSpan, 0, 2, new NullLogger<CharonUploadQueue>());
            for (int i = 0; i < 5; i++) queue.Enqueue(Item($"e{i}"));

            await queue.Trigger(CancellationToken.None);

            // 5 items, chunk size 2 -> 3 calls.
            Assert.Equal(3, fake.Payloads.Count);
            Assert.Equal(2, fake.Payloads[0].Tasks["task-1"].Count);
            Assert.Equal(1, fake.Payloads[2].Tasks["task-1"].Count);
        }

        [Fact]
        public async Task UploadMarksChunkFailedOnNon2xxTaskStatus()
        {
            var fake = new FakeCharonClient { StatusToReturn = 403 };
            await using var queue = new CharonUploadQueue(fake, "int-1", "task-1",
                Timeout.InfiniteTimeSpan, 0, 1000, new NullLogger<CharonUploadQueue>());
            queue.Enqueue(Item("a"));

            var result = await queue.Trigger(CancellationToken.None);

            Assert.Empty(result.Uploaded!);
            Assert.Single(result.Failed!);
            Assert.False(result.IsFailed); // per-task failure, not a transport fatal
        }

        [Fact]
        public async Task UploadReturnsFatalOnTransportException()
        {
            var fake = new FakeCharonClient { ThrowOnPayload = new CharonException("down", 503) };
            await using var queue = new CharonUploadQueue(fake, "int-1", "task-1",
                Timeout.InfiniteTimeSpan, 0, 1000, new NullLogger<CharonUploadQueue>());
            queue.Enqueue(Item("a"));

            var result = await queue.Trigger(CancellationToken.None);

            Assert.True(result.IsFailed);
            Assert.IsType<CharonException>(result.Exception);
        }

        [Fact]
        public async Task DatapointsExpandOneItemPerReading()
        {
            var fake = new FakeCharonClient();
            await using var queue = new CharonDatapointsUploadQueue(fake, "int-1", "dps",
                Timeout.InfiniteTimeSpan, 0, 100000, 10000, new NullLogger<CharonDatapointsUploadQueue>());
            queue.Enqueue("ts-1", new Datapoint(DateTime.UtcNow, 1.0));
            queue.Enqueue("ts-1", new Datapoint(DateTime.UtcNow, 2.0));

            await queue.Trigger(CancellationToken.None);

            Assert.Single(fake.Payloads);
            var items = fake.Payloads[0].Tasks["dps"];
            Assert.Equal(2, items.Count);
            Assert.Equal("ts-1", items[0].GetProperty("externalId").GetString());
            Assert.True(items[0].TryGetProperty("timestamp", out _));
            Assert.True(items[0].TryGetProperty("value", out _));
        }

        [Fact]
        public async Task DatapointsChunkToDatapointLimit()
        {
            var fake = new FakeCharonClient();
            await using var queue = new CharonDatapointsUploadQueue(fake, "int-1", "dps",
                Timeout.InfiniteTimeSpan, 0, 2, 10000, new NullLogger<CharonDatapointsUploadQueue>());
            for (int i = 0; i < 5; i++) queue.Enqueue("ts-1", new Datapoint(DateTime.UtcNow.AddSeconds(i), i));

            await queue.Trigger(CancellationToken.None);

            // 5 datapoints, limit 2 -> 3 calls.
            Assert.Equal(3, fake.Payloads.Count);
        }

        [Fact]
        public void AddCharonWriterRegistersClientAndWriter()
        {
            var services = new ServiceCollection();
            services.AddLogging();
            services.AddSingleton(new ConnectionConfig
            {
                Project = "proj",
                Integration = new IntegrationConfig { ExternalId = "int-1" },
                CdfWriter = new CdfWriterConfig { Enabled = true, BaseUrl = "https://charon.example" },
            });
            services.AddSingleton(new BaseCogniteConfig());
            services.AddCharonWriter();

            using var provider = services.BuildServiceProvider();

            Assert.NotNull(provider.GetService<ICharonClient>());
            Assert.NotNull(provider.GetService<CharonWriter>());
        }
    }
}
