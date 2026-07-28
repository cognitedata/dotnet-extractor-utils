using Cognite.Extensions;
using Cognite.Extractor.Common;
using Cognite.Extractor.Utils;
using Cognite.Extractor.Testing;
using CogniteSdk;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Dynamic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;
using Cognite.Extractor.Testing.Mock;
using Moq;

namespace ExtractorUtils.Test.Unit
{
    public class EventTest
    {
        private const string _project = "someProject";
        private const string _host = "https://test.cognitedata.com";

        private readonly ITestOutputHelper _output;
        public EventTest(ITestOutputHelper output)
        {
            _output = output;
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
        public async Task TestEnsureEvents(params string[] ids)
        {
            if (ids == null) throw new ArgumentNullException(nameof(ids));
            string path = "test-ensure-events-config.yml";
            string[] lines = {  "version: 2",
                                "logger:",
                                "  console:",
                                "    level: verbose",
                                "cognite:",
                               $"  project: {_project}",
                               $"  host: {_host}",
                                "  cdf-chunking:",
                                "    events: 2",
                                "  cdf-throttling:",
                                "    events: 2" };
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
                var events = new EventsMock();

                foreach (var id in ids)
                {
                    if (id.StartsWith("duplicated"))
                    {
                        events.MockEvent(id);
                    }
                }

                mock.AddMatcher(events.MakeCreateEventsMatcher(Times.AtLeast(1)));
                mock.AddMatcher(events.MakeGetByIdsMatcher(Times.AtLeast(1)));
                Func<IEnumerable<string>, IEnumerable<EventCreate>> createFunction =
                    (idxs) =>
                    {
                        var toCreate = new List<EventCreate>();
                        foreach (var id in idxs)
                        {
                            toCreate.Add(new EventCreate
                            {
                                ExternalId = id
                            });
                        }
                        return toCreate;
                    };
                var ts = await cogniteDestination.GetOrCreateEventsAsync(
                    ids,
                    createFunction,
                    RetryMode.OnErrorKeepDuplicates,
                    SanitationMode.Remove,
                    CancellationToken.None
                );
                Assert.Equal(ids.Length, ts.Results.Count());
                Assert.Equal(events.Events.Count, ids.Length);
                events.Clear();

                foreach (var id in ids)
                {
                    if (id.StartsWith("duplicated"))
                    {
                        events.MockEvent(id);
                    }
                }

                var newEvents = createFunction(ids);
                using (var source = new CancellationTokenSource(5_000))
                {
                    // a timeout would fail the test
                    await cogniteDestination.EnsureEventsExistsAsync(newEvents, RetryMode.OnFatal, SanitationMode.Remove, source.Token);
                }
                Assert.Equal(ids.Length, events.Events.Count);
            }

            System.IO.File.Delete(path);
        }

        [Fact]
        public async Task TestUploadQueue()
        {
            string path = "test-event-queue-config.yml";
            string[] lines = {  "version: 2",
                                "logger:",
                                "  console:",
                                "    level: verbose",
                                "cognite:",
                               $"  project: {_project}",
                               $"  host: {_host}",
                                "  cdf-chunking:",
                                "    events: 2",
                                "  cdf-throttling:",
                                "    events: 2" };
            System.IO.File.WriteAllLines(path, lines);

            var services = new ServiceCollection();
            services.AddConfig<BaseConfig>(path, 2);
            services.AddTestLogging(_output);
            CdfMock.RegisterHttpClient(services);
            services.AddCogniteClient("testApp", setLogger: true, setMetrics: false);
            var index = 0;

            int evtCount = 0;
            int cbCount = 0;

            using (var source = new CancellationTokenSource())
            using (var provider = services.BuildServiceProvider())
            {
                var cogniteDestination = provider.GetRequiredService<CogniteDestination>();
                var logger = provider.GetRequiredService<ILogger<EventTest>>();
                var mock = provider.GetRequiredService<CdfMock>();
                var events = new EventsMock();
                mock.AddMatcher(events.MakeCreateEventsMatcher(Times.Between(7, 13, Moq.Range.Inclusive)));
                // queue with 1 sec upload interval
                await using (var queue = cogniteDestination.CreateEventUploadQueue(TimeSpan.FromSeconds(1), 0, res =>
                {
                    evtCount += res.Uploaded?.Count() ?? 0;
                    cbCount++;
                    return Task.CompletedTask;
                }))
                {
                    var enqueueTask = Task.Run(async () =>
                    {
                        while (index < 13)
                        {
                            queue.Enqueue(new EventCreate
                            {
                                ExternalId = "id " + index,
                                StartTime = DateTime.UtcNow.ToUnixTimeMilliseconds(),
                                EndTime = DateTime.UtcNow.ToUnixTimeMilliseconds()
                            });
                            await Task.Delay(100, source.Token);
                            index++;
                        }
                    });
                    var uploadTask = queue.Start(source.Token);

                    var t = Task.WhenAny(uploadTask, enqueueTask);
                    await t;
                    logger.LogInformation("Enqueueing task completed. Disposing of the upload queue");
                }
                logger.LogInformation("Upload queue disposed");

                Assert.Equal(13, evtCount);
                Assert.True(cbCount <= 3);
                cbCount = 0;
                mock.AssertAndClear();
                mock.AddMatcher(events.MakeCreateEventsMatcher(Times.Exactly(6)));

                // queue with maximum size
                await using (var queue = cogniteDestination.CreateEventUploadQueue(TimeSpan.FromMinutes(10), 5, res =>
                {
                    evtCount += res.Uploaded?.Count() ?? 0;
                    cbCount++;
                    return Task.CompletedTask;
                }))
                {
                    var enqueueTask = Task.Run(async () =>
                    {
                        while (index < 23)
                        {
                            queue.Enqueue(new EventCreate
                            {
                                ExternalId = "id " + index,
                                StartTime = DateTime.UtcNow.ToUnixTimeMilliseconds(),
                                EndTime = DateTime.UtcNow.ToUnixTimeMilliseconds()
                            });
                            await Task.Delay(100, source.Token);
                            index++;
                        }
                    });
                    var uploadTask = queue.Start(source.Token);

                    await enqueueTask;

                    // test cancelling the token;
                    source.Cancel();
                    await uploadTask;
                    Assert.True(uploadTask.IsCompleted);
                    logger.LogInformation("Enqueueing task cancelled. Disposing of the upload queue");
                }
                logger.LogInformation("Upload queue disposed");

                Assert.Equal(23, evtCount);
                Assert.Equal(3, cbCount);
            }

            System.IO.File.Delete(path);
        }

        [Fact]
        public async Task TestUploadQueueBuffer()
        {
            string path = "test-event-queue-buffer-config.yml";
            string[] lines = {  "version: 2",
                                "logger:",
                                "  console:",
                                "    level: verbose",
                                "cognite:",
                               $"  project: {_project}",
                               $"  host: {_host}",
                                "  cdf-chunking:",
                                "    events: 2",
                                "  cdf-throttling:",
                                "    events: 2",
                                "  cdf-retries:",
                                "    max-retries: 0" };
            System.IO.File.WriteAllLines(path, lines);

            var services = new ServiceCollection();
            services.AddConfig<BaseConfig>(path, 2);
            services.AddTestLogging(_output);
            CdfMock.RegisterHttpClient(services);
            services.AddCogniteClient("testApp", setLogger: true, setMetrics: false);

            System.IO.File.Create("event-buffer.bin").Close();

            using (var source = new CancellationTokenSource())
            using (var provider = services.BuildServiceProvider())
            {
                var cogniteDestination = provider.GetRequiredService<CogniteDestination>();
                var logger = provider.GetRequiredService<ILogger<EventTest>>();
                var mock = provider.GetRequiredService<CdfMock>();
                var events = new EventsMock();
                // Just the successful inserts, 5 batches of 2
                mock.AddMatcher(events.MakeCreateEventsMatcher(Times.Exactly(5)));
                mock.AddTokenInspectEndpoint(Times.AtLeastOnce(), _project);
                await using (var queue = cogniteDestination.CreateEventUploadQueue(TimeSpan.Zero, 0, null, "event-buffer.bin"))
                {
                    var _ = queue.Start(source.Token);
                    for (int i = 0; i < 10; i++)
                    {
                        queue.Enqueue(new EventCreate
                        {
                            ExternalId = "id " + i,
                            StartTime = DateTime.UtcNow.ToUnixTimeMilliseconds(),
                            EndTime = DateTime.UtcNow.ToUnixTimeMilliseconds()
                        });
                    }
                    mock.RejectAllMessages = true;
                    Assert.Equal(0, new FileInfo("event-buffer.bin").Length);
                    await queue.Trigger(CancellationToken.None);
                    Assert.True(new FileInfo("event-buffer.bin").Length > 0);
                    Assert.Empty(events.Events);
                    await queue.Trigger(CancellationToken.None);
                    Assert.True(new FileInfo("event-buffer.bin").Length > 0);
                    Assert.Empty(events.Events);
                    mock.RejectAllMessages = false;
                    await queue.Trigger(CancellationToken.None);
                    Assert.Equal(0, new FileInfo("event-buffer.bin").Length);
                    Assert.Equal(10, events.Events.Count);
                    logger.LogInformation("Disposing of the upload queue");
                }
                logger.LogInformation("Upload queue disposed");
            }
            System.IO.File.Delete("event-buffer.bin");
            System.IO.File.Delete(path);
        }
    }
}
