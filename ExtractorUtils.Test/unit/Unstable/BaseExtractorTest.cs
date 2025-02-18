using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extractor.Common;
using Cognite.Extractor.Testing;
using Cognite.Extractor.Utils;
using Cognite.ExtractorUtils.Unstable;
using Cognite.ExtractorUtils.Unstable.Configuration;
using Cognite.ExtractorUtils.Unstable.Tasks;
using CogniteSdk.Alpha;
using ExtractorUtils.Test.unit.Unstable;
using Microsoft.Extensions.DependencyInjection;
using Newtonsoft.Json;
using Xunit;
using Xunit.Abstractions;

namespace ExtractorUtils.Test.Unit.Unstable
{
    class DummyConfig { }

    class DummyExtractor : Cognite.ExtractorUtils.Unstable.BaseExtractor<DummyConfig>
    {
        public Action<ExtractorTaskScheduler> InitAction { get; set; }

        public DummyExtractor(
            ConfigWrapper<DummyConfig> config,
            IServiceProvider provider,
            ExtractorTaskScheduler taskScheduler,
            IIntegrationSink sink,
            CogniteDestination destination = null) : base(config, provider,
            taskScheduler, sink, destination)
        {
        }

        public void AddMonitoredTaskPub(Func<CancellationToken, Task> task, ExtractorTaskResult staticResult, string name)
        {
            AddMonitoredTask(task, staticResult, name);
        }

        public async Task CancelMonitoredTaskAndWaitPub(string name)
        {
            await CancelMonitoredTaskAndWait(name);
        }

        protected override Task InitTasks()
        {
            InitAction?.Invoke(TaskScheduler);
            return Task.CompletedTask;
        }

        protected override ExtractorId GetExtractorVersion()
        {
            return new ExtractorId
            {
                Version = "1.0.0",
                ExternalId = "my-extractor"
            };
        }
    }

    public class BaseExtractorTest
    {
        private readonly ITestOutputHelper _output;
        public BaseExtractorTest(ITestOutputHelper output)
        {
            _output = output;
        }

        private (DummyExtractor, DummySink) CreateExtractor(int? revision = null)
        {
            var sink = new DummySink();
            var services = new ServiceCollection();
            services.AddSingleton(new ConfigWrapper<DummyConfig>(new DummyConfig(), revision));
            services.AddSingleton<IIntegrationSink>(sink);
            services.AddTestLogging(_output);
            services.AddTransient<ExtractorTaskScheduler>();
            services.AddTransient<DummyExtractor>();
            var provider = services.BuildServiceProvider();
            return (provider.GetRequiredService<DummyExtractor>(), sink);
        }

        [Fact]
        public async Task TestBaseExtractor()
        {
            var (ext, sink) = CreateExtractor();
            var taskCompletedEvent = new ManualResetEvent(false);
            // Run the extractor and verify that scheduled tasks are run.
            ext.InitAction = (sched) =>
            {
                sched.AddScheduledTask(new RunQuickTask("task1", async (task, token) =>
                {
                    await Task.Delay(100, token);
                    taskCompletedEvent.Set();
                    return new TaskUpdatePayload();
                }), true);
            };
            var runTask = ext.Start(CancellationToken.None);
            Assert.True(await CommonUtils.WaitAsync(taskCompletedEvent, TimeSpan.FromSeconds(2), CancellationToken.None));
            await ext.DisposeAsync();

            Assert.Single(sink.TaskStart);
            Assert.Single(sink.TaskEnd);
            Assert.Empty(sink.Errors);
        }

        [Fact]
        public async Task TestBaseExtractorInnerError()
        {
            var (ext, sink) = CreateExtractor();
            var taskCompletedEvent = new ManualResetEvent(false);
            // Run the extractor and verify that scheduled tasks are run.
            ext.InitAction = (sched) =>
            {
                sched.AddScheduledTask(new RunQuickTask("task1", async (task, token) =>
                {
                    await Task.Delay(100, token);
                    taskCompletedEvent.Set();
                    throw new Exception("Inner error");
                }), true);
            };
            var runTask = ext.Start(CancellationToken.None);
            Assert.True(await CommonUtils.WaitAsync(taskCompletedEvent, TimeSpan.FromSeconds(2), CancellationToken.None));
            await ext.DisposeAsync();

            Assert.Single(sink.TaskStart);
            Assert.Single(sink.TaskEnd);
            Assert.Equal(2, sink.Errors.Count);
            Assert.Equal("Inner error", sink.Errors[0].Description);
        }

        [Fact]
        public async Task TestBaseExtractorMonitoredError()
        {
            var (ext, sink) = CreateExtractor();
            var runTask = ext.Start(CancellationToken.None);
            ext.AddMonitoredTaskPub(async t =>
            {
                await Task.Delay(100, t);
                throw new Exception("Monitored error");
            }, ExtractorTaskResult.Unexpected, "task1");
            var delayTask = Task.Delay(2000);
            Assert.NotEqual(delayTask, await Task.WhenAny(runTask, delayTask));
            Assert.Equal(2, sink.Errors.Count);
            Assert.Equal("Internal task task1 failed, restarting extractor: Monitored error", sink.Errors[0].Description);
        }

        [Fact]
        public async Task TestBaseExtractorUnexpectedExit()
        {
            var (ext, sink) = CreateExtractor();
            var runTask = ext.Start(CancellationToken.None);
            ext.AddMonitoredTaskPub(async t =>
            {
                await Task.Delay(100, t);
            }, ExtractorTaskResult.Unexpected, "task1");
            var delayTask = Task.Delay(2000);
            Assert.NotEqual(delayTask, await Task.WhenAny(runTask, delayTask));
            Assert.Equal(2, sink.Errors.Count);
            Assert.Equal("Internal task task1 completed, but was not expected to stop, restarting extractor.", sink.Errors[0].Description);
        }

        [Fact]
        public async Task TestCancelMonitoredTask()
        {
            var (ext, sink) = CreateExtractor();
            var runTask = ext.Start(CancellationToken.None);
            ext.AddMonitoredTaskPub(async t =>
            {
                while (!t.IsCancellationRequested)
                {
                    await Task.Delay(100, t);
                }
            }, ExtractorTaskResult.Unexpected, "task1");
            var delayTask = Task.Delay(2000);
            Assert.NotEqual(delayTask, await Task.WhenAny(ext.CancelMonitoredTaskAndWaitPub("task1"), delayTask));
            Assert.NotEqual(delayTask, await Task.WhenAny(ext.Shutdown(), delayTask));
            Assert.NotEqual(delayTask, await Task.WhenAny(runTask, delayTask));

            // Dispose should work, even if we're already shut-down.
            await ext.DisposeAsync();
        }
    }
}