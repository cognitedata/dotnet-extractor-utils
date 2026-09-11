using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extractor.Common;
using Cognite.Extractor.Configuration;
using Cognite.Extractor.Testing;
using Cognite.Extractor.Utils;
using Cognite.Extractor.Utils.Unstable;
using Cognite.Extractor.Utils.Unstable.Configuration;
using Cognite.Extractor.Utils.Unstable.Tasks;
using CogniteSdk.Alpha;
using ExtractorUtils.Test.unit.Unstable;
using Microsoft.Extensions.DependencyInjection;
using Xunit;
using Xunit.Abstractions;

namespace ExtractorUtils.Test.Unit.Unstable
{
    class DummyConfig : VersionedConfig
    {
        public string Foo { get; set; }
        public override void GenerateDefaults()
        {
        }
    }

    class DummyExtractor : Cognite.Extractor.Utils.Unstable.BaseExtractor<DummyConfig>
    {
        public Action<ExtractorTaskScheduler> InitAction { get; set; }
        public Action<DummyExtractor> InitActionsAction { get; set; }
        public IIntegrationSink Sink { get; }

        public DummyExtractor(
            ConfigWrapper<DummyConfig> config,
            IServiceProvider provider,
            ExtractorTaskScheduler taskScheduler,
            IIntegrationSink sink,
            CogniteDestination destination = null) : base(config, provider,
            taskScheduler, sink, destination)
        {
            Sink = sink;
        }

        public void AddMonitoredTaskPub(Func<CancellationToken, Task> task, SchedulerTaskResult staticResult, string name)
        {
            AddMonitoredTask(task, staticResult, name);
        }

        public async Task CancelMonitoredTaskAndWaitPub(string name)
        {
            await CancelMonitoredTaskAndWait(name);
        }

        public void RegisterActionPub(CustomAction<DummyConfig> action)
        {
            RegisterAction(action);
        }

        protected override Task InitTasks()
        {
            InitAction?.Invoke(TaskScheduler);
            return Task.CompletedTask;
        }

        protected override Task InitActions()
        {
            InitActionsAction?.Invoke(this);
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
            Assert.Single(sink.StartupRequests);

            var req = sink.StartupRequests[0];
            Assert.Single(req.Tasks);
            Assert.Equal("task1", req.Tasks.ElementAt(0).Name);
            Assert.Equal("My task", req.Tasks.ElementAt(0).Description);
            Assert.Equal(TaskType.batch, req.Tasks.ElementAt(0).Type);
            Assert.Equal("my-extractor", req.Extractor.ExternalId);
            Assert.Equal("1.0.0", req.Extractor.Version);
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
            }, SchedulerTaskResult.Unexpected, "task1");
            var delayTask = Task.Delay(2000);
            // We should get the monitored error, not a timeout.
            Assert.Equal(runTask, await Task.WhenAny(runTask, delayTask));
            Assert.Equal(2, sink.Errors.Count);
            Assert.Equal("Task task1 failed: Monitored error", sink.Errors[0].Description);
        }

        [Fact]
        public async Task TestBaseExtractorUnexpectedExit()
        {
            var (ext, sink) = CreateExtractor();
            var runTask = ext.Start(CancellationToken.None);
            ext.AddMonitoredTaskPub(async t =>
            {
                await Task.Delay(100, t);
            }, SchedulerTaskResult.Unexpected, "task1");
            var delayTask = Task.Delay(2000);
            Assert.NotEqual(delayTask, await Task.WhenAny(runTask, delayTask));
            Assert.Equal(2, sink.Errors.Count);
            Assert.Equal("Task task1 completed, but was not expected to stop.", sink.Errors[0].Description);
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
            }, SchedulerTaskResult.Unexpected, "task1");
            var delayTask = Task.Delay(2000);
            Assert.NotEqual(delayTask, await Task.WhenAny(ext.CancelMonitoredTaskAndWaitPub("task1"), delayTask));
            Assert.NotEqual(delayTask, await Task.WhenAny(ext.Shutdown(), delayTask));
            Assert.NotEqual(delayTask, await Task.WhenAny(runTask, delayTask));

            // Dispose should work, even if we're already shut-down.
            await ext.DisposeAsync();
        }

        [Fact]
        public async Task TestInitActionsRunsAfterInitTasks()
        {
            var (ext, sink) = CreateExtractor();
            var order = new List<string>();

            ext.InitAction = (sched) => order.Add("InitTasks");
            ext.InitActionsAction = (e) => order.Add("InitActions");

            await ext.Init(CancellationToken.None);

            Assert.Equal(new[] { "InitTasks", "InitActions" }, order);
        }

        [Fact]
        public async Task TestRegisterActionDuplicateNameThrows()
        {
            var (ext, sink) = CreateExtractor();
            ext.InitActionsAction = (e) =>
            {
                e.RegisterActionPub(new CustomAction<DummyConfig>("my_action", (ctx, tok) => Task.CompletedTask));
                Assert.Throws<InvalidOperationException>(() =>
                    e.RegisterActionPub(new CustomAction<DummyConfig>("my_action", (ctx, tok) => Task.CompletedTask)));
            };

            await ext.Init(CancellationToken.None);
        }

        [Fact]
        public async Task TestRegisterActionCollidingWithAutoGeneratedNameThrows()
        {
            var (ext, sink) = CreateExtractor();
            ext.InitAction = (sched) =>
            {
                var task = new RunQuickTask("MyTask", (_, tok) => Task.FromResult<TaskUpdatePayload>(null));
                task.Metadata.Action = true;
                sched.AddScheduledTask(task, false);
            };
            ext.InitActionsAction = (e) =>
            {
                Assert.Throws<InvalidOperationException>(() =>
                    e.RegisterActionPub(new CustomAction<DummyConfig>("Start MyTask", (ctx, tok) => Task.CompletedTask)));
                Assert.Throws<InvalidOperationException>(() =>
                    e.RegisterActionPub(new CustomAction<DummyConfig>("Stop MyTask", (ctx, tok) => Task.CompletedTask)));
            };

            await ext.Init(CancellationToken.None);
        }

        [Fact]
        public async Task TestStartupRequestAdvertisesAutoGeneratedAndCustomActions()
        {
            var (ext, sink) = CreateExtractor();
            var taskCompletedEvent = new ManualResetEvent(false);

            ext.InitAction = (sched) =>
            {
                var actionableTask = new RunQuickTask("ActionableTask", async (_, tok) =>
                {
                    await Task.Delay(10, tok);
                    taskCompletedEvent.Set();
                    return null;
                });
                actionableTask.Metadata.Action = true;
                sched.AddScheduledTask(actionableTask, true);

                // A non-actionable task must not get auto-generated Start/Stop actions.
                var plainTask = new RunQuickTask("PlainTask", (_, tok) => Task.FromResult<TaskUpdatePayload>(null));
                sched.AddScheduledTask(plainTask, false);
            };
            ext.InitActionsAction = (e) =>
            {
                e.RegisterActionPub(new CustomAction<DummyConfig>("fetch_logs", (ctx, tok) => Task.CompletedTask, "Fetch logs"));
            };

            var runTask = ext.Start(CancellationToken.None);
            Assert.True(await CommonUtils.WaitAsync(taskCompletedEvent, TimeSpan.FromSeconds(2), CancellationToken.None));
            await ext.DisposeAsync();

            Assert.Single(sink.StartupRequests);
            var actions = sink.StartupRequests[0].AvailableActions.ToList();

            Assert.Equal(3, actions.Count);
            Assert.Equal("Start ActionableTask", actions[0].Name);
            Assert.Equal(ActionType.start_task, actions[0].Type);
            Assert.Equal("ActionableTask", actions[0].Task);
            Assert.Equal("Stop ActionableTask", actions[1].Name);
            Assert.Equal(ActionType.stop_task, actions[1].Type);
            Assert.Equal("ActionableTask", actions[1].Task);
            Assert.Equal("fetch_logs", actions[2].Name);
            Assert.Equal(ActionType.custom, actions[2].Type);
            Assert.Equal("Fetch logs", actions[2].Description);

            Assert.DoesNotContain(actions, a => a.Task == "PlainTask");
        }
    }
}
