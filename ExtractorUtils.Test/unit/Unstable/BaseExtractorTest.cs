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

        private static IntegrationAction MakeAction(string externalId, string actionName, ActionStatus status = ActionStatus.pending)
        {
            return new IntegrationAction
            {
                ExternalId = externalId,
                ActionName = actionName,
                Status = status,
                CreatedTime = 0,
                LastUpdatedTime = 0,
            };
        }

        private async Task<(DummyExtractor, DummySink)> StartExtractorWithActionableTask(
            Action<RunQuickTask> configureTask = null)
        {
            var (ext, sink) = CreateExtractor();
            var task = new RunQuickTask("MyTask", (_, tok) => Task.FromResult<TaskUpdatePayload>(null));
            task.Metadata.Action = true;
            configureTask?.Invoke(task);

            ext.InitAction = (sched) => sched.AddScheduledTask(task, false);
            _ = ext.Start(CancellationToken.None);
            await TestUtils.WaitForCondition(() => sink.ActionDispatcher != null, 5);
            return (ext, sink);
        }

        [Fact]
        public async Task TestDispatchStartActionSucceeds()
        {
            var (ext, sink) = await StartExtractorWithActionableTask();

            await sink.ActionDispatcher(new List<IntegrationAction> { MakeAction("action-1", "Start MyTask") });

            await TestUtils.WaitForCondition(
                () => sink.ActionUpdates.Any(u => u.ExternalId == "action-1" && u.Status == ActionStatus.succeeded), 5);

            var updates = sink.ActionUpdates.Where(u => u.ExternalId == "action-1").ToList();
            Assert.Equal(ActionStatus.running, updates[0].Status);
            Assert.Equal(ActionStatus.succeeded, updates[1].Status);

            await ext.DisposeAsync();
        }

        [Fact]
        public async Task TestDispatchStartActionAlreadyRunningFails()
        {
            // A blocking task keeps the first Start in flight, so the second one reliably hits
            // "already running" instead of racing the first one's near-instant completion.
            using var blockEvt = new ManualResetEvent(false);
            var (ext, sink) = await StartExtractorWithActionableTaskBlocking(blockEvt);

            await sink.ActionDispatcher(new List<IntegrationAction> { MakeAction("action-1", "Start MyTask") });
            await TestUtils.WaitForCondition(
                () => sink.ActionUpdates.Any(u => u.ExternalId == "action-1" && u.Status == ActionStatus.running), 5);

            await sink.ActionDispatcher(new List<IntegrationAction> { MakeAction("action-2", "Start MyTask") });
            await TestUtils.WaitForCondition(
                () => sink.ActionUpdates.Any(u => u.ExternalId == "action-2" && u.Status == ActionStatus.failed), 5);

            var failure = sink.ActionUpdates.Single(u => u.ExternalId == "action-2");
            Assert.Equal(ActionStatus.failed, failure.Status);
            Assert.Contains("already running", failure.ResultMessage);

            blockEvt.Set();
            await ext.DisposeAsync();
        }

        private async Task<(DummyExtractor, DummySink)> StartExtractorWithActionableTaskBlocking(
            ManualResetEvent blockEvt, ManualResetEvent taskStartedEvt = null)
        {
            var (ext, sink) = CreateExtractor();
            var task = new RunQuickTask("MyTask", async (_, tok) =>
            {
                taskStartedEvt?.Set();
                await CommonUtils.WaitAsync(blockEvt, Timeout.InfiniteTimeSpan, tok);
                return null;
            });
            task.Metadata.Action = true;

            ext.InitAction = (sched) => sched.AddScheduledTask(task, false);
            _ = ext.Start(CancellationToken.None);
            await TestUtils.WaitForCondition(() => sink.ActionDispatcher != null, 5);
            return (ext, sink);
        }

        [Fact]
        public async Task TestDispatchStartActionNotRunnableFailsFast()
        {
            var (ext, sink) = await StartExtractorWithActionableTask(task => task.CanRun = () => false);

            await sink.ActionDispatcher(new List<IntegrationAction> { MakeAction("action-1", "Start MyTask") });

            await TestUtils.WaitForCondition(
                () => sink.ActionUpdates.Any(u => u.ExternalId == "action-1"), 5);

            // Only one update: CanRunNow() being false must be caught before `running` is ever reported.
            var updates = sink.ActionUpdates.Where(u => u.ExternalId == "action-1").ToList();
            Assert.Single(updates);
            Assert.Equal(ActionStatus.failed, updates[0].Status);
            Assert.Contains("not currently able to run", updates[0].ResultMessage);

            await ext.DisposeAsync();
        }

        [Fact]
        public async Task TestDispatchStopActionSucceeds()
        {
            using var blockEvt = new ManualResetEvent(false);
            var (ext, sink) = await StartExtractorWithActionableTaskBlocking(blockEvt);

            // Start it first via the scheduler directly (not through an action), then Stop it.
            await sink.ActionDispatcher(new List<IntegrationAction> { MakeAction("start-1", "Start MyTask") });
            await TestUtils.WaitForCondition(
                () => sink.ActionUpdates.Any(u => u.ExternalId == "start-1" && u.Status == ActionStatus.running), 5);

            await sink.ActionDispatcher(new List<IntegrationAction> { MakeAction("stop-1", "Stop MyTask") });

            await TestUtils.WaitForCondition(
                () => sink.ActionUpdates.Any(u => u.ExternalId == "stop-1"), 5);
            var stopUpdate = sink.ActionUpdates.Single(u => u.ExternalId == "stop-1");
            Assert.Equal(ActionStatus.succeeded, stopUpdate.Status);

            // The original Start action's own dispatch observes the task being cancelled.
            await TestUtils.WaitForCondition(
                () => sink.ActionUpdates.Any(u => u.ExternalId == "start-1" && u.Status == ActionStatus.canceled), 5);

            await ext.DisposeAsync();
        }

        [Fact]
        public async Task TestCancelPendingForStopActionReportsCanceled()
        {
            // A Stop cancelled before ever being dispatched must still get a terminal status, or
            // the server redelivers it forever.
            var (ext, sink) = await StartExtractorWithActionableTask();

            await sink.ActionDispatcher(new List<IntegrationAction>
            {
                MakeAction("stop-1", "Stop MyTask", ActionStatus.cancel_pending)
            });

            await TestUtils.WaitForCondition(
                () => sink.ActionUpdates.Any(u => u.ExternalId == "stop-1" && u.Status == ActionStatus.canceled), 5);

            await ext.DisposeAsync();
        }

        [Fact]
        public async Task TestDispatchStopActionNotRunningFails()
        {
            var (ext, sink) = await StartExtractorWithActionableTask();

            await sink.ActionDispatcher(new List<IntegrationAction> { MakeAction("action-1", "Stop MyTask") });

            await TestUtils.WaitForCondition(
                () => sink.ActionUpdates.Any(u => u.ExternalId == "action-1"), 5);
            var update = sink.ActionUpdates.Single(u => u.ExternalId == "action-1");
            Assert.Equal(ActionStatus.failed, update.Status);
            Assert.Contains("is not currently running", update.ResultMessage);

            await ext.DisposeAsync();
        }

        [Fact]
        public async Task TestDispatchStartActionForUnregisteredTaskFails()
        {
            var (ext, sink) = await StartExtractorWithActionableTask();

            await sink.ActionDispatcher(new List<IntegrationAction> { MakeAction("action-1", "Start NoSuchTask") });

            await TestUtils.WaitForCondition(
                () => sink.ActionUpdates.Any(u => u.ExternalId == "action-1"), 5);
            var update = sink.ActionUpdates.Single(u => u.ExternalId == "action-1");
            Assert.Equal(ActionStatus.failed, update.Status);
            Assert.Equal("No action named 'Start NoSuchTask' registered", update.ResultMessage);

            await ext.DisposeAsync();
        }

        [Fact]
        public async Task TestDispatchStopActionForUnregisteredTaskFails()
        {
            var (ext, sink) = await StartExtractorWithActionableTask();

            await sink.ActionDispatcher(new List<IntegrationAction> { MakeAction("action-1", "Stop NoSuchTask") });

            await TestUtils.WaitForCondition(
                () => sink.ActionUpdates.Any(u => u.ExternalId == "action-1"), 5);
            var update = sink.ActionUpdates.Single(u => u.ExternalId == "action-1");
            Assert.Equal(ActionStatus.failed, update.Status);
            Assert.Equal("No action named 'Stop NoSuchTask' registered", update.ResultMessage);

            await ext.DisposeAsync();
        }

        [Fact]
        public async Task TestDispatchUnknownActionNameFails()
        {
            var (ext, sink) = await StartExtractorWithActionableTask();

            await sink.ActionDispatcher(new List<IntegrationAction> { MakeAction("action-1", "Something Unregistered") });

            await TestUtils.WaitForCondition(
                () => sink.ActionUpdates.Any(u => u.ExternalId == "action-1"), 5);
            var update = sink.ActionUpdates.Single(u => u.ExternalId == "action-1");
            Assert.Equal(ActionStatus.failed, update.Status);
            Assert.Contains("No action named", update.ResultMessage);

            await ext.DisposeAsync();
        }

        [Fact]
        public async Task TestCancelPendingForStartActionCancelsUnderlyingTaskAndReportsCanceled()
        {
            using var blockEvt = new ManualResetEvent(false);
            using var taskStartedEvt = new ManualResetEvent(false);
            var (ext, sink) = await StartExtractorWithActionableTaskBlocking(blockEvt, taskStartedEvt);

            await sink.ActionDispatcher(new List<IntegrationAction> { MakeAction("action-1", "Start MyTask") });
            await TestUtils.WaitForCondition(
                () => sink.ActionUpdates.Any(u => u.ExternalId == "action-1" && u.Status == ActionStatus.running), 5);

            // `running` only means the task was queued, not that ActiveTask is set yet --
            // TryCancelTask is a no-op until then. Wait for the task to actually start so this
            // test doesn't race a window that's microseconds-wide in production (against a ~30s
            // checkin interval) but immediate here.
            taskStartedEvt.WaitOne();

            // Simulate odin redelivering the same action with cancel_pending, e.g. because
            // someone cancelled it via the standalone /actions/cancel API.
            await sink.ActionDispatcher(new List<IntegrationAction>
            {
                MakeAction("action-1", "Start MyTask", ActionStatus.cancel_pending)
            });

            await TestUtils.WaitForCondition(
                () => sink.ActionUpdates.Any(u => u.ExternalId == "action-1" && u.Status == ActionStatus.canceled), 5);

            await ext.DisposeAsync();
        }

        [Fact]
        public async Task TestCancelPendingForUnknownActionIsSafeNoOp()
        {
            // A cancel_pending redelivery for an externalId this process never dispatched (e.g.
            // it already completed, or the extractor restarted) must not throw or queue anything.
            var (ext, sink) = await StartExtractorWithActionableTask();

            await sink.ActionDispatcher(new List<IntegrationAction>
            {
                MakeAction("never-dispatched", "Start MyTask", ActionStatus.cancel_pending)
            });

            // Give it a moment to (not) do anything, then confirm nothing was queued.
            await Task.Delay(100);
            Assert.DoesNotContain(sink.ActionUpdates, u => u.ExternalId == "never-dispatched");

            await ext.DisposeAsync();
        }

        [Fact]
        public async Task TestCancelPendingForStartActionWithUnregisteredTaskDoesNotAbortBatch()
        {
            // No task named "NoSuchTask" exists, so this falls through to the generic
            // cancel_pending path (reports canceled) instead of hitting TaskScheduler -- and
            // must not stop the second action in the batch from being processed too.
            var (ext, sink) = await StartExtractorWithActionableTask();

            await sink.ActionDispatcher(new List<IntegrationAction>
            {
                MakeAction("stale-cancel", "Start NoSuchTask", ActionStatus.cancel_pending),
                MakeAction("action-1", "Start MyTask"),
            });

            await TestUtils.WaitForCondition(
                () => sink.ActionUpdates.Any(u => u.ExternalId == "action-1" && u.Status == ActionStatus.succeeded), 5);
            await TestUtils.WaitForCondition(
                () => sink.ActionUpdates.Any(u => u.ExternalId == "stale-cancel" && u.Status == ActionStatus.canceled), 5);

            await ext.DisposeAsync();
        }

        private async Task<(DummyExtractor, DummySink)> StartExtractorWithCustomAction(CustomAction<DummyConfig> action)
        {
            var (ext, sink) = CreateExtractor();
            ext.InitActionsAction = e => e.RegisterActionPub(action);
            _ = ext.Start(CancellationToken.None);
            await TestUtils.WaitForCondition(() => sink.ActionDispatcher != null, 5);
            return (ext, sink);
        }

        [Fact]
        public async Task TestDispatchCustomActionSucceeds()
        {
            var (ext, sink) = await StartExtractorWithCustomAction(new CustomAction<DummyConfig>(
                "my_action",
                async (ctx, tok) =>
                {
                    await Task.Delay(10, tok);
                    ctx.SetResult("Done", new Dictionary<string, string> { ["fileCount"] = "3" });
                }));

            await sink.ActionDispatcher(new List<IntegrationAction> { MakeAction("action-1", "my_action") });

            await TestUtils.WaitForCondition(
                () => sink.ActionUpdates.Any(u => u.ExternalId == "action-1" && u.Status == ActionStatus.succeeded), 5);

            var updates = sink.ActionUpdates.Where(u => u.ExternalId == "action-1").ToList();
            Assert.Equal(ActionStatus.running, updates[0].Status);
            Assert.Equal(ActionStatus.succeeded, updates[1].Status);
            Assert.Equal("Done", updates[1].ResultMessage);
            Assert.Equal("3", updates[1].ResultMetadata["fileCount"]);

            await ext.DisposeAsync();
        }

        [Fact]
        public async Task TestDispatchCustomActionSucceedsWithoutCallingSetResult()
        {
            // An author who never calls SetResult still produces a valid terminal `succeeded`
            // update, not an action stuck pending forever.
            var (ext, sink) = await StartExtractorWithCustomAction(new CustomAction<DummyConfig>(
                "my_action", (ctx, tok) => Task.CompletedTask));

            await sink.ActionDispatcher(new List<IntegrationAction> { MakeAction("action-1", "my_action") });

            await TestUtils.WaitForCondition(
                () => sink.ActionUpdates.Any(u => u.ExternalId == "action-1" && u.Status == ActionStatus.succeeded), 5);

            await ext.DisposeAsync();
        }

        [Fact]
        public async Task TestDispatchCustomActionWithActionErrorReportsStructuredFailure()
        {
            var (ext, sink) = await StartExtractorWithCustomAction(new CustomAction<DummyConfig>(
                "my_action",
                (ctx, tok) => throw new ActionError("invalid_parameter", "start_date is invalid", "expected ISO 8601")));

            await sink.ActionDispatcher(new List<IntegrationAction> { MakeAction("action-1", "my_action") });

            await TestUtils.WaitForCondition(
                () => sink.ActionUpdates.Any(u => u.ExternalId == "action-1" && u.Status == ActionStatus.failed), 5);

            var update = sink.ActionUpdates.Single(u => u.ExternalId == "action-1" && u.Status == ActionStatus.failed);
            Assert.Equal("start_date is invalid", update.ResultMessage);
            Assert.Equal("invalid_parameter", update.ResultMetadata["errorType"]);
            Assert.Equal("expected ISO 8601", update.ResultMetadata["errorDetail"]);

            await ext.DisposeAsync();
        }

        [Fact]
        public async Task TestDispatchCustomActionWithGenericExceptionReportsFailure()
        {
            var (ext, sink) = await StartExtractorWithCustomAction(new CustomAction<DummyConfig>(
                "my_action",
                (ctx, tok) => throw new InvalidOperationException("boom")));

            await sink.ActionDispatcher(new List<IntegrationAction> { MakeAction("action-1", "my_action") });

            await TestUtils.WaitForCondition(
                () => sink.ActionUpdates.Any(u => u.ExternalId == "action-1" && u.Status == ActionStatus.failed), 5);

            var update = sink.ActionUpdates.Single(u => u.ExternalId == "action-1" && u.Status == ActionStatus.failed);
            Assert.Equal("boom", update.ResultMessage);

            await ext.DisposeAsync();
        }

        [Fact]
        public async Task TestDispatchCustomActionReceivesContextFields()
        {
            ActionContext<DummyConfig> captured = null;
            var (ext, sink) = await StartExtractorWithCustomAction(new CustomAction<DummyConfig>(
                "my_action",
                (ctx, tok) =>
                {
                    captured = ctx;
                    return Task.CompletedTask;
                }));

            var callMetadata = new Dictionary<string, string> { ["startDate"] = "2026-01-01" };
            var action = MakeAction("action-1", "my_action");
            action.CallMetadata = callMetadata;

            await sink.ActionDispatcher(new List<IntegrationAction> { action });

            await TestUtils.WaitForCondition(() => captured != null, 5);
            Assert.Equal("action-1", captured.ExternalId);
            Assert.Equal("2026-01-01", captured.CallMetadata["startDate"]);
            Assert.NotNull(captured.ApplicationConfig);

            await ext.DisposeAsync();
        }

        [Fact]
        public async Task TestDispatchCustomActionCancelInFlightReportsCanceled()
        {
            using var startedEvt = new ManualResetEvent(false);
            var (ext, sink) = await StartExtractorWithCustomAction(new CustomAction<DummyConfig>(
                "my_action",
                async (ctx, tok) =>
                {
                    startedEvt.Set();
                    await Task.Delay(Timeout.Infinite, tok);
                }));

            await sink.ActionDispatcher(new List<IntegrationAction> { MakeAction("action-1", "my_action") });
            startedEvt.WaitOne();

            // Simulate odin redelivering the same action with cancel_pending, e.g. because
            // someone cancelled it via the standalone /actions/cancel API.
            await sink.ActionDispatcher(new List<IntegrationAction>
            {
                MakeAction("action-1", "my_action", ActionStatus.cancel_pending)
            });

            await TestUtils.WaitForCondition(
                () => sink.ActionUpdates.Any(u => u.ExternalId == "action-1" && u.Status == ActionStatus.canceled), 5);

            await ext.DisposeAsync();
        }

        [Fact]
        public async Task TestDispatchCustomActionCancelInFlightWithThrowingCallbackDoesNotAbortBatch()
        {
            // A throwing cancellation callback shouldn't abort the whole batch -- check this
            // action still resolves to `canceled`, and a second, unrelated action still runs.
            using var startedEvt = new ManualResetEvent(false);
            var (ext, sink) = await StartExtractorWithCustomAction(new CustomAction<DummyConfig>(
                "my_action",
                async (ctx, tok) =>
                {
                    tok.Register(() => throw new InvalidOperationException("callback blew up"));
                    startedEvt.Set();
                    await Task.Delay(Timeout.Infinite, tok);
                }));

            await sink.ActionDispatcher(new List<IntegrationAction> { MakeAction("action-1", "my_action") });
            startedEvt.WaitOne();

            await sink.ActionDispatcher(new List<IntegrationAction>
            {
                MakeAction("action-1", "my_action", ActionStatus.cancel_pending),
                MakeAction("action-2", "Something Unregistered"),
            });

            await TestUtils.WaitForCondition(
                () => sink.ActionUpdates.Any(u => u.ExternalId == "action-1" && u.Status == ActionStatus.canceled), 5);
            await TestUtils.WaitForCondition(
                () => sink.ActionUpdates.Any(u => u.ExternalId == "action-2" && u.Status == ActionStatus.failed), 5);

            await ext.DisposeAsync();
        }

        [Fact]
        public async Task TestDispatchCustomActionAfterShutdownIsIgnored()
        {
            // ShutdownInternal's cancel loop only runs once, at the top -- an action dispatched
            // after that must be stopped from starting at all, by _shuttingDown.
            var callCount = 0;
            var (ext, sink) = await StartExtractorWithCustomAction(new CustomAction<DummyConfig>(
                "my_action", (ctx, tok) =>
                {
                    Interlocked.Increment(ref callCount);
                    return Task.CompletedTask;
                }));

            await ext.DisposeAsync();

            await sink.ActionDispatcher(new List<IntegrationAction> { MakeAction("action-1", "my_action") });
            await Task.Delay(100);

            Assert.Equal(0, Volatile.Read(ref callCount));
            Assert.DoesNotContain(sink.ActionUpdates, u => u.ExternalId == "action-1");
        }

        [Fact]
        public async Task TestShutdownCancelsInFlightCustomActions()
        {
            using var startedEvt = new ManualResetEvent(false);
            var (ext, sink) = await StartExtractorWithCustomAction(new CustomAction<DummyConfig>(
                "my_action",
                async (ctx, tok) =>
                {
                    startedEvt.Set();
                    await Task.Delay(Timeout.Infinite, tok);
                }));

            await sink.ActionDispatcher(new List<IntegrationAction> { MakeAction("action-1", "my_action") });
            startedEvt.WaitOne();

            // Shut down while the action is still running -- its token must be cancelled, not
            // left running past the extractor's own lifetime.
            await ext.DisposeAsync();

            await TestUtils.WaitForCondition(
                () => sink.ActionUpdates.Any(u => u.ExternalId == "action-1" && u.Status == ActionStatus.canceled), 5);
        }

        [Fact]
        public async Task TestShutdownWithThrowingCancellationCallbackStillCancelsOtherActions()
        {
            // One action can't prove this fix works -- DisposeAsync hides the exception either
            // way. Both throw here so that whichever gets enumerated first still breaks the test
            // if unfixed (dictionary order isn't guaranteed).
            using var startedA = new ManualResetEvent(false);
            using var startedB = new ManualResetEvent(false);
            var (ext, sink) = CreateExtractor();
            ext.InitActionsAction = e =>
            {
                e.RegisterActionPub(new CustomAction<DummyConfig>("action_a", async (ctx, tok) =>
                {
                    tok.Register(() => throw new InvalidOperationException("callback blew up"));
                    startedA.Set();
                    await Task.Delay(Timeout.Infinite, tok);
                }));
                e.RegisterActionPub(new CustomAction<DummyConfig>("action_b", async (ctx, tok) =>
                {
                    tok.Register(() => throw new InvalidOperationException("callback blew up"));
                    startedB.Set();
                    await Task.Delay(Timeout.Infinite, tok);
                }));
            };
            _ = ext.Start(CancellationToken.None);
            await TestUtils.WaitForCondition(() => sink.ActionDispatcher != null, 5);

            await sink.ActionDispatcher(new List<IntegrationAction>
            {
                MakeAction("a", "action_a"),
                MakeAction("b", "action_b"),
            });
            startedA.WaitOne();
            startedB.WaitOne();

            await ext.DisposeAsync();

            await TestUtils.WaitForCondition(
                () => sink.ActionUpdates.Any(u => u.ExternalId == "a" && u.Status == ActionStatus.canceled), 5);
            await TestUtils.WaitForCondition(
                () => sink.ActionUpdates.Any(u => u.ExternalId == "b" && u.Status == ActionStatus.canceled), 5);
        }

        [Fact]
        public async Task TestDispatchCustomActionNotInFlightCancelPendingReportsCanceled()
        {
            // Never dispatched by this process (e.g. cancelled before we ever saw it, or after a
            // restart) -- must still get a terminal status, or the server redelivers it forever.
            var (ext, sink) = await StartExtractorWithCustomAction(new CustomAction<DummyConfig>(
                "my_action", (ctx, tok) => Task.CompletedTask));

            await sink.ActionDispatcher(new List<IntegrationAction>
            {
                MakeAction("never-dispatched", "my_action", ActionStatus.cancel_pending)
            });

            await TestUtils.WaitForCondition(
                () => sink.ActionUpdates.Any(u => u.ExternalId == "never-dispatched" && u.Status == ActionStatus.canceled), 5);

            await ext.DisposeAsync();
        }

        [Fact]
        public async Task TestDispatchCustomActionRedeliveredBeforeCompletionIsNotInvokedTwice()
        {
            var invocationCount = 0;
            using var startedEvt = new ManualResetEvent(false);
            using var releaseEvt = new ManualResetEvent(false);
            var (ext, sink) = await StartExtractorWithCustomAction(new CustomAction<DummyConfig>(
                "my_action",
                async (ctx, tok) =>
                {
                    Interlocked.Increment(ref invocationCount);
                    startedEvt.Set();
                    await CommonUtils.WaitAsync(releaseEvt, Timeout.InfiniteTimeSpan, tok);
                }));

            var action = MakeAction("action-1", "my_action");
            await sink.ActionDispatcher(new List<IntegrationAction> { action });
            startedEvt.WaitOne();

            // Redelivered before the first dispatch finished -- e.g. it's still showing up in
            // pendingActions since odin hasn't seen a terminal update for it yet.
            await sink.ActionDispatcher(new List<IntegrationAction> { action });
            await sink.ActionDispatcher(new List<IntegrationAction> { action });

            // Not cancel_pending, so the token is untouched -- releasing the block just lets the
            // target finish normally.
            releaseEvt.Set();
            await TestUtils.WaitForCondition(
                () => sink.ActionUpdates.Any(u => u.ExternalId == "action-1" && u.Status == ActionStatus.succeeded), 5);

            Assert.Equal(1, invocationCount);

            await ext.DisposeAsync();
        }

        [Fact]
        public async Task TestDispatchCustomActionCollidingWithUnknownNameStillFails()
        {
            // An unknown action name must still fail -- registering some other custom action
            // shouldn't make unrelated names resolve.
            var (ext, sink) = await StartExtractorWithCustomAction(new CustomAction<DummyConfig>(
                "my_action", (ctx, tok) => Task.CompletedTask));

            await sink.ActionDispatcher(new List<IntegrationAction> { MakeAction("action-1", "some_other_action") });

            await TestUtils.WaitForCondition(
                () => sink.ActionUpdates.Any(u => u.ExternalId == "action-1"), 5);
            var update = sink.ActionUpdates.Single(u => u.ExternalId == "action-1");
            Assert.Equal(ActionStatus.failed, update.Status);
            Assert.Contains("No action named", update.ResultMessage);

            await ext.DisposeAsync();
        }
    }
}
