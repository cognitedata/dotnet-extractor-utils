using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extractor.Common;
using Cognite.Extractor.Testing;
using Cognite.Extractor.Utils.Unstable.Tasks;
using CogniteSdk.Alpha;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;
using Xunit.Abstractions;

namespace ExtractorUtils.Test.unit.Unstable
{
    class DummySink : BaseErrorReporter, IIntegrationSink
    {
        public List<ExtractorError> Errors { get; } = new();
        public List<(string, DateTime)> TaskStart { get; } = new();
        public List<(string, DateTime)> TaskEnd { get; } = new();

        public List<StartupRequest> StartupRequests { get; } = new();
        public List<ActionUpdate> ActionUpdates { get; } = new();
        public Func<IReadOnlyList<IntegrationAction>, Task> ActionDispatcher { get; private set; }

        public Task Flush(CancellationToken token)
        {
            return Task.CompletedTask;
        }

#nullable enable
        public override ExtractorError NewError(ErrorLevel level, string description, string? details = null, DateTime? now = null, string? type = null, int? configRevision = null)
        {
            return new ExtractorError(level, description, this, details, null, now, type, configRevision);
        }
#nullable restore

        public void ReportError(ExtractorError error)
        {
            Errors.Add(error);
        }

        public void ReportTaskEnd(string taskName, TaskUpdatePayload update = null, DateTime? timestamp = null)
        {
            TaskEnd.Add((taskName, timestamp ?? DateTime.UtcNow));
        }

        public void ReportTaskStart(string taskName, TaskUpdatePayload update = null, DateTime? timestamp = null)
        {
            TaskStart.Add((taskName, timestamp ?? DateTime.UtcNow));
        }

        public async Task RunPeriodicCheckIn(CancellationToken token, StartupRequest startupPayload, TimeSpan? interval = null)
        {
            StartupRequests.Add(startupPayload);
            while (!token.IsCancellationRequested) await Task.Delay(100000, token);
        }

        public void QueueActionUpdate(ActionUpdate update)
        {
            ActionUpdates.Add(update);
        }

        public void SetActionDispatcher(Func<IReadOnlyList<IntegrationAction>, Task> dispatcher)
        {
            ActionDispatcher = dispatcher;
        }
    }

    class RunQuickTask : BaseSchedulableTask
    {
        bool _errorIsFatal;
        public override bool ErrorIsFatal => _errorIsFatal;

        bool? _cancellationIsFatal;
        public override bool CancellationIsFatal => _cancellationIsFatal ?? base.CancellationIsFatal;

        private TimeSpan _schedule = TimeSpan.Zero;
        public bool SetErrorFatal { set => _errorIsFatal = value; }
        public bool SetCancellationFatal { set => _cancellationIsFatal = value; }
        public Func<bool> CanRun { get; set; } = () => true;

        public override string Name { get; }

        private Func<BaseErrorReporter, CancellationToken, Task<TaskUpdatePayload>> _func;

        public override TaskMetadata Metadata { get; } = new TaskMetadata(TaskType.batch)
        {
            Description = "My task"
        };

        public RunQuickTask(string name, Func<BaseErrorReporter, CancellationToken, Task<TaskUpdatePayload>> func)
        {
            _func = func;
            Name = name;
        }

        public override ITimeSpanProvider Schedule => _schedule != TimeSpan.Zero ? new BasicTimeSpanProvider(_schedule) : null;

        public override bool CanRunNow()
        {
            return CanRun();
        }

        public override Task<TaskUpdatePayload> Run(BaseErrorReporter task, CancellationToken token)
        {
            return _func(task, token);
        }

        public void SetSchedule(TimeSpan schedule)
        {
            _schedule = schedule;
        }
    }

    public class TaskSchedulerTest
    {
        private readonly ITestOutputHelper _output;
        public TaskSchedulerTest(ITestOutputHelper output)
        {
            _output = output;
        }

        // ExtractorTaskScheduler keeps its task registry in a private field, so tests that need
        // to reach the internal RegisteredTask directly (to reproduce a state the public API
        // can't construct on its own, such as "completed but not yet finished") go through
        // reflection. RegisteredTask itself is `internal`, not `private`, so it's directly usable
        // here thanks to ExtractorUtils' InternalsVisibleTo grant to this assembly.
        private static RegisteredTask GetRegisteredTask(ExtractorTaskScheduler sched, string name)
        {
            var field = typeof(ExtractorTaskScheduler).GetField("_tasks", BindingFlags.NonPublic | BindingFlags.Instance);
            var tasks = (Dictionary<string, RegisteredTask>)field!.GetValue(sched)!;
            return tasks[name];
        }

        [Fact]
        public async Task TestScheduler()
        {
            var sink = new DummySink();
            using var sched = new ExtractorTaskScheduler(sink, new NullLogger<ExtractorTaskScheduler>());
            using var source = new CancellationTokenSource();

            var running = sched.Run(source.Token);

            // Immediate task should finish
            using var evt = new ManualResetEvent(false);
            var task = new RunQuickTask("Task1", async (_, tok) =>
            {
                _output.WriteLine("Enter task");
                await CommonUtils.WaitAsync(evt, Timeout.InfiniteTimeSpan, tok);
                _output.WriteLine("Exit task");
                return null;
            });
            sched.AddScheduledTask(task, true);
            var waitTask = sched.WaitForNextEndOfTask("Task1", TimeSpan.FromSeconds(5));
            evt.Set();
            await waitTask;

            // Dependent tasks should run in sequence.
            var seq = new List<int>();
            bool[] finished = new bool[3];

            evt.Reset();

            for (int i = 0; i < 3; i++)
            {
                int c = i;
                var t = new RunQuickTask($"SeqTask{c}", async (_, tok) =>
                {
                    _output.WriteLine("Begin task " + c);
                    if (c == 0)
                    {
                        await CommonUtils.WaitAsync(evt, Timeout.InfiniteTimeSpan, tok);
                    }
                    _output.WriteLine("Finish task " + c);
                    seq.Add(c);
                    finished[c] = true;
                    return null;
                });
                t.CanRun = () =>
                {
                    _output.WriteLine("Check can run " + c);
                    return c == 0 || finished[c - 1];
                };
                sched.AddScheduledTask(t, true);
            }

            waitTask = sched.WaitForNextEndOfTask("SeqTask2", TimeSpan.FromSeconds(5));
            evt.Set();
            await waitTask;

            for (int i = 0; i < 3; i++)
            {
                Assert.Equal(i, seq[i]);
            }

            // Check that we have correct start and end tasks
            Assert.Equal(4, sink.TaskStart.Count);
            Assert.Equal(4, sink.TaskEnd.Count);
            Assert.Empty(sink.Errors);

            Assert.Equal("Task1", sink.TaskStart[0].Item1);
            Assert.Equal("Task1", sink.TaskEnd[0].Item1);
            Assert.Equal("SeqTask0", sink.TaskStart[1].Item1);
            Assert.Equal("SeqTask0", sink.TaskEnd[1].Item1);
            Assert.Equal("SeqTask1", sink.TaskStart[2].Item1);
            Assert.Equal("SeqTask1", sink.TaskEnd[2].Item1);
            Assert.Equal("SeqTask2", sink.TaskStart[3].Item1);
            Assert.Equal("SeqTask2", sink.TaskEnd[3].Item1);

            source.Cancel();
            await running;
        }

        [Fact]
        public async Task TestSchedulerErrors()
        {
            var sink = new DummySink();
            using var sched = new ExtractorTaskScheduler(sink, new NullLogger<ExtractorTaskScheduler>());
            using var source = new CancellationTokenSource();

            var running = sched.Run(source.Token);

            // Test report some errors
            using var evt = new ManualResetEvent(false);
            var task = new RunQuickTask("Task1", async (cbs, tok) =>
            {
                await CommonUtils.WaitAsync(evt, Timeout.InfiniteTimeSpan, tok);
                cbs.Warning("Instant warning");
                cbs.BeginWarning("Longer warning", "details").Dispose();
                cbs.Error("Instant error");
                cbs.BeginError("Longer error", "details").Dispose();
                return null;
            });
            sched.AddScheduledTask(task, true);
            var waitTask = sched.WaitForNextEndOfTask("Task1", TimeSpan.FromSeconds(5));
            evt.Set();
            await waitTask;

            // Each error is reported twice, once for start, once for end.
            // In the real sink, these are deduplicated.
            Assert.Equal(8, sink.Errors.Count);
            for (int i = 0; i < 4; i++)
            {
                Assert.Equal(ErrorLevel.warning, sink.Errors[i].Level);
            }
            for (int i = 4; i < 8; i++)
            {
                Assert.Equal(ErrorLevel.error, sink.Errors[i].Level);
            }
            Assert.Single(sink.TaskStart);
            Assert.Single(sink.TaskEnd);
        }

        [Fact]
        public async Task TestSchedulerFatalError()
        {
            var sink = new DummySink();
            using var sched = new ExtractorTaskScheduler(sink, new NullLogger<ExtractorTaskScheduler>());
            using var source = new CancellationTokenSource();

            var running = sched.Run(source.Token);

            using var evt = new ManualResetEvent(false);

            var task = new RunQuickTask("Task1", async (cbs, tok) =>
            {
                await CommonUtils.WaitAsync(evt, Timeout.InfiniteTimeSpan, tok);
                throw new Exception("Uh oh");
            });

            // Should not fail, just report a fatal error.
            sched.AddScheduledTask(task, true);
            var waitTask = sched.WaitForNextEndOfTask("Task1", TimeSpan.FromSeconds(5));
            evt.Set();
            // WaitForNextEndOfTask throws if the task fails.
            await Assert.ThrowsAsync<Exception>(async () => await waitTask);

            Assert.Equal(2, sink.Errors.Count);
            Assert.Equal(ErrorLevel.fatal, sink.Errors[0].Level);
            Assert.Equal(ErrorLevel.fatal, sink.Errors[1].Level);
            Assert.Single(sink.TaskStart);
            Assert.Single(sink.TaskEnd);

            // Should kill the scheduler
            task.SetErrorFatal = true;
            sched.ScheduleTaskNow("Task1");

            await Assert.ThrowsAsync<Exception>(async () => await running);

            Assert.Equal(4, sink.Errors.Count);
            Assert.Equal(ErrorLevel.fatal, sink.Errors[2].Level);
            Assert.Equal(ErrorLevel.fatal, sink.Errors[3].Level);
            Assert.Equal(2, sink.TaskStart.Count);
            Assert.Equal(2, sink.TaskEnd.Count);
        }

        [Fact]
        public async Task TestSchedulerCancel()
        {
            var sink = new DummySink();

            using var sched = new ExtractorTaskScheduler(sink, TestLogging.GetTestLogger<ExtractorTaskScheduler>(_output));
            using var source = new CancellationTokenSource();

            var running = sched.Run(source.Token);

            using var startEvt = new ManualResetEvent(false);
            using var evt = new ManualResetEvent(false);
            var task = new RunQuickTask("Task1", async (cbs, tok) =>
            {
                startEvt.Set();
                await CommonUtils.WaitAsync(evt, Timeout.InfiniteTimeSpan, tok);
                return null;
            });

            sched.AddScheduledTask(task, true);
            startEvt.WaitOne();

            await sched.CancelInnerAndWait(1000, sink);

            Assert.Single(sink.TaskStart);
            Assert.Single(sink.TaskEnd);
            Assert.Single(sink.Errors.DistinctBy(e => e.ExternalId));
            var err = sink.Errors[0];
            Assert.Equal(ErrorLevel.warning, err.Level);
            Assert.Equal("Task1", err.TaskName);
            Assert.Equal("Task was cancelled", err.Description);
        }

        [Fact]
        public async Task TestWaitWhenCancel()
        {
            var sink = new DummySink();

            using var sched = new ExtractorTaskScheduler(sink, TestLogging.GetTestLogger<ExtractorTaskScheduler>(_output));
            using var source = new CancellationTokenSource();

            var running = sched.Run(source.Token);

            using var startEvt = new ManualResetEvent(false);
            using var evt = new ManualResetEvent(false);
            bool taskTokenWasCancelled = false;
            var task = new RunQuickTask("Task1", async (cbs, tok) =>
            {
                startEvt.Set();
                try
                {
                    await CommonUtils.WaitAsync(evt, TimeSpan.FromSeconds(5), tok);
                }
                catch (OperationCanceledException)
                {
                    taskTokenWasCancelled = tok.IsCancellationRequested;
                    throw;
                }
                return null;
            });

            sched.AddScheduledTask(task, true);
            startEvt.WaitOne();

            // Task should be canceled while waiting for it to finish, and WaitForNextEndOfTask should throw TaskCanceledException.
            var waitTask = sched.WaitForNextEndOfTask("Task1", TimeSpan.FromSeconds(3));

            sched.CancelTask("Task1", "Test cancellation");

            await Assert.ThrowsAsync<AggregateException>(async () => await waitTask);

            Assert.True(taskTokenWasCancelled);
            Assert.Single(sink.Errors.DistinctBy(e => e.ExternalId));
            var err = sink.Errors[0];
            Assert.Equal("Task1", err.TaskName);
            Assert.Equal("Task was cancelled", err.Description);
            Assert.Equal("Test cancellation", err.Details);
        }

        [Fact]
        public async Task TestSchedulerCancelInnerAndWaitWithErrorIsFatalDoesNotCrash()
        {
            // Regression test for EDG-874: previously, an ordinary, expected shutdown
            // (CancelInnerAndWait) of a scheduler with an ErrorIsFatal=true task still running
            // would cause the scheduler's own Run task to fault, even though nothing actually
            // went wrong. This is reachable through BaseExtractor.ShutdownInternal ->
            // TaskScheduler.CancelInnerAndWait, with no Actions/Stop-action feature involved at
            // all -- any extractor with a long-running ErrorIsFatal continuous task (as
            // opcua-extractor-net has) would hit this on every normal shutdown.
            var sink = new DummySink();
            using var sched = new ExtractorTaskScheduler(sink, TestLogging.GetTestLogger<ExtractorTaskScheduler>(_output));
            using var source = new CancellationTokenSource();

            var running = sched.Run(source.Token);

            using var startEvt = new ManualResetEvent(false);
            using var evt = new ManualResetEvent(false);
            var task = new RunQuickTask("Task1", async (cbs, tok) =>
            {
                startEvt.Set();
                await CommonUtils.WaitAsync(evt, Timeout.InfiniteTimeSpan, tok);
                return null;
            });
            task.SetErrorFatal = true;

            sched.AddScheduledTask(task, true);
            startEvt.WaitOne();

            await sched.CancelInnerAndWait(5000, sink);

            // The scheduler's Run task must complete cleanly, not throw, even though the task
            // that was cancelled underneath it has ErrorIsFatal set.
            var result = await running;
            Assert.Equal(SchedulerTaskResult.Expected, result);

            Assert.Single(sink.TaskStart);
            Assert.Single(sink.TaskEnd);
            Assert.Single(sink.Errors.DistinctBy(e => e.ExternalId));
            var err = sink.Errors[0];
            Assert.Equal(ErrorLevel.warning, err.Level);
            Assert.Equal("Task1", err.TaskName);
            Assert.Equal("Task was cancelled", err.Description);
        }

        [Fact]
        public async Task TestSchedulerCancelTaskWithErrorIsFatalCrashesSchedulerByDefault()
        {
            // A task with ErrorIsFatal=true that hasn't explicitly opted out via
            // CancellationIsFatal=false is, by default, also fatal-on-Stop: the framework has no
            // way to know on its own whether this particular task can safely be interrupted
            // mid-run, so CancellationIsFatal defaults to matching ErrorIsFatal. Cancelling such a
            // task via CancelTask (the Stop-action path) must therefore still crash the scheduler,
            // same as it did before EDG-874 -- only scheduler-shutdown-caused cancellation is
            // unconditionally non-fatal (see TestSchedulerCancelInnerAndWaitWithErrorIsFatalDoesNotCrash).
            var sink = new DummySink();
            using var sched = new ExtractorTaskScheduler(sink, TestLogging.GetTestLogger<ExtractorTaskScheduler>(_output));
            using var source = new CancellationTokenSource();

            var running = sched.Run(source.Token);

            using var startEvt = new ManualResetEvent(false);
            using var evt = new ManualResetEvent(false);
            var task = new RunQuickTask("Task1", async (cbs, tok) =>
            {
                startEvt.Set();
                await CommonUtils.WaitAsync(evt, TimeSpan.FromSeconds(5), tok);
                return null;
            });
            task.SetErrorFatal = true;

            sched.AddScheduledTask(task, true);
            startEvt.WaitOne();

            sched.CancelTask("Task1", "Stop action");

            // The scheduler's Run task must fault -- ErrorIsFatal is preserved on Stop by default.
            await Assert.ThrowsAnyAsync<Exception>(async () => await running);

            Assert.Single(sink.Errors.DistinctBy(e => e.ExternalId));
            var err = sink.Errors[0];
            Assert.Equal(ErrorLevel.warning, err.Level);
            Assert.Equal("Task1", err.TaskName);
            Assert.Equal("Task was cancelled", err.Description);
            Assert.Equal("Stop action", err.Details);
        }

        [Fact]
        public async Task TestSchedulerCancelTaskWithCancellationIsFatalFalseDoesNotCrashScheduler()
        {
            // A task that explicitly opts out via CancellationIsFatal=false (while still keeping
            // ErrorIsFatal=true for genuine unexpected failures) may safely be Stopped mid-run:
            // cancelling it via CancelTask must not crash the whole scheduler. The waiter for that
            // specific task should still observe the cancellation, but nothing beyond that task
            // should be affected.
            var sink = new DummySink();
            using var sched = new ExtractorTaskScheduler(sink, TestLogging.GetTestLogger<ExtractorTaskScheduler>(_output));
            using var source = new CancellationTokenSource();

            var running = sched.Run(source.Token);

            using var startEvt = new ManualResetEvent(false);
            using var evt = new ManualResetEvent(false);
            var task = new RunQuickTask("Task1", async (cbs, tok) =>
            {
                startEvt.Set();
                await CommonUtils.WaitAsync(evt, TimeSpan.FromSeconds(5), tok);
                return null;
            });
            task.SetErrorFatal = true;
            task.SetCancellationFatal = false;

            sched.AddScheduledTask(task, true);
            startEvt.WaitOne();

            var waitTask = sched.WaitForNextEndOfTask("Task1", TimeSpan.FromSeconds(3));

            sched.CancelTask("Task1", "Stop action");

            // The waiter for this task is still told it was cancelled.
            await Assert.ThrowsAnyAsync<Exception>(async () => await waitTask);

            // But the scheduler itself must still be running -- this task declared itself safe to
            // Stop mid-run, so ErrorIsFatal must not turn that intentional Stop into a
            // process-crashing failure.
            Assert.False(running.IsCompleted);

            Assert.Single(sink.Errors.DistinctBy(e => e.ExternalId));
            var err = sink.Errors[0];
            Assert.Equal(ErrorLevel.warning, err.Level);
            Assert.Equal("Task1", err.TaskName);
            Assert.Equal("Task was cancelled", err.Description);
            Assert.Equal("Stop action", err.Details);

            // The scheduler should still be able to run other tasks normally afterwards.
            using var evt2 = new ManualResetEvent(false);
            var task2Ran = false;
            var task2 = new RunQuickTask("Task2", async (_, tok) =>
            {
                task2Ran = true;
                return null;
            });
            sched.AddScheduledTask(task2, true);
            await sched.WaitForNextEndOfTask("Task2", TimeSpan.FromSeconds(5));
            Assert.True(task2Ran);

            source.Cancel();
            await running;
        }

        [Fact]
        public async Task TestTryCancelTaskReturnsFalseWhenIdle()
        {
            // EDG-875: TryCancelTask must be able to tell "there was nothing to cancel" apart
            // from "successfully cancelled a running task" -- CancelTask's void return cannot
            // express this, which is exactly the ambiguity a Stop action needs resolved to
            // report `failed` ("is not currently running") instead of a false success.
            var sink = new DummySink();
            using var sched = new ExtractorTaskScheduler(sink, TestLogging.GetTestLogger<ExtractorTaskScheduler>(_output));
            using var source = new CancellationTokenSource();

            var running = sched.Run(source.Token);

            // A task with a schedule far in the future is registered, but not running yet.
            var task = new ScheduledTask("Task1", (_, tok) => Task.FromResult<TaskUpdatePayload>(null), TimeSpan.FromMinutes(10));
            sched.AddScheduledTask(task, false);

            Assert.False(sched.TryCancelTask("Task1"));
            // A true no-op: no error, no task-end event, nothing changed.
            Assert.Empty(sink.Errors);
            Assert.Empty(sink.TaskEnd);

            source.Cancel();
            await running;
        }

        [Fact]
        public async Task TestTryCancelTaskReturnsTrueWhenRunning()
        {
            var sink = new DummySink();
            using var sched = new ExtractorTaskScheduler(sink, TestLogging.GetTestLogger<ExtractorTaskScheduler>(_output));
            using var source = new CancellationTokenSource();

            var running = sched.Run(source.Token);

            using var startEvt = new ManualResetEvent(false);
            using var evt = new ManualResetEvent(false);
            var task = new RunQuickTask("Task1", async (_, tok) =>
            {
                startEvt.Set();
                await CommonUtils.WaitAsync(evt, TimeSpan.FromSeconds(5), tok);
                return null;
            });
            sched.AddScheduledTask(task, true);
            startEvt.WaitOne();

            var waitTask = sched.WaitForNextEndOfTask("Task1", TimeSpan.FromSeconds(3));
            Assert.True(sched.TryCancelTask("Task1", "Stop action"));
            await Assert.ThrowsAnyAsync<Exception>(async () => await waitTask);

            source.Cancel();
            await running;
        }

        [Fact]
        public void TestTryCancelTaskThrowsForUnknownTask()
        {
            var sink = new DummySink();
            using var sched = new ExtractorTaskScheduler(sink, TestLogging.GetTestLogger<ExtractorTaskScheduler>(_output));
            Assert.Throws<InvalidOperationException>(() => sched.TryCancelTask("DoesNotExist"));
        }

        [Fact]
        public async Task TestTryCancelTaskReturnsFalseForCompletedButNotYetFinishedTask()
        {
            // Regression test for a Gemini-review-flagged race: a task whose underlying Task has
            // already completed, but whose ActiveTask hasn't been cleared yet because the
            // scheduler's tick loop hasn't run FinishTask on it, must be treated as "not
            // running" by TryCancelTask -- otherwise Cancel() would mark an already-finished
            // task as CancelledIntentionally, causing FinishTask to later misreport a task that
            // actually completed successfully as cancelled.
            //
            // The scheduler's Run() loop is deliberately never started here, so nothing can race
            // in and clear ActiveTask -- this reproduces the "completed but not yet finished"
            // state deterministically via direct access to the internal RegisteredTask (visible
            // to this assembly via InternalsVisibleTo), rather than relying on hitting a real
            // timing window.
            var sink = new DummySink();
            using var sched = new ExtractorTaskScheduler(sink, TestLogging.GetTestLogger<ExtractorTaskScheduler>(_output));

            var task = new RunQuickTask("Task1", (_, __) => Task.FromResult<TaskUpdatePayload>(null));
            sched.AddScheduledTask(task, false);

            var registered = GetRegisteredTask(sched, "Task1");
            registered.Run(DateTime.UtcNow, CancellationToken.None);
            await registered.ActiveTask!.Task;

            Assert.True(registered.ActiveTask.Task.IsCompleted);
            Assert.False(sched.TryCancelTask("Task1", "Stop action"));
        }

        [Fact]
        public async Task TestTryScheduleTaskNowReturnsFalseWhenAlreadyRunning()
        {
            // EDG-875: mirrors TestTryCancelTaskReturnsFalseWhenIdle for the Start-action case --
            // a Start handler needs to distinguish "queued to run" from "already running" to
            // report `failed` ("already running") instead of a false success.
            var sink = new DummySink();
            using var sched = new ExtractorTaskScheduler(sink, TestLogging.GetTestLogger<ExtractorTaskScheduler>(_output));
            using var source = new CancellationTokenSource();

            var running = sched.Run(source.Token);

            using var startEvt = new ManualResetEvent(false);
            using var evt = new ManualResetEvent(false);
            var task = new RunQuickTask("Task1", async (_, tok) =>
            {
                startEvt.Set();
                await CommonUtils.WaitAsync(evt, Timeout.InfiniteTimeSpan, tok);
                return null;
            });
            sched.AddScheduledTask(task, true);
            startEvt.WaitOne();

            Assert.False(sched.TryScheduleTaskNow("Task1"));

            evt.Set();
            await sched.WaitForNextEndOfTask("Task1", TimeSpan.FromSeconds(5));

            source.Cancel();
            await running;
        }

        [Fact]
        public async Task TestTryScheduleTaskNowReturnsTrueAndRunsWhenIdle()
        {
            var sink = new DummySink();
            using var sched = new ExtractorTaskScheduler(sink, TestLogging.GetTestLogger<ExtractorTaskScheduler>(_output));
            using var source = new CancellationTokenSource();

            var running = sched.Run(source.Token);

            var ran = false;
            var task = new ScheduledTask("Task1", (_, tok) =>
            {
                ran = true;
                return Task.FromResult<TaskUpdatePayload>(null);
            }, TimeSpan.FromMinutes(10));
            // Not run immediately -- only the explicit TryScheduleTaskNow call below should start it.
            sched.AddScheduledTask(task, false);

            // Register the wait *before* triggering the task, not after -- otherwise this races
            // a fast-completing task finishing and flushing its waiters before this call gets to
            // AddWaiter, which would then hang until the 5s timeout. See
            // TestScheduler above for the same "register wait, then trigger" convention this
            // codebase already uses elsewhere, and BaseExtractor.RunStartTaskAction (EDG-878) for
            // where the identical bug was originally found and fixed in production code -- this
            // test had the same latent issue and was just lucky not to hit it until now.
            var waitTask = sched.WaitForNextEndOfTask("Task1", TimeSpan.FromSeconds(5));
            Assert.True(sched.TryScheduleTaskNow("Task1"));
            await waitTask;
            Assert.True(ran);

            source.Cancel();
            await running;
        }

        [Fact]
        public void TestTryScheduleTaskNowThrowsForUnknownTask()
        {
            var sink = new DummySink();
            using var sched = new ExtractorTaskScheduler(sink, TestLogging.GetTestLogger<ExtractorTaskScheduler>(_output));
            Assert.Throws<InvalidOperationException>(() => sched.TryScheduleTaskNow("DoesNotExist"));
        }

        [Fact]
        public async Task TestTryScheduleTaskNowReturnsTrueForCompletedButNotYetFinishedTask()
        {
            // Regression test for a Gemini-review-flagged race, mirroring
            // TestTryCancelTaskReturnsFalseForCompletedButNotYetFinishedTask for the Start-action
            // case: a task whose underlying Task has completed, but whose ActiveTask hasn't been
            // cleared yet because the scheduler's tick loop hasn't run FinishTask, must not be
            // reported as "already running" -- otherwise a Start action would spuriously fail on
            // a task that in fact just finished.
            var sink = new DummySink();
            using var sched = new ExtractorTaskScheduler(sink, TestLogging.GetTestLogger<ExtractorTaskScheduler>(_output));

            var task = new RunQuickTask("Task1", (_, __) => Task.FromResult<TaskUpdatePayload>(null));
            sched.AddScheduledTask(task, false);

            var registered = GetRegisteredTask(sched, "Task1");
            registered.Run(DateTime.UtcNow, CancellationToken.None);
            await registered.ActiveTask!.Task;

            Assert.True(registered.ActiveTask.Task.IsCompleted);
            Assert.True(sched.TryScheduleTaskNow("Task1"));
        }

        [Fact]
        public async Task TestCanTaskRunNowReflectsTaskState()
        {
            // EDG-875: a task can be successfully queued (TryScheduleTaskNow returns true) while
            // still being unable to actually start, for an unbounded period -- e.g. a task that
            // requires a live external connection. CanTaskRunNow must be able to observe this
            // independently, so a Start handler can fail fast instead of hanging.
            var sink = new DummySink();
            using var sched = new ExtractorTaskScheduler(sink, TestLogging.GetTestLogger<ExtractorTaskScheduler>(_output));
            using var source = new CancellationTokenSource();

            var running = sched.Run(source.Token);

            var canRun = false;
            var task = new RunQuickTask("Task1", (_, tok) => Task.FromResult<TaskUpdatePayload>(null))
            {
                CanRun = () => canRun
            };
            sched.AddScheduledTask(task, false);

            Assert.False(sched.CanTaskRunNow("Task1"));
            canRun = true;
            Assert.True(sched.CanTaskRunNow("Task1"));

            source.Cancel();
            await running;
        }

        [Fact]
        public void TestCanTaskRunNowThrowsForUnknownTask()
        {
            var sink = new DummySink();
            using var sched = new ExtractorTaskScheduler(sink, TestLogging.GetTestLogger<ExtractorTaskScheduler>(_output));
            Assert.Throws<InvalidOperationException>(() => sched.CanTaskRunNow("DoesNotExist"));
        }

        [Fact]
        public async Task TestFutureScheduledTasks()
        {
            var sink = new DummySink();
            using var sched = new ExtractorTaskScheduler(sink, TestLogging.GetTestLogger<ExtractorTaskScheduler>(_output));
            using var source = new CancellationTokenSource();

            var running = sched.Run(source.Token);

            var task1RunCount = 0;
            var task2RunCount = 0;

            // Create tasks with schedules in the future
            var task1 = new ScheduledTask("FutureTask1", (_, tok) =>
            {
                task1RunCount++;
                _output.WriteLine($"FutureTask1 ran, count: {task1RunCount}");
                return Task.FromResult<TaskUpdatePayload>(null);
            }, TimeSpan.FromMilliseconds(1000)); // Scheduled 1 second in the future

            var task2 = new ScheduledTask("FutureTask2", (_, tok) =>
            {
                task2RunCount++;
                _output.WriteLine($"FutureTask2 ran, count: {task2RunCount}");
                return Task.FromResult<TaskUpdatePayload>(null);
            }, TimeSpan.FromSeconds(2)); // Scheduled 2 seconds in the future

            // Add tasks with runImmediately=false, so they will use their schedule
            sched.AddScheduledTask(task1, false);
            sched.AddScheduledTask(task2, false);

            // Give the scheduler a moment to process
            await Task.Delay(50);

            // Neither task should have run yet since they are scheduled in the future
            Assert.Equal(0, task1RunCount);
            Assert.Equal(0, task2RunCount);
            Assert.Empty(sink.TaskStart);
            Assert.Empty(sink.TaskEnd);

            await Task.Delay(1050); // Ensure some time has passed


            // Task1 should have run, Task2 should not
            Assert.Equal(1, task1RunCount);
            Assert.Equal(0, task2RunCount);
            Assert.Single(sink.TaskEnd);
            Assert.Equal("FutureTask1", sink.TaskEnd[0].Item1);

            await Task.Delay(1000); // Ensure some time has passed
            await Task.Delay(50);

            // Both tasks should have run now
            Assert.Equal(2, task1RunCount);
            Assert.Equal(1, task2RunCount);
            Assert.Equal(3, sink.TaskEnd.Count);
            Assert.Contains(sink.TaskEnd, item => item.Item1 == "FutureTask2");

            source.Cancel();
            await running;
        }
    }

    class ScheduledTask : BaseSchedulableTask
    {
        private readonly bool _errorIsFatal;
        public override bool ErrorIsFatal => _errorIsFatal;

        public override string Name { get; }

        private readonly Func<BaseErrorReporter, CancellationToken, Task<TaskUpdatePayload>> _func;
        private readonly TimeSpan _schedule;

        public override ITimeSpanProvider Schedule => new BasicTimeSpanProvider(_schedule);

        public override TaskMetadata Metadata { get; } = new TaskMetadata(TaskType.batch)
        {
            Description = "Scheduled task"
        };

        public ScheduledTask(string name, Func<BaseErrorReporter, CancellationToken, Task<TaskUpdatePayload>> func, TimeSpan schedule, bool errorIsFatal = false)
        {
            _func = func;
            Name = name;
            _schedule = schedule;
            _errorIsFatal = errorIsFatal;
        }

        public override bool CanRunNow() => true;

        public override Task<TaskUpdatePayload> Run(BaseErrorReporter task, CancellationToken token)
        {
            return _func(task, token);
        }
    }
}
