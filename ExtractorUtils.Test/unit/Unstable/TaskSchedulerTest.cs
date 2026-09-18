using System;
using System.Collections.Generic;
using System.Linq;
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
    }

    class RunQuickTask : BaseSchedulableTask
    {
        bool _errorIsFatal;
        public override bool ErrorIsFatal => _errorIsFatal;

        private TimeSpan _schedule = TimeSpan.Zero;
        public bool SetErrorFatal { set => _errorIsFatal = value; }
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

        [Fact]
        public async Task TestFinishTaskDoesNotReportCancelledForTaskThatCompletedBeforeCancellation()
        {
            // EDG-883: regression test for a race where a task that already completed
            // successfully could be misreported as "cancelled", if an unrelated later
            // cancellation (e.g. scheduler shutdown) happened to fire before the scheduler's own
            // loop got around to calling FinishTask for that already-finished task.
            //
            // Exercises RegisteredTask directly (rather than going through the full
            // ExtractorTaskScheduler.Run loop, as the originally-flaky
            // BaseExtractorTest.TestBaseExtractor does) specifically so this is deterministic:
            // by explicitly awaiting the task's own completion before cancelling, the ordering
            // this test needs ("task finished" strictly before "cancellation requested") is
            // guaranteed on every run, rather than depending on incidental scheduler/thread-pool
            // timing that only reproduced the bug intermittently.
            var sink = new DummySink();
            var reporter = new TaskReporter("task1", sink);
            var task = new RunQuickTask("task1", (_, tok) => Task.FromResult<TaskUpdatePayload>(null));
            using var registered = new RegisteredTask(task, reporter, runImmediately: true);

            using var outerSource = new CancellationTokenSource();
            registered.Run(DateTime.UtcNow, outerSource.Token);

            // Wait for the task to actually finish running.
            await registered.ActiveTask!.Task;

            // Cancel strictly after the task already completed successfully. This cascades into
            // the per-task linked CancellationTokenSource (see RegisteredTask.Run), which is
            // exactly the mechanism that used to cause a false "cancelled" report.
            outerSource.Cancel();

            registered.FinishTask(DateTime.UtcNow);

            Assert.Empty(sink.Errors);
        }

        [Fact]
        public async Task TestFinishTaskStillReportsCancelledForTaskThatFaultedWhileCancellationWasRequested()
        {
            // Companion to the test above, verifying the fix didn't overcorrect: a task that
            // faults with an *unrelated* exception (not a clean OperationCanceledException tied
            // to its own token) while cancellation had already been requested must still be
            // reported as a benign cancellation-related warning, not escalated to Fatal. This is
            // pre-existing, deliberate behavior -- it must keep working, and this test would
            // fail if WasCancellationRequestedAtCompletion were hardcoded to always be false
            // (which would make the test above pass for the wrong reason).
            var sink = new DummySink();
            var reporter = new TaskReporter("task1", sink);
            using var startEvt = new ManualResetEvent(false);
            using var evt = new ManualResetEvent(false);
            // Deliberately waits on CancellationToken.None, not `tok` -- this task ignores its
            // own cancellation token and instead faults with an unrelated exception once
            // signalled, rather than ending in the Canceled state.
            var task = new RunQuickTask("task1", async (_, tok) =>
            {
                startEvt.Set();
                await CommonUtils.WaitAsync(evt, Timeout.InfiniteTimeSpan, CancellationToken.None);
                throw new InvalidOperationException("boom");
            });
            using var registered = new RegisteredTask(task, reporter, runImmediately: true);

            using var outerSource = new CancellationTokenSource();
            registered.Run(DateTime.UtcNow, outerSource.Token);

            // Wait for the delegate to actually start before cancelling -- otherwise this can
            // race Task.Run's own pre-execution cancellation check (which can cancel the outer
            // task before the delegate ever runs at all if the token is already cancelled by the
            // time a thread-pool thread picks it up), rather than exercising the intended
            // "faulted while already running" scenario.
            startEvt.WaitOne();

            // Cancellation is requested while the task is still running (well before it faults).
            outerSource.Cancel();
            evt.Set();

            try
            {
                await registered.ActiveTask!.Task;
            }
            catch (InvalidOperationException)
            {
                // Expected -- the task faults rather than cancelling cleanly.
            }

            registered.FinishTask(DateTime.UtcNow);

            // A single logical report shows up as two entries with the same ExternalId (once
            // for start, once for end) -- matches the existing convention in e.g.
            // TestSchedulerCancel above.
            Assert.Single(sink.Errors.DistinctBy(e => e.ExternalId));
            Assert.Equal(ErrorLevel.warning, sink.Errors[0].Level);
            Assert.Equal("Task was cancelled", sink.Errors[0].Description);
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
