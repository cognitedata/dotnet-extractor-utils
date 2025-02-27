using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extractor.Common;
using Cognite.ExtractorUtils.Unstable.Tasks;
using CogniteSdk.Alpha;
using Xunit;
using Xunit.Abstractions;

namespace ExtractorUtils.Test.unit.Unstable
{
    class DummySink : BaseErrorReporter, IIntegrationSink
    {
        public List<ExtractorError> Errors { get; } = new();
        public List<(string, DateTime)> TaskStart { get; } = new();
        public List<(string, DateTime)> TaskEnd { get; } = new();

        public override ExtractorError NewError(ErrorLevel level, string description, string details = null, DateTime? now = null)
        {
            return new ExtractorError(level, description, this, details, null, now);
        }

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
    }

    class RunQuickTask : BaseSchedulableTask
    {
        bool _errorIsFatal;
        public override bool ErrorIsFatal => _errorIsFatal;

        public bool SetErrorFatal { set => _errorIsFatal = value; }
        public Func<bool> CanRun { get; set; } = () => true;

        public override string Name { get; }

        private Func<BaseErrorReporter, CancellationToken, Task<TaskUpdatePayload>> _func;

        public RunQuickTask(string name, Func<BaseErrorReporter, CancellationToken, Task<TaskUpdatePayload>> func)
        {
            _func = func;
            Name = name;
        }

        public override bool CanRunNow()
        {
            return CanRun();
        }

        public override Task<TaskUpdatePayload> Run(BaseErrorReporter task, CancellationToken token)
        {
            return _func(task, token);
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
            using var sched = new ExtractorTaskScheduler(sink);
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
            using var sched = new ExtractorTaskScheduler(sink);
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
            using var sched = new ExtractorTaskScheduler(sink);
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
    }
}