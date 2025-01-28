using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extractor.Common;
using Cognite.Extractor.Testing;
using Cognite.ExtractorUtils.Unstable.Tasks;
using CogniteSdk.Alpha;
using Xunit;
using Xunit.Abstractions;

namespace ExtractorUtils.Test.unit.Unstable
{
    class DummySink : IIntegrationSink
    {
        public List<ExtractorError> Errors { get; } = new();
        public List<(string, DateTime)> TaskStart { get; } = new();
        public List<(string, DateTime)> TaskEnd { get; } = new();


        public void ReportError(ExtractorError error)
        {
            Errors.Add(error);
        }

        public void ReportTaskEnd(string taskName, DateTime? timestamp = null)
        {
            TaskEnd.Add((taskName, timestamp ?? DateTime.UtcNow));
        }

        public void ReportTaskStart(string taskName, DateTime? timestamp = null)
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

        private Func<BaseErrorReporter, CancellationToken, Task> _func;

        public RunQuickTask(Func<BaseErrorReporter, CancellationToken, Task> func)
        {
            _func = func;
        }

        public override bool CanRunNow()
        {
            return CanRun();
        }

        public override Task Run(BaseErrorReporter task, CancellationToken token)
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
            var task = new RunQuickTask(async (_, tok) =>
            {
                _output.WriteLine("Enter task");
                await CommonUtils.WaitAsync(evt, Timeout.InfiniteTimeSpan, tok);
                _output.WriteLine("Exit task");
            });
            sched.AddScheduledTask("Task1", null, task, true);
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
                var t = new RunQuickTask(async (_, tok) =>
                {
                    _output.WriteLine("Begin task " + c);
                    if (c == 0)
                    {
                        await CommonUtils.WaitAsync(evt, Timeout.InfiniteTimeSpan, tok);
                    }
                    _output.WriteLine("Finish task " + c);
                    seq.Add(c);
                    finished[c] = true;
                });
                t.CanRun = () =>
                {
                    _output.WriteLine("Check can run " + c);
                    return c == 0 || finished[c - 1];
                };
                sched.AddScheduledTask($"SeqTask{c}", null, t, true);
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
            var task = new RunQuickTask(async (cbs, tok) =>
            {
                await CommonUtils.WaitAsync(evt, Timeout.InfiniteTimeSpan, tok);
                cbs.Warning("Instant warning");
                cbs.BeginWarning("Longer warning", "details").Dispose();
                cbs.Error("Instant error");
                cbs.BeginError("Longer error", "details").Dispose();
            });
            sched.AddScheduledTask("Task1", null, task, true);
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

            var task = new RunQuickTask(async (cbs, tok) =>
            {
                await CommonUtils.WaitAsync(evt, Timeout.InfiniteTimeSpan, tok);
                throw new Exception("Uh oh");
            });

            // Should not fail, just report a fatal error.
            sched.AddScheduledTask("Task1", null, task, true);
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