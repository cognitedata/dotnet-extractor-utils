using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extractor.Utils.Unstable.Tasks;
using CogniteSdk.Alpha;
using Xunit;

namespace ExtractorUtils.Test.unit.Unstable
{
    public class ActionsTest
    {
        [Fact]
        public void TestSetResultThrowsIfCalledTwice()
        {
            var sink = new DummySink();
            var ctx = new ActionContext<string>("config", null, "integration-1", "action-1", null, sink);

            ctx.SetResult("first");
            Assert.Throws<InvalidOperationException>(() => ctx.SetResult("second"));
        }

        [Fact]
        public void TestSetResultRecordsMessageAndMetadata()
        {
            var sink = new DummySink();
            var ctx = new ActionContext<string>("config", null, "integration-1", "action-1", null, sink);

            var metadata = new Dictionary<string, string> { ["fileCount"] = "3" };
            ctx.SetResult("Uploaded 3 files", metadata);

            Assert.Equal("Uploaded 3 files", ctx.ResultMessage);
            Assert.Equal("3", ctx.ResultMetadata["fileCount"]);
        }

        [Fact]
        public void TestResultIsNullIfSetResultNeverCalled()
        {
            // Mirrors the design intent (see ActionContext.SetResult's doc comment): an author
            // who never calls SetResult still produces a valid, if empty, terminal result --
            // this is not an error state the dispatcher needs to special-case.
            var sink = new DummySink();
            var ctx = new ActionContext<string>("config", null, "integration-1", "action-1", null, sink);

            Assert.Null(ctx.ResultMessage);
            Assert.Null(ctx.ResultMetadata);
        }

        [Fact]
        public void TestReportProgressQueuesRunningUpdate()
        {
            var sink = new DummySink();
            var ctx = new ActionContext<string>("config", null, "integration-1", "action-1", null, sink);

            ctx.ReportProgress("50% complete");

            Assert.Single(sink.ActionUpdates);
            var update = sink.ActionUpdates[0];
            Assert.Equal("action-1", update.ExternalId);
            Assert.Equal(ActionStatus.running, update.Status);
            Assert.Equal("50% complete", update.ResultMessage);
        }

        [Fact]
        public void TestReportProgressCanBeCalledMultipleTimesAndDoesNotBlockSetResult()
        {
            var sink = new DummySink();
            var ctx = new ActionContext<string>("config", null, "integration-1", "action-1", null, sink);

            ctx.ReportProgress("25% complete");
            ctx.ReportProgress("75% complete");
            // Must not throw -- ReportProgress does not interact with the "call once" guard.
            ctx.SetResult("Done");

            Assert.Equal(2, sink.ActionUpdates.Count);
            Assert.Equal("Done", ctx.ResultMessage);
        }

        [Fact]
        public void TestActionContextExposesConstructorArguments()
        {
            var sink = new DummySink();
            var callMetadata = new Dictionary<string, string> { ["startDate"] = "2026-01-01" };
            var ctx = new ActionContext<string>("my-config", null, "integration-1", "action-1", callMetadata, sink);

            Assert.Equal("my-config", ctx.ApplicationConfig);
            Assert.Null(ctx.CdfClient);
            Assert.Equal("integration-1", ctx.IntegrationExternalId);
            Assert.Equal("action-1", ctx.ExternalId);
            Assert.Equal("2026-01-01", ctx.CallMetadata["startDate"]);
        }

        [Fact]
        public void TestActionErrorResultMetadataIncludesErrorTypeAndDetail()
        {
            var err = new ActionError("invalid_parameter", "start_date is not a valid date", "expected ISO 8601, got 'foo'");

            Assert.Equal("invalid_parameter", err.ErrorType);
            Assert.Equal("start_date is not a valid date", err.Message);
            Assert.Equal("invalid_parameter", err.ResultMetadata["errorType"]);
            Assert.Equal("expected ISO 8601, got 'foo'", err.ResultMetadata["errorDetail"]);
        }

        [Fact]
        public void TestActionErrorResultMetadataOmitsDetailIfNotProvided()
        {
            var err = new ActionError("missing_parameter", "start_date is required");

            Assert.Equal("missing_parameter", err.ResultMetadata["errorType"]);
            Assert.False(err.ResultMetadata.ContainsKey("errorDetail"));
        }

        [Fact]
        public void TestActionErrorRequiresErrorType()
        {
            Assert.Throws<ArgumentException>(() => new ActionError(null, "message"));
            Assert.Throws<ArgumentException>(() => new ActionError("", "message"));
        }

        [Fact]
        public void TestCustomActionRequiresNameAndTarget()
        {
            Func<ActionContext<string>, CancellationToken, Task> target = (ctx, tok) => Task.CompletedTask;

            Assert.Throws<ArgumentException>(() => new CustomAction<string>(null, target));
            Assert.Throws<ArgumentException>(() => new CustomAction<string>("", target));
            Assert.Throws<ArgumentNullException>(() => new CustomAction<string>("name", null));
        }

        [Fact]
        public void TestCustomActionExposesConstructorArguments()
        {
            Func<ActionContext<string>, CancellationToken, Task> target = (ctx, tok) => Task.CompletedTask;
            var action = new CustomAction<string>("fetch_logs", target, "Fetch logs");

            Assert.Equal("fetch_logs", action.Name);
            Assert.Equal("Fetch logs", action.Description);
            Assert.Same(target, action.Target);
        }
    }
}
