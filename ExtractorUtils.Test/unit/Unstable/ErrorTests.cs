using System;
using Cognite.Extractor.Utils.Unstable.Tasks;
using CogniteSdk.Alpha;
using Xunit;

namespace ExtractorUtils.Test.unit.Unstable
{
    public class ErrorTests
    {
        [Fact]
        public void ToSdk_PropagatesTypeAndConfigRevision()
        {
            var sink = new DummySink();
            var now = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            var error = sink.NewError(ErrorLevel.fatal, "bad config", details: "stack trace here", now: now, type: "parse", configRevision: 7);
            error.Instant();

            var sdk = error.ToSdk();

            Assert.Equal(ErrorLevel.fatal, sdk.Level);
            Assert.Equal("bad config", sdk.Description);
            Assert.Equal("stack trace here", sdk.Details);
            Assert.Equal("parse", sdk.Type);
            Assert.Equal(7, sdk.ConfigRevision);
        }

        [Fact]
        public void ToSdk_TypeAndConfigRevisionAreNullByDefault()
        {
            var sink = new DummySink();
            var error = sink.NewError(ErrorLevel.warning, "mild issue");
            error.Instant();

            var sdk = error.ToSdk();

            Assert.Null(sdk.Type);
            Assert.Null(sdk.ConfigRevision);
        }
    }
}
