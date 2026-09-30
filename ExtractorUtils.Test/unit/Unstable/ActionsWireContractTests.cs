using System.Text.Json;
using CogniteSdk.Alpha;
using Xunit;

namespace ExtractorUtils.Test.unit.Unstable
{
    /// <summary>
    /// EDG-875: pins down JSON/enum wire behaviors the whole Actions feature depends on, so a
    /// future unrelated change to shared JsonSerializerOptions elsewhere can't silently break it.
    ///
    /// These tests use an explicit camelCase JsonSerializerOptions, matching the global policy
    /// cognite-sdk-dotnet sets (Oryx.Cognite/src/Common.fs) for all wire traffic -- this repo
    /// never overrides it itself, so replicating it here is what makes the test representative
    /// of real (de)serialization rather than .NET's PascalCase default.
    /// </summary>
    public class ActionsWireContractTests
    {
        private static readonly JsonSerializerOptions CamelCaseOptions = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        };

        [Theory]
        [InlineData(ActionStatus.pending, "pending")]
        [InlineData(ActionStatus.running, "running")]
        [InlineData(ActionStatus.failed, "failed")]
        [InlineData(ActionStatus.succeeded, "succeeded")]
        [InlineData(ActionStatus.cancel_pending, "cancel_pending")]
        [InlineData(ActionStatus.canceled, "canceled")]
        public void ActionStatusSerializesToExactWireString(ActionStatus status, string expected)
        {
            // The [JsonStringEnumConverter] attribute lives on the enum type itself, so this
            // does not depend on CamelCaseOptions -- verifying that explicitly by using
            // default options here, unlike the other tests in this file.
            var json = JsonSerializer.Serialize(status);
            Assert.Equal($"\"{expected}\"", json);

            var roundTripped = JsonSerializer.Deserialize<ActionStatus>(json);
            Assert.Equal(status, roundTripped);
        }

        [Theory]
        [InlineData(ActionType.start_task, "start_task")]
        [InlineData(ActionType.stop_task, "stop_task")]
        [InlineData(ActionType.custom, "custom")]
        public void ActionTypeSerializesToExactWireString(ActionType type, string expected)
        {
            var json = JsonSerializer.Serialize(type);
            Assert.Equal($"\"{expected}\"", json);

            var roundTripped = JsonSerializer.Deserialize<ActionType>(json);
            Assert.Equal(type, roundTripped);
        }

        [Fact]
        public void CheckInResponseIgnoresUnknownTopLevelAndNestedProperties()
        {
            // Simulates a checkin response containing a field this version of the client
            // doesn't know about yet (both at the top level and nested inside a pending
            // action), alongside a real, well-formed pendingActions entry. This must not throw --
            // if it did, a server-side addition of a new field could break every extractor still
            // running an older client version.
            var json = """
            {
                "externalId": "my-integration",
                "lastConfigRevision": 3,
                "aFieldThisClientHasNeverHeardOf": { "foo": "bar" },
                "pendingActions": [
                    {
                        "externalId": "action-1",
                        "actionName": "Stop MyTask",
                        "status": "pending",
                        "callMetadata": { "key": "value" },
                        "createdTime": 1000,
                        "lastUpdatedTime": 2000,
                        "aFutureFieldOnAnAction": 42
                    }
                ]
            }
            """;

            var response = JsonSerializer.Deserialize<CheckInResponse>(json, CamelCaseOptions);

            Assert.NotNull(response);
            Assert.Equal("my-integration", response.ExternalId);
            Assert.Equal(3, response.LastConfigRevision);
            Assert.Single(response.PendingActions);

            var action = System.Linq.Enumerable.First(response.PendingActions);
            Assert.Equal("action-1", action.ExternalId);
            Assert.Equal("Stop MyTask", action.ActionName);
            Assert.Equal(ActionStatus.pending, action.Status);
            Assert.Equal("value", action.CallMetadata["key"]);
        }

        [Fact]
        public void CheckInResponseWithNoPendingActionsDeserializesToEmpty()
        {
            // An ordinary checkin response with nothing for the extractor to do -- the common
            // case, exercised here to make sure it doesn't require pendingActions to be present.
            var json = """{ "externalId": "my-integration" }""";

            var response = JsonSerializer.Deserialize<CheckInResponse>(json, CamelCaseOptions);

            Assert.NotNull(response);
            Assert.Null(response.PendingActions);
        }

        [Fact]
        public void ActionUpdateRoundTripsWithCamelCaseWireNames()
        {
            var update = new ActionUpdate
            {
                ExternalId = "action-1",
                Status = ActionStatus.succeeded,
                ResultMessage = "Uploaded 3 files",
                ResultMetadata = new System.Collections.Generic.Dictionary<string, string>
                {
                    ["fileCount"] = "3",
                },
            };

            var json = JsonSerializer.Serialize(update, CamelCaseOptions);

            // Wire format must be camelCase -- this is what odin's Pydantic models
            // (alias_generator=camelize) actually expect on the request body.
            Assert.Contains("\"externalId\"", json);
            Assert.Contains("\"resultMessage\"", json);
            Assert.Contains("\"resultMetadata\"", json);
            Assert.Contains("\"succeeded\"", json);
            Assert.DoesNotContain("\"ExternalId\"", json);

            var roundTripped = JsonSerializer.Deserialize<ActionUpdate>(json, CamelCaseOptions);
            Assert.Equal(update.ExternalId, roundTripped.ExternalId);
            Assert.Equal(update.Status, roundTripped.Status);
            Assert.Equal(update.ResultMessage, roundTripped.ResultMessage);
            Assert.Equal("3", roundTripped.ResultMetadata["fileCount"]);
        }
    }
}
