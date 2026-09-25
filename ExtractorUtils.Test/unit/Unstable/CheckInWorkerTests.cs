using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extensions.Unstable;
using Cognite.Extractor.Utils.Unstable;
using Cognite.Extractor.Utils.Unstable.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Newtonsoft.Json;
using Xunit;
using Xunit.Abstractions;
using Cognite.Extensions;
using Cognite.Extractor.Testing;
using Cognite.Extractor.Configuration;
using Cognite.Extractor.Utils;
using Cognite.Extractor.Utils.Unstable.Tasks;
using Microsoft.Extensions.Logging;
using CogniteSdk.Alpha;
using CogniteSdk;


namespace ExtractorUtils.Test.Unit.Unstable
{
    public class CheckInWorkerTests
    {
        private List<dynamic> taskEvents = new();
        private List<dynamic> errors = new();
        private List<dynamic> startupRequests = new();
        private List<dynamic> actionUpdates = new();

        private int? _lastConfigRevision;
        private List<dynamic> _pendingActionsToReturn = new();
        private int _failCheckInsRemaining;
        // When set, the next checkin attempt returns a 404 whose body identifies this specific
        // externalId as missing (matching odin's actual error shape for an unknown action),
        // instead of the generic transient-failure simulation below. Cleared after firing once.
        private string _notFoundActionExternalId;
        // Fires exactly once, immediately before a simulated checkin failure -- lets a test
        // queue a new update "while the failing send is still in flight", deterministically,
        // without needing real thread concurrency (everything here is single-threaded async).
        private Action _onCheckInFailureAboutToHappen;

        private readonly ITestOutputHelper _output;
        private int _checkInCount;
        private int _startupCount;
        public CheckInWorkerTests(ITestOutputHelper output)
        {
            _output = output;
        }


        private ConnectionConfig GetConfig()
        {
            return new ConnectionConfig
            {
                Project = "project",
                BaseUrl = "https://greenfield.cognitedata.com",
                Integration = new IntegrationConfig
                {
                    ExternalId = "test-integration"
                },
                Authentication = new ClientCredentialsConfig
                {
                    ClientId = "someId",
                    ClientSecret = "thisIsASecret",
                    Scopes = new Cognite.Common.ListOrSpaceSeparated("https://greenfield.cognitedata.com/.default"),
                    MinTtl = "60s",
                    Resource = "resource",
                    Audience = "audience",
                    TokenUrl = "http://example.url/token",
                }
            };
        }

        private List<int> _checkInCallbacks = new();

        private (ServiceProvider, CheckInWorker) GetCheckInWorker()
        {
            var config = GetConfig();

            var services = new ServiceCollection();
            services.AddConfig(config, typeof(ConnectionConfig));
            var mocks = TestUtilities.GetMockedHttpClientFactory(mockCheckInAsync);
            var mockHttpMessageHandler = mocks.handler;
            var mockFactory = mocks.factory;
            services.AddSingleton(mockFactory.Object);
            services.AddTestLogging(_output);
            DestinationUtilsUnstable.AddCogniteClient(services, "myApp", null, setLogger: true, setMetrics: true, setHttpClient: true);
            var provider = services.BuildServiceProvider();

            var client = provider.GetRequiredService<Client>();

            return (provider, new CheckInWorker(
                config.Integration.ExternalId,
                provider.GetRequiredService<ILogger<CheckInWorker>>(),
                client,
                _checkInCallbacks.Add,
                0
            ));
        }

        [Fact]
        public async Task TestCheckInWorker()
        {
            var (provider, checkIn) = GetCheckInWorker();
            using var p = provider;
            using var source = new CancellationTokenSource();
            // Check that this doesn't crash, and properly cancels out at the end.
            var runTask = checkIn.RunPeriodicCheckIn(source.Token, new StartupRequest(), Timeout.InfiniteTimeSpan);
            // First, we should very quickly report a check-in on the start of the run task...
            await TestUtils.WaitForCondition(() => _checkInCount == 1, 5);

            // Report an empty check-in.
            await checkIn.Flush(source.Token);
            Assert.Equal(2, _checkInCount);

            var start = DateTime.UtcNow;
            // Report a check-in with some task starts
            checkIn.ReportTaskStart("task1", null, start);
            checkIn.ReportTaskStart("task2", null, start);
            await checkIn.Flush(source.Token);
            Assert.Equal(3, _checkInCount);
            Assert.Equal(2, taskEvents.Count);
            Assert.Equal("task1", (string)taskEvents[0].name);
            Assert.Equal("started", (string)taskEvents[0].type);
            Assert.NotNull(taskEvents[0].timestamp);

            _lastConfigRevision = 1;

            // Report some errors
            checkIn.ReportError(new ExtractorError(ErrorLevel.warning, "test", checkIn, now: start.AddSeconds(1)));
            checkIn.ReportError(new ExtractorError(ErrorLevel.error, "test", checkIn, now: start.AddSeconds(1)));

            await checkIn.Flush(source.Token);
            Assert.Equal(4, _checkInCount);
            Assert.Equal(2, errors.Count);
            Assert.Single(_checkInCallbacks);
            Assert.Equal(1, _checkInCallbacks[0]);

            // Report some task ends
            checkIn.ReportTaskEnd("task1", null, start.AddSeconds(2));
            checkIn.ReportTaskEnd("task2", null, start.AddSeconds(2));
            await checkIn.Flush(source.Token);
            Assert.Equal(5, _checkInCount);
            Assert.Equal(4, taskEvents.Count);
            Assert.Single(_checkInCallbacks);

            source.Cancel();
            await TestUtils.RunWithTimeout(runTask, 5);

            Assert.Single(startupRequests);
            Assert.Equal("test-integration", (string)startupRequests[0].externalId);
            Assert.Equal(1, _startupCount);
        }

        [Fact]
        public async Task TestCheckInWorkerBatch()
        {
            var (provider, checkIn) = GetCheckInWorker();
            using var p = provider;
            using var source = new CancellationTokenSource();

            // Check that this doesn't crash, and properly cancels out at the end.
            var runTask = checkIn.RunPeriodicCheckIn(source.Token, new StartupRequest(), Timeout.InfiniteTimeSpan);
            // First, we should very quickly report a check-in on the start of the run task...
            await TestUtils.WaitForCondition(() => _checkInCount == 1, 5);

            // Lots of task updates.
            var start = DateTime.UtcNow;
            for (int i = 0; i < 1000; i++)
            {
                checkIn.ReportTaskStart("task1", null, start.AddSeconds(i));
                checkIn.ReportTaskEnd("task1", null, start.AddSeconds(i + 1));
            }
            // Add errors in wrong order. The one offset by 1 should be written first.
            checkIn.ReportError(new ExtractorError(ErrorLevel.error, "test", checkIn, now: start.AddSeconds(900)));
            checkIn.ReportError(new ExtractorError(ErrorLevel.warning, "test", checkIn, now: start.AddSeconds(1)));

            await checkIn.Flush(source.Token);
            Assert.Equal(3, _checkInCount);
            Assert.Equal(2000, taskEvents.Count);
            Assert.Equal(2, errors.Count);
            Assert.Equal(ErrorLevel.warning, (ErrorLevel)errors[0].level);
            Assert.Equal(ErrorLevel.error, (ErrorLevel)errors[1].level);

            // Lots of errors
            taskEvents.Clear();
            errors.Clear();
            for (int i = 0; i < 2000; i++)
            {
                checkIn.ReportError(new ExtractorError(ErrorLevel.error, "test", checkIn, now: start.AddSeconds(i)));
            }
            checkIn.ReportTaskStart("task1", null, start.AddSeconds(1));
            checkIn.ReportTaskEnd("task1", null, start.AddSeconds(1900));
            await checkIn.Flush(source.Token);
            Assert.Equal(5, _checkInCount);
            Assert.Equal(2, taskEvents.Count);
            Assert.Equal(2000, errors.Count);

            source.Cancel();
            await TestUtils.RunWithTimeout(runTask, 5);
        }

        [Fact]
        public async Task TestCheckInWorkerCallback()
        {
            var (provider, checkIn) = GetCheckInWorker();
            using var p = provider;
            using var source = new CancellationTokenSource();

            // Check that this doesn't crash, and properly cancels out at the end.
            var runTask = checkIn.RunPeriodicCheckIn(source.Token, new StartupRequest(), Timeout.InfiniteTimeSpan);
            // First, we should very quickly report a check-in on the start of the run task...
            await TestUtils.WaitForCondition(() => _checkInCount == 1, 5);

            // Flush, without getting a new config revision.
            await checkIn.Flush(source.Token);
            Assert.Empty(_checkInCallbacks);

            // Set a new revision and flush again.
            _lastConfigRevision = 1;
            await checkIn.Flush(source.Token);
            Assert.Single(_checkInCallbacks);
            Assert.Equal(1, _checkInCallbacks[0]);

            // Flush without changes
            await checkIn.Flush(source.Token);
            Assert.Single(_checkInCallbacks);
            Assert.Equal(1, _checkInCallbacks[0]);

            // Flush with a new revision
            _lastConfigRevision = 2;
            await checkIn.Flush(source.Token);
            Assert.Equal(2, _checkInCallbacks.Count);
            Assert.Equal(2, _checkInCallbacks[1]);
            Assert.Equal(1, _checkInCallbacks[0]);
        }

        [Fact]
        public async Task TestActionUpdateQueuingAndSending()
        {
            var (provider, checkIn) = GetCheckInWorker();
            using var p = provider;
            using var source = new CancellationTokenSource();

            var runTask = checkIn.RunPeriodicCheckIn(source.Token, new StartupRequest(), Timeout.InfiniteTimeSpan);
            await TestUtils.WaitForCondition(() => _checkInCount == 1, 5);

            checkIn.QueueActionUpdate(new ActionUpdate
            {
                ExternalId = "action-1",
                Status = ActionStatus.succeeded,
                ResultMessage = "Uploaded 3 files",
                ResultMetadata = new Dictionary<string, string> { ["fileCount"] = "3" },
            });
            checkIn.QueueActionUpdate(new ActionUpdate
            {
                ExternalId = "action-2",
                Status = ActionStatus.failed,
                ResultMessage = "Task not currently running",
            });

            await checkIn.Flush(source.Token);

            Assert.Equal(2, actionUpdates.Count);
            Assert.Equal("action-1", (string)actionUpdates[0].externalId);
            Assert.Equal("succeeded", (string)actionUpdates[0].status);
            Assert.Equal("3", (string)actionUpdates[0].resultMetadata.fileCount);
            Assert.Equal("action-2", (string)actionUpdates[1].externalId);
            Assert.Equal("failed", (string)actionUpdates[1].status);

            source.Cancel();
            await TestUtils.RunWithTimeout(runTask, 5);
        }

        [Fact]
        public async Task TestActionUpdateBatchCap()
        {
            // EDG-876: at most MAX_ACTION_UPDATES_PER_CHECKIN (100) action updates may be sent
            // per checkin request, matching odin's confirmed 0-100 limit on actionUpdates[].
            // Excess must remain queued for a later checkin, not be dropped.
            var (provider, checkIn) = GetCheckInWorker();
            using var p = provider;
            using var source = new CancellationTokenSource();

            var runTask = checkIn.RunPeriodicCheckIn(source.Token, new StartupRequest(), Timeout.InfiniteTimeSpan);
            await TestUtils.WaitForCondition(() => _checkInCount == 1, 5);

            for (int i = 0; i < 150; i++)
            {
                checkIn.QueueActionUpdate(new ActionUpdate
                {
                    ExternalId = $"action-{i}",
                    Status = ActionStatus.succeeded,
                });
            }

            await checkIn.Flush(source.Token);

            // 150 action updates, capped at 100 per checkin, must result in two checkin calls
            // beyond the initial startup-triggered one, with none dropped.
            Assert.Equal(150, actionUpdates.Count);
            Assert.True(_checkInCount >= 3, $"Expected at least 3 checkins, got {_checkInCount}");

            source.Cancel();
            await TestUtils.RunWithTimeout(runTask, 5);
        }

        [Fact]
        public async Task TestActionUpdateRequeuedOnFailedSend()
        {
            var (provider, checkIn) = GetCheckInWorker();
            using var p = provider;
            using var source = new CancellationTokenSource();

            var runTask = checkIn.RunPeriodicCheckIn(source.Token, new StartupRequest(), Timeout.InfiniteTimeSpan);
            await TestUtils.WaitForCondition(() => _checkInCount == 1, 5);

            checkIn.QueueActionUpdate(new ActionUpdate
            {
                ExternalId = "action-1",
                Status = ActionStatus.succeeded,
            });

            // The next checkin attempt fails transiently (simulating a network error, not a 400/404).
            _failCheckInsRemaining = 1;
            await checkIn.Flush(source.Token);

            // Nothing was received server-side, and the update must not have been lost.
            Assert.Empty(actionUpdates);

            // The next flush should succeed and include the requeued update.
            await checkIn.Flush(source.Token);
            Assert.Single(actionUpdates);
            Assert.Equal("action-1", (string)actionUpdates[0].externalId);

            source.Cancel();
            await TestUtils.RunWithTimeout(runTask, 5);
        }

        [Fact]
        public async Task TestActionUpdateWithUnknownExternalIdDropsOnlyThatUpdate()
        {
            // Regression test: when a checkin fails because odin reports a specific action
            // externalId as missing (e.g. a stale queue entry from before a restart), only that
            // action update should be dropped -- everything else in the batch (errors, task
            // updates, and any other action updates) must be requeued and retried, not discarded.
            var (provider, checkIn) = GetCheckInWorker();
            using var p = provider;
            using var source = new CancellationTokenSource();

            var runTask = checkIn.RunPeriodicCheckIn(source.Token, new StartupRequest(), Timeout.InfiniteTimeSpan);
            await TestUtils.WaitForCondition(() => _checkInCount == 1, 5);

            checkIn.QueueActionUpdate(new ActionUpdate
            {
                ExternalId = "bad-action",
                Status = ActionStatus.succeeded,
            });
            checkIn.QueueActionUpdate(new ActionUpdate
            {
                ExternalId = "good-action",
                Status = ActionStatus.succeeded,
            });
            checkIn.ReportTaskStart("task1", null, DateTime.UtcNow);

            _notFoundActionExternalId = "bad-action";
            await checkIn.Flush(source.Token);

            // Nothing was received server-side yet -- the whole request failed.
            Assert.Empty(actionUpdates);
            Assert.Empty(taskEvents);

            // The next flush should succeed, containing the requeued task update and the
            // still-valid action update, but never the one that was reported missing.
            await checkIn.Flush(source.Token);
            Assert.Single(actionUpdates);
            Assert.Equal("good-action", (string)actionUpdates[0].externalId);
            Assert.Single(taskEvents);

            source.Cancel();
            await TestUtils.RunWithTimeout(runTask, 5);
        }

        [Fact]
        public async Task TestFailedActionUpdateIsRequeuedBeforeNewerOnes()
        {
            // ActionUpdate has no timestamp field, so unlike errors/task updates (re-sorted by
            // time on every send regardless of queue order), action updates are always sent in
            // raw queue order. A stale update that failed to send must be retried *before* any
            // update queued after the failed attempt -- for the same action externalId, sending
            // them out of order could reverse their effective final state at the server. Matches
            // python-extractor-utils' checkin_worker.py, which prepends requeued action updates
            // for the same reason.
            //
            // Note: this only exercises the bug if the *newer* update is queued while the
            // failing send is still in flight, i.e. after "running" has already been drained out
            // of the worker's internal queue into the in-flight request, but before the failure
            // is observed and "running" is requeued. If "succeeded" were queued strictly after
            // Flush() had already returned, the internal queue would be empty at requeue time and
            // insert-at-front vs. append-at-end would be indistinguishable -- this is exactly the
            // gap in an earlier version of this test that let it pass against the bugged code.
            var (provider, checkIn) = GetCheckInWorker();
            using var p = provider;
            using var source = new CancellationTokenSource();

            var runTask = checkIn.RunPeriodicCheckIn(source.Token, new StartupRequest(), Timeout.InfiniteTimeSpan);
            await TestUtils.WaitForCondition(() => _checkInCount == 1, 5);

            checkIn.QueueActionUpdate(new ActionUpdate
            {
                ExternalId = "action-1",
                Status = ActionStatus.running,
                ResultMessage = "50% complete",
            });

            _failCheckInsRemaining = 1;
            _onCheckInFailureAboutToHappen = () => checkIn.QueueActionUpdate(new ActionUpdate
            {
                ExternalId = "action-1",
                Status = ActionStatus.succeeded,
                ResultMessage = "Done",
            });
            await checkIn.Flush(source.Token);
            Assert.Empty(actionUpdates);

            await checkIn.Flush(source.Token);

            // The stale "running" update must be sent first, not after "succeeded".
            Assert.Equal(2, actionUpdates.Count);
            Assert.Equal("running", (string)actionUpdates[0].status);
            Assert.Equal("succeeded", (string)actionUpdates[1].status);

            source.Cancel();
            await TestUtils.RunWithTimeout(runTask, 5);
        }

        [Fact]
        public async Task TestActionDispatcherInvokedWithPendingActions()
        {
            var (provider, checkIn) = GetCheckInWorker();
            using var p = provider;
            using var source = new CancellationTokenSource();

            List<IntegrationAction> received = null;
            checkIn.SetActionDispatcher(actions =>
            {
                received = actions.ToList();
                return Task.CompletedTask;
            });

            dynamic pendingAction = new ExpandoObject();
            pendingAction.externalId = "action-1";
            pendingAction.actionName = "Stop MyTask";
            pendingAction.status = "pending";
            pendingAction.createdTime = 1000L;
            pendingAction.lastUpdatedTime = 1000L;
            _pendingActionsToReturn.Add(pendingAction);

            var runTask = checkIn.RunPeriodicCheckIn(source.Token, new StartupRequest(), Timeout.InfiniteTimeSpan);

            // The startup response is the one carrying the pending action in this test, so the
            // dispatcher should be invoked before the loop even reaches its first periodic flush.
            await TestUtils.WaitForCondition(() => received != null, 5);

            Assert.NotNull(received);
            Assert.Single(received);
            Assert.Equal("action-1", received[0].ExternalId);
            Assert.Equal("Stop MyTask", received[0].ActionName);
            Assert.Equal(ActionStatus.pending, received[0].Status);

            source.Cancel();
            await TestUtils.RunWithTimeout(runTask, 5);
        }

        [Fact]
        public async Task TestActionDispatcherExceptionIsLoggedNotThrown()
        {
            var (provider, checkIn) = GetCheckInWorker();
            using var p = provider;
            using var source = new CancellationTokenSource();

            checkIn.SetActionDispatcher(actions => throw new InvalidOperationException("dispatcher blew up"));

            dynamic pendingAction = new ExpandoObject();
            pendingAction.externalId = "action-1";
            pendingAction.actionName = "Stop MyTask";
            pendingAction.status = "pending";
            pendingAction.createdTime = 1000L;
            pendingAction.lastUpdatedTime = 1000L;
            _pendingActionsToReturn.Add(pendingAction);

            // Should not throw, and should not prevent startup from completing.
            var runTask = checkIn.RunPeriodicCheckIn(source.Token, new StartupRequest(), Timeout.InfiniteTimeSpan);
            await TestUtils.WaitForCondition(() => _checkInCount == 1, 5);

            // The worker must still be able to check in normally afterwards.
            await checkIn.Flush(source.Token);
            Assert.Equal(2, _checkInCount);

            source.Cancel();
            await TestUtils.RunWithTimeout(runTask, 5);
        }

        [Fact]
        public async Task TestNoPendingActionsDoesNotInvokeDispatcher()
        {
            var (provider, checkIn) = GetCheckInWorker();
            using var p = provider;
            using var source = new CancellationTokenSource();

            var dispatcherCalled = false;
            checkIn.SetActionDispatcher(actions =>
            {
                dispatcherCalled = true;
                return Task.CompletedTask;
            });

            var runTask = checkIn.RunPeriodicCheckIn(source.Token, new StartupRequest(), Timeout.InfiniteTimeSpan);
            await TestUtils.WaitForCondition(() => _checkInCount == 1, 5);
            await checkIn.Flush(source.Token);

            Assert.False(dispatcherCalled);

            source.Cancel();
            await TestUtils.RunWithTimeout(runTask, 5);
        }

        [Fact]
        public async Task TestPendingActionsWithNoDispatcherRegisteredDoesNotThrow()
        {
            // No SetActionDispatcher call at all -- must not throw, just log and move on.
            var (provider, checkIn) = GetCheckInWorker();
            using var p = provider;
            using var source = new CancellationTokenSource();

            dynamic pendingAction = new ExpandoObject();
            pendingAction.externalId = "action-1";
            pendingAction.actionName = "Stop MyTask";
            pendingAction.status = "pending";
            pendingAction.createdTime = 1000L;
            pendingAction.lastUpdatedTime = 1000L;
            _pendingActionsToReturn.Add(pendingAction);

            var runTask = checkIn.RunPeriodicCheckIn(source.Token, new StartupRequest(), Timeout.InfiniteTimeSpan);
            await TestUtils.WaitForCondition(() => _checkInCount == 1, 5);

            source.Cancel();
            await TestUtils.RunWithTimeout(runTask, 5);
        }

        private async Task<HttpResponseMessage> mockCheckInAsync(
            HttpRequestMessage message,
            CancellationToken token
        )
        {
            var uri = message.RequestUri.ToString();
            if (uri == "http://example.url/token")
            {
                var reply = "{" + Environment.NewLine +
                       $"  \"token_type\": \"Bearer\",{Environment.NewLine}" +
                       $"  \"expires_in\": 2,{Environment.NewLine}" +
                       $"  \"access_token\": \"token\"{Environment.NewLine}" +
                        "}";
                // Return 200
                var response = new HttpResponseMessage
                {
                    StatusCode = HttpStatusCode.OK,
                    Content = new StringContent(reply)
                };

                return response;
            }
            else if (uri.Contains("/checkin"))
            {
                var content = await message.Content.ReadAsStringAsync(token);
                _output.WriteLine(content);

                if (_failCheckInsRemaining > 0)
                {
                    _failCheckInsRemaining--;
                    var callback = _onCheckInFailureAboutToHappen;
                    _onCheckInFailureAboutToHappen = null;
                    callback?.Invoke();
                    throw new HttpRequestException("Simulated transient failure");
                }

                if (_notFoundActionExternalId != null)
                {
                    var badId = _notFoundActionExternalId;
                    _notFoundActionExternalId = null;
                    var errorBody = JsonConvert.SerializeObject(new
                    {
                        error = new
                        {
                            code = 404,
                            message = "One or more actions not found",
                            missing = new[] { new { externalId = badId } },
                        }
                    });
                    var notFoundResponse = new HttpResponseMessage
                    {
                        StatusCode = HttpStatusCode.NotFound,
                        Content = new StringContent(errorBody)
                    };
                    notFoundResponse.Content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
                    return notFoundResponse;
                }

                var data = JsonConvert.DeserializeObject<dynamic>(content);
                Assert.Equal("test-integration", (string)data.externalId);

                if (data.taskEvents != null)
                {
                    taskEvents.AddRange(data.taskEvents);
                }
                if (data.errors != null)
                {
                    errors.AddRange(data.errors);
                }
                if (data.actionUpdates != null)
                {
                    actionUpdates.AddRange(data.actionUpdates);
                }
                _checkInCount++;
            }
            else
            {
                Assert.Contains("/startup", uri);
                var content = await message.Content.ReadAsStringAsync(token);
                _output.WriteLine(content);
                var data = JsonConvert.DeserializeObject<dynamic>(content);
                Assert.Equal("test-integration", (string)data.externalId);

                startupRequests.Add(data);

                _startupCount++;
            }

            dynamic resData = new ExpandoObject();
            resData.lastConfigRevision = _lastConfigRevision;
            resData.externalId = "test-integration";
            // pendingActions is only populated for the *next* mocked response after a test sets
            // _pendingActionsToReturn, then cleared -- mirroring how a real server would only
            // include truly-pending actions once, not repeat them on every subsequent checkin.
            if (_pendingActionsToReturn.Count > 0)
            {
                resData.pendingActions = _pendingActionsToReturn;
                _pendingActionsToReturn = new List<dynamic>();
            }
            var resBody = JsonConvert.SerializeObject(resData);
            var fresponse = new HttpResponseMessage
            {
                StatusCode = HttpStatusCode.OK,
                Content = new StringContent(resBody)
            };
            fresponse.Content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
            fresponse.Headers.Add("x-request-id", "1");

            return fresponse;
        }

        [Fact]
        public async Task TestErrorSortingAndDeduplication()
        {
            var (provider, checkIn) = GetCheckInWorker();
            using var p = provider;
            using var source = new CancellationTokenSource();

            var runTask = checkIn.RunPeriodicCheckIn(source.Token, new StartupRequest(), Timeout.InfiniteTimeSpan);
            await TestUtils.WaitForCondition(() => _checkInCount == 1, 5);

            var start = DateTime.UtcNow;

            // Test 1: Errors with different timestamps should be sorted correctly
            // Create errors in reverse chronological order, they should be sorted properly
            checkIn.ReportError(new ExtractorError(ErrorLevel.error, "late-error", checkIn, now: start.AddSeconds(30)));
            checkIn.ReportError(new ExtractorError(ErrorLevel.warning, "middle-error", checkIn, now: start.AddSeconds(15)));
            checkIn.ReportError(new ExtractorError(ErrorLevel.error, "early-error", checkIn, now: start.AddSeconds(5)));

            errors.Clear();
            taskEvents.Clear();

            await checkIn.Flush(source.Token);

            // Verify sorting by time
            Assert.Equal(3, errors.Count);
            Assert.Equal("early-error", (string)errors[0].description); // Time 5
            Assert.Equal("middle-error", (string)errors[1].description); // Time 15
            Assert.Equal("late-error", (string)errors[2].description); // Time 30

            // Test 3: Verify errors with EndTime are sorted by EndTime
            errors.Clear();
            var error1 = new ExtractorError(ErrorLevel.warning, "error-1", checkIn, now: start.AddSeconds(10));
            var error2 = new ExtractorError(ErrorLevel.warning, "error-2", checkIn, now: start.AddSeconds(20));
            var ongoingError = new ExtractorError(ErrorLevel.error, "ongoing-error", checkIn, now: start.AddSeconds(15));

            checkIn.ReportError(error1);
            checkIn.ReportError(error2);

            // Finish error2 before error1, so EndTime order is reversed from StartTime order
            error2.Finish(start.AddSeconds(25));
            error1.Finish(start.AddSeconds(40));

            await checkIn.Flush(source.Token);

            // Should be sorted by EndTime (25, then 40)
            Assert.Equal(3, errors.Count);
            Assert.Equal("ongoing-error", (string)errors[0].description); // EndTime null
            Assert.Equal("error-2", (string)errors[1].description); // EndTime 25
            Assert.Equal("error-1", (string)errors[2].description); // EndTime 40

            // Test 4: Verify truncation of long descriptions/details still works
            errors.Clear();
            var longDescription = new string('a', 6000);
            var longDetails = new string('b', 6000);

            checkIn.ReportError(new ExtractorError(ErrorLevel.error, longDescription, checkIn, longDetails, now: start));

            await checkIn.Flush(source.Token);

            Assert.Single(errors);
            Assert.Equal(5000, ((string)errors[0].description).Length);
            Assert.Equal(5000, ((string)errors[0].details).Length);
        }
    }
}
