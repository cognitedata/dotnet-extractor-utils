using Cognite.Extensions;
using Cognite.Extractor.Utils;
using CogniteSdk;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace ExtractorUtils.Test.Integration
{
    /// <summary>
    /// End-to-end integration tests for the Charon REST-poll pipeline.
    ///
    /// Required env vars (pointing at the az-arn-dev-002 cluster where Charon is deployed):
    ///   CHARON_TEST_PROJECT    CDF project name
    ///   CHARON_TEST_HOST       CDF base URL
    ///   CHARON_TEST_SCOPE      OAuth2 scope
    ///   CHARON_TEST_CLIENT_ID  Service principal client ID
    ///   CHARON_TEST_TENANT     AAD tenant
    ///   CHARON_TEST_SECRET     Client secret
    /// </summary>
    public class CharonIntegrationTest
    {
        private readonly ITestOutputHelper _output;

        public CharonIntegrationTest(ITestOutputHelper output)
        {
            _output = output;
        }

        // ---------------------------------------------------------------------------
        // Helpers
        // ---------------------------------------------------------------------------

        /// <summary>
        /// Poll /jobs/logs for <paramref name="jobExternalId"/> until type="ok" or a
        /// failure type is seen, with a 15s × 40-attempt ceiling (10-minute total).
        /// The 5-minute REST-poll floor means we need at least two cycles to be safe.
        /// </summary>
        private async Task PollJobLogsUntilOk(
            CDFTester tester,
            string jobExternalId,
            CancellationToken token)
        {
            var auth = tester.Provider.GetRequiredService<IAuthenticator>();
            var httpFactory = tester.Provider.GetRequiredService<IHttpClientFactory>();
            var baseUrl = tester.Host.TrimEnd('/');
            var plutoUrl = $"{baseUrl}/api/v1/projects/{tester.Project}/pluto/jobs/logs";

            string? lastType = null;

            for (int attempt = 0; attempt < 40; attempt++)
            {
                var bearerToken = await auth.GetToken(token);
                using var req = new HttpRequestMessage(
                    HttpMethod.Get,
                    $"{plutoUrl}?job={Uri.EscapeDataString(jobExternalId)}&limit=5");
                req.Headers.Authorization = new AuthenticationHeaderValue("Bearer", bearerToken);

                var client = httpFactory.CreateClient("default");
                using var resp = await client.SendAsync(req, token);
                var body = await resp.Content.ReadAsStringAsync();
                var doc = JsonDocument.Parse(body);
                var items = doc.RootElement.GetProperty("items");

                if (items.GetArrayLength() > 0)
                {
                    lastType = items[0].GetProperty("type").GetString();
                    _output.WriteLine($"  attempt {attempt + 1}/40: type={lastType}");
                    if (lastType == "ok") return;
                    if (lastType == "transform_error" || lastType == "cdf_write_error")
                    {
                        Assert.Fail(
                            $"Charon job failed with {lastType}:\n" +
                            JsonSerializer.Serialize(
                                items[0],
                                new JsonSerializerOptions { WriteIndented = true }));
                    }
                }
                else
                {
                    _output.WriteLine($"  attempt {attempt + 1}/40: no log entries yet");
                }

                await Task.Delay(15_000, token);
            }

            Assert.Fail($"Timed out after 40 × 15s waiting for job 'ok'. last type: {lastType ?? "none"}");
        }

        // ---------------------------------------------------------------------------
        // Tests
        // ---------------------------------------------------------------------------

        [Fact]
        public async Task TestCharonUploadQueue()
        {
            using var tester = new CDFTester(CogniteHost.Charon, _output);

            var tsExternalId = $"{tester.Prefix} charon-queue-ts";
            var timeseries = new[] { new TimeSeriesCreate { ExternalId = tsExternalId } };

            _output.WriteLine($"project:  {tester.Project}");
            _output.WriteLine($"ts id:    {tsExternalId}");

            int dpCount = 0;
            int cbCount = 0;
            var startMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

            try
            {
                // Ensure the timeseries exists in CDF so Pluto can write to it.
                var ensureResult = await tester.Destination.EnsureTimeSeriesExistsAsync(
                    timeseries, RetryMode.OnError, SanitationMode.Remove, tester.Source.Token);
                Assert.True(ensureResult.IsAllGood, "Failed to ensure test timeseries exists");

                // Resolve ICharonClient from DI — registered automatically by AddCogniteClient
                // when CogniteConfig.Charon.Enabled = true.
                var charonClient = tester.Provider.GetService<ICharonClient>();
                Assert.NotNull(charonClient);

                using var queue = tester.Destination.CreateTimeSeriesUploadQueue(
                    TimeSpan.Zero, 0,
                    res =>
                    {
                        dpCount += res.Uploaded?.Count() ?? 0;
                        cbCount++;
                        _output.WriteLine($"  callback: uploaded {res.Uploaded?.Count() ?? 0} dps (total {dpCount})");
                        return Task.CompletedTask;
                    },
                    charon: charonClient);

                // Enqueue 10 datapoints with distinct 1-second-apart timestamps.
                // Using DateTime.UtcNow in a tight loop collapses to a few unique ms values;
                // CDF deduplicates by timestamp so we must space them out explicitly.
                var baseTime = DateTime.UtcNow;
                for (int i = 0; i < 10; i++)
                {
                    queue.Enqueue(
                        Identity.Create(tsExternalId),
                        new Datapoint(baseTime.AddSeconds(i), (double)i));
                }

                // Trigger an explicit upload — this is synchronous from the test's perspective,
                // waits for /setup (lazy) + /insert_payload to complete before returning.
                var triggerResult = await queue.Trigger(tester.Source.Token);
                dpCount += triggerResult.Uploaded?.Count() ?? 0;
                cbCount++;
                _output.WriteLine($"  trigger: uploaded {triggerResult.Uploaded?.Count() ?? 0} dps");

                Assert.Equal(10, dpCount);
                Assert.True(cbCount >= 1);

                // job_external_id is stored on the client after /setup.
                var jobExternalId = charonClient.JobExternalId;
                Assert.NotNull(jobExternalId);
                _output.WriteLine($"polling /jobs/logs for {jobExternalId} ...");

                await PollJobLogsUntilOk(tester, jobExternalId!, tester.Source.Token);

                // Verify datapoints landed in CDF via Pluto.
                await Task.Delay(2000, tester.Source.Token);
                var dps = await tester.Destination.CogniteClient.DataPoints.ListAsync(
                    new DataPointsQuery
                    {
                        Start = (startMs - 5000).ToString(),
                        End = (startMs + 20000).ToString(),
                        Items = new[]
                        {
                            new DataPointsQueryItem
                            {
                                ExternalId = tsExternalId,
                                Limit = 20
                            }
                        }
                    }, tester.Source.Token);

                Assert.NotNull(dps);
                Assert.NotEmpty(dps.Items);
                var item = dps.Items.First();
                Assert.NotNull(item.NumericDatapoints);
                Assert.Equal(10, item.NumericDatapoints.Datapoints.Count);
                _output.WriteLine(
                    $"  confirmed {item.NumericDatapoints.Datapoints.Count} datapoints in CDF via Charon ✓");
            }
            finally
            {
                await tester.Destination.CogniteClient.TimeSeries.DeleteAsync(
                    new TimeSeriesDelete
                    {
                        IgnoreUnknownIds = true,
                        Items = new[] { Identity.Create(tsExternalId) }
                    }, tester.Source.Token);
            }
        }

        [Fact]
        public async Task TestCharonSetupIsIdempotent()
        {
            using var tester = new CDFTester(CogniteHost.Charon, _output);

            var charonClient = tester.Provider.GetService<ICharonClient>();
            Assert.NotNull(charonClient);

            // Call setup twice — both must succeed without error.
            await charonClient.SetupAsync(tester.Source.Token);
            var jobId1 = charonClient.JobExternalId;
            _output.WriteLine($"  first /setup: OK (job={jobId1})");

            await charonClient.SetupAsync(tester.Source.Token);
            var jobId2 = charonClient.JobExternalId;
            _output.WriteLine($"  second /setup (idempotency): OK (job={jobId2}) ✓");

            // Both calls must return the same job external ID.
            Assert.Equal(jobId1, jobId2);
        }

        [Fact]
        public void TestCharonDisabledFallsBackToCDF()
        {
            // Build services inline with charon.enabled: false — no real CDF credentials needed.
            string path = $"charon-disabled-test-{Guid.NewGuid()}.yml";
            string[] lines = {
                "version: 2",
                "cognite:",
                "  project: test-project",
                "  host: https://example.cognitedata.com",
                "  charon:",
                "    enabled: false"
            };
            System.IO.File.WriteAllLines(path, lines);
            try
            {
                using var tester = new CDFTester(lines, _output);

                // ICharonClient must be null when Charon is explicitly disabled.
                var charonClient = tester.Provider.GetService<ICharonClient>();
                Assert.Null(charonClient);
                _output.WriteLine("  ICharonClient is null when charon.enabled: false ✓");

                // The queue factory returns a plain TimeSeriesUploadQueue (no Charon routing).
                using var queue = tester.Destination.CreateTimeSeriesUploadQueue(
                    TimeSpan.Zero, 0, null, charon: charonClient);
                Assert.IsType<TimeSeriesUploadQueue>(queue);
                _output.WriteLine("  CreateTimeSeriesUploadQueue returns plain TimeSeriesUploadQueue ✓");
            }
            finally
            {
                System.IO.File.Delete(path);
            }
        }
    }
}
