using System;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text.Json;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Xunit;
using Xunit.Abstractions;
using Cognite.Extractor.Utils;
using Cognite.Extractor.Testing;
using Cognite.Extractor.Common;
using CogniteSdk;
using System.Linq;
using Cognite.Extractor.Testing.Mock;
using Moq;

namespace ExtractorUtils.Test.Unit
{
    public class RawHighAvailabilityTest
    {
        private const string _authTenant = "someTenant";

        private const string _project = "someProject";

        private const string _host = "https://test.cognitedata.com";

        private const string _dbName = "testDb";

        private const string _tableName = "testTable";

        private readonly ITestOutputHelper _output;

        public RawHighAvailabilityTest(ITestOutputHelper output)
        {
            _output = output;
        }

        [Fact]
        public async Task TestUploadLogToState()
        {
            int index = 0;
            string path = "test-upload-log-to-state-config";
            SetupConfig(index, path);

            var services = new ServiceCollection();
            services.AddConfig<MyTestConfig>(path, 2);
            services.AddTestLogging(_output);
            CdfMock.RegisterHttpClient(services);
            services.AddCogniteClient("testApp");

            using (var provider = services.BuildServiceProvider())
            {
                var mock = provider.GetRequiredService<CdfMock>();
                var raw = new RawMock();
                var insertMatcher = raw.CreateRawRowsMatcher(Times.Exactly(3));
                mock.AddMatcher(insertMatcher);

                var logger = provider.GetRequiredService<ILogger<RawHighAvailabilityTest>>();

                var extractorManager = CreateRawExtractorManager(provider);

                await extractorManager.UploadLogToState();

                // Checking that the initial log has been inserted into db.
                var table = raw.GetTable(_dbName, _tableName);
                Assert.Single(table.Rows);
                var row = table.GetRow<RawLogData>(index.ToString());
                Assert.False(row.Columns.Active);
                Assert.True(row.Columns.TimeStamp < DateTime.UtcNow);

                // Updating the status.
                DateTime prevTimeStamp = row.Columns.TimeStamp;
                extractorManager._state.UpdatedStatus = true;

                await extractorManager.UploadLogToState();

                // Testing that the status has been changed.
                Assert.Single(table.Rows);
                row = table.GetRow<RawLogData>(index.ToString());
                Assert.True(row.Columns.Active);
                Assert.True(row.Columns.TimeStamp > prevTimeStamp);

                // Testing making the endpoint return an error.
                insertMatcher.ForceErrorStatus = 400;
                prevTimeStamp = row.Columns.TimeStamp;
                extractorManager._state.UpdatedStatus = false;

                await extractorManager.UploadLogToState();

                // Checking that the db remains unchanged after the error.
                row = table.GetRow<RawLogData>(index.ToString());
                Assert.True(row.Columns.Active);
                Assert.True(row.Columns.TimeStamp == prevTimeStamp);
            }

            System.IO.File.Delete(path);
        }

        [Fact]
        public async Task TestUpdateExtractorState()
        {
            int index = 0;
            string path = "test-update-extractor-state-config";
            SetupConfig(index, path);

            var services = new ServiceCollection();
            services.AddConfig<MyTestConfig>(path, 2);
            services.AddTestLogging(_output);
            CdfMock.RegisterHttpClient(services);
            services.AddCogniteClient("testApp");

            using (var provider = services.BuildServiceProvider())
            {
                var mock = provider.GetRequiredService<CdfMock>();
                var raw = new RawMock();
                var getMatcher = raw.GetRawRowsMatcher(Times.AtLeastOnce());
                mock.AddMatcher(getMatcher);
                var insertMatcher = raw.CreateRawRowsMatcher(Times.AtLeastOnce());
                mock.AddMatcher(insertMatcher);

                var logger = provider.GetRequiredService<ILogger<RawHighAvailabilityTest>>();

                var extractorManager = CreateRawExtractorManager(provider);

                var table = raw.GetOrCreateTable(_dbName, _tableName);

                table.Add("0", new RawLogData(DateTime.UtcNow, true));
                table.Add("1", new RawLogData(DateTime.UtcNow.Subtract(new TimeSpan(0, 0, 10)), false));
                table.Add("2", new RawLogData(DateTime.UtcNow.Subtract(new TimeSpan(0, 0, 30)), false));
                table.Add("3", new RawLogData(DateTime.UtcNow.Subtract(new TimeSpan(0, 0, 50)), false));

                Assert.True(extractorManager._state.CurrentState.Count == 0);

                await extractorManager.UploadLogToState();
                await extractorManager.UpdateExtractorState();

                // Testing that the state has changed.
                Assert.Equal(4, extractorManager._state.CurrentState.Count);

                // Checking that each value in the state is the same as in the db.
                foreach (RawExtractorInstance instance in extractorManager._state.CurrentState.Values)
                {
                    string key = instance.Index.ToString();
                    if (table.Rows.ContainsKey(key) && key != "0")
                    {
                        var row = table.GetRow<RawLogData>(key);
                        Assert.True(row.Columns.Active == instance.Active);
                        Assert.True(row.Columns.TimeStamp == instance.TimeStamp);
                    }
                }

                // Testing updating the active status and timestamp for a given extractor.
                string testKey = "1";
                table.Add(testKey, new RawLogData(DateTime.UtcNow, true));

                await extractorManager.UploadLogToState();
                await extractorManager.UpdateExtractorState();

                foreach (RawExtractorInstance instance in extractorManager._state.CurrentState.Values)
                {
                    if (instance.Index == short.Parse(testKey))
                    {
                        var row = table.GetRow<RawLogData>(testKey);
                        // Checking that the valus has been changed for the given extractor.
                        Assert.True(row.Columns.Active == instance.Active);
                        Assert.True(row.Columns.TimeStamp == instance.TimeStamp);
                    }
                }

                // Testing removing an extractor after it has been initialized.
                // If an extractor has been initialized but then returns an empty
                // row in the state it will use the last seen log.
                testKey = "2";
                RawLogData logCopy = table.GetRow<RawLogData>(testKey).Columns;
                table.Rows.Remove(testKey);

                await extractorManager.UploadLogToState();
                await extractorManager.UpdateExtractorState();

                // Checking that the state still has all the rows.
                Assert.True(extractorManager._state.CurrentState.Count == 4);

                foreach (RawExtractorInstance instance in extractorManager._state.CurrentState.Values)
                {
                    if (instance.Index == short.Parse(testKey))
                    {
                        // Checking that the previous value from the state is reused.
                        Assert.True(logCopy.Active == instance.Active);
                        Assert.True(logCopy.TimeStamp == instance.TimeStamp);
                    }
                }

                // Inserting the removed extractor back and checking that the new value is used again.
                table.Add(testKey, new RawLogData(DateTime.UtcNow, false));

                await extractorManager.UploadLogToState();
                await extractorManager.UpdateExtractorState();

                foreach (RawExtractorInstance instance in extractorManager._state.CurrentState.Values)
                {
                    if (instance.Index == short.Parse(testKey))
                    {
                        var row = table.GetRow<RawLogData>(testKey);
                        // Checking that the removed value has been replaced.
                        Assert.True(row.Columns.Active == instance.Active);
                        Assert.True(row.Columns.TimeStamp == instance.TimeStamp);

                    }
                }

                // Testing making the endpoint return an error.
                getMatcher.ForceErrorStatus = 400;
                testKey = "3";
                table.Add(testKey, new RawLogData(DateTime.UtcNow, !table.GetRow<RawLogData>(testKey).Columns.Active));

                await extractorManager.UploadLogToState();
                await extractorManager.UpdateExtractorState();

                foreach (RawExtractorInstance instance in extractorManager._state.CurrentState.Values)
                {
                    if (instance.Index == short.Parse(testKey))
                    {
                        var row = table.GetRow<RawLogData>(testKey);
                        // Checking that if the endpoint fails the current state will remain unchanged.
                        Assert.False(row.Columns.Active == instance.Active);
                        Assert.False(row.Columns.TimeStamp == instance.TimeStamp);
                    }
                }
            }

            System.IO.File.Delete(path);
        }

        [Fact]
        public void TestCheckForMultipleActiveExtractors()
        {
            int index = 1;
            string path = "test-multiple-active-extractors-config";
            SetupConfig(index, path);

            var services = new ServiceCollection();
            services.AddConfig<MyTestConfig>(path, 2);
            services.AddTestLogging(_output);
            services.AddCogniteClient("testApp");

            using (var provider = services.BuildServiceProvider())
            {
                var logger = provider.GetRequiredService<ILogger<RawHighAvailabilityTest>>();

                var source1 = new CancellationTokenSource();
                var extractorManager = CreateRawExtractorManager(provider, source1);

                List<IExtractorInstance> extractorInstances = new List<IExtractorInstance>();
                extractorInstances.Add(new RawExtractorInstance(0, DateTime.UtcNow, true));
                extractorInstances.Add(new RawExtractorInstance(1, DateTime.UtcNow, true));
                extractorInstances.Add(new RawExtractorInstance(2, DateTime.UtcNow.Subtract(new TimeSpan(0, 0, 50)), false));

                extractorManager._state.CurrentState = extractorInstances.ToDictionary(ex => ex.Index);
                extractorManager.CheckForMultipleActiveExtractors();

                // Extractor 1 will be cancelled because both 0 and 1 are active, where 0 has higher priority.
                Assert.True(source1.IsCancellationRequested);

                var source2 = new CancellationTokenSource();
                extractorManager = CreateRawExtractorManager(provider, source2);

                extractorInstances.Clear();
                extractorInstances.Add(new RawExtractorInstance(0, DateTime.UtcNow, true));
                extractorInstances.Add(new RawExtractorInstance(1, DateTime.UtcNow.Subtract(new TimeSpan(0, 0, 20)), true));
                extractorInstances.Add(new RawExtractorInstance(2, DateTime.UtcNow.Subtract(new TimeSpan(0, 0, 50)), true));

                extractorManager._state.CurrentState = extractorInstances.ToDictionary(ex => ex.Index);
                extractorManager.CheckForMultipleActiveExtractors();

                // Extractor 1 will not be cancelled because it is not active.
                Assert.False(source2.IsCancellationRequested);

                var source3 = new CancellationTokenSource();
                extractorManager = CreateRawExtractorManager(provider, source3);

                extractorInstances.Clear();
                extractorInstances.Add(new RawExtractorInstance(0, DateTime.UtcNow, true));
                extractorInstances.Add(new RawExtractorInstance(1, DateTime.UtcNow, true));
                extractorInstances.Add(new RawExtractorInstance(2, DateTime.UtcNow, true));

                extractorManager._state.CurrentState = extractorInstances.ToDictionary(ex => ex.Index);
                extractorManager.CheckForMultipleActiveExtractors();

                // Extractor 1 will be cancelled because there are multiple active extractors and 0 has higher priority.
                Assert.True(source3.IsCancellationRequested);

                var source4 = new CancellationTokenSource();
                extractorManager = CreateRawExtractorManager(provider, source4);

                extractorInstances.Clear();
                extractorInstances.Add(new RawExtractorInstance(1, DateTime.UtcNow, true));
                extractorInstances.Add(new RawExtractorInstance(2, DateTime.UtcNow, true));
                extractorInstances.Add(new RawExtractorInstance(3, DateTime.UtcNow, true));

                extractorManager._state.CurrentState = extractorInstances.ToDictionary(ex => ex.Index);
                extractorManager.CheckForMultipleActiveExtractors();

                // Extractor 1 will not be cancelled because it has higher priority than 2 and 3.
                Assert.False(source4.IsCancellationRequested);
            }

            System.IO.File.Delete(path);
        }

        [Fact]
        public void TestShouldBecomeActive()
        {
            int index = 1;
            string path = "test-should-become-active-config";
            SetupConfig(index, path);

            var services = new ServiceCollection();
            services.AddConfig<MyTestConfig>(path, 2);
            services.AddTestLogging(_output);
            services.AddCogniteClient("testApp");

            using (var provider = services.BuildServiceProvider())
            {
                var logger = provider.GetRequiredService<ILogger<RawHighAvailabilityTest>>();

                var extractorManager = CreateRawExtractorManager(provider);

                List<IExtractorInstance> extractorInstances = new List<IExtractorInstance>();
                extractorInstances.Add(new RawExtractorInstance(1, DateTime.UtcNow, false));
                extractorInstances.Add(new RawExtractorInstance(2, DateTime.UtcNow.Subtract(new TimeSpan(0, 0, 30)), false));
                extractorInstances.Add(new RawExtractorInstance(3, DateTime.UtcNow.Subtract(new TimeSpan(0, 0, 30)), false));
                extractorManager._state.CurrentState = extractorInstances.ToDictionary(ex => ex.Index);

                // Extractor 1 will become active because 2 and 3 are not within the time threshold.
                Assert.True(extractorManager.ShouldBecomeActive());

                extractorInstances.Clear();
                extractorInstances.Add(new RawExtractorInstance(1, DateTime.UtcNow, false));
                extractorInstances.Add(new RawExtractorInstance(2, DateTime.UtcNow, true));
                extractorInstances.Add(new RawExtractorInstance(3, DateTime.UtcNow.Subtract(new TimeSpan(0, 0, 30)), false));
                extractorManager._state.CurrentState = extractorInstances.ToDictionary(ex => ex.Index);

                // Extractor 1 will not become active because 2 is already active and within the time threshold.
                Assert.False(extractorManager.ShouldBecomeActive());

                extractorInstances.Clear();
                extractorInstances.Add(new RawExtractorInstance(1, DateTime.UtcNow, false));
                extractorInstances.Add(new RawExtractorInstance(2, DateTime.UtcNow.Subtract(new TimeSpan(0, 0, 30)), true));
                extractorInstances.Add(new RawExtractorInstance(3, DateTime.UtcNow.Subtract(new TimeSpan(0, 0, 30)), true));
                extractorManager._state.CurrentState = extractorInstances.ToDictionary(ex => ex.Index);

                // Extractor 1 will become active because 2 and 3 are over the time threshold.
                Assert.True(extractorManager.ShouldBecomeActive());

                extractorInstances.Clear();
                extractorInstances.Add(new RawExtractorInstance(0, DateTime.UtcNow, false));
                extractorInstances.Add(new RawExtractorInstance(1, DateTime.UtcNow, false));
                extractorInstances.Add(new RawExtractorInstance(2, DateTime.UtcNow.Subtract(new TimeSpan(0, 0, 30)), true));
                extractorManager._state.CurrentState = extractorInstances.ToDictionary(ex => ex.Index);

                // Extractor 1 will not become active because 0 has higher priority.
                Assert.False(extractorManager.ShouldBecomeActive());

                extractorInstances.Clear();
                extractorManager._state.CurrentState = extractorInstances.ToDictionary(ex => ex.Index);

                // Extractor 1 will not become active because the state is empty.
                Assert.False(extractorManager.ShouldBecomeActive());
            }

            System.IO.File.Delete(path);
        }

        private void SetupConfig(int index, string path)
        {
            string[] config = {
                    "version: 2",
                    "logger:",
                    "  console:",
                    "    level: verbose",
                    "cognite:",
                    $"  project: {_project}",
                    $"  host: {_host}",
                    "high-availability:",
                    $"  index: {index}",
                    $"  raw:",
                    $"    database-name: {_dbName}",
                    $"    table-name: {_tableName}"};
            System.IO.File.WriteAllLines(path, config);
        }

        private RawHighAvailabilityManager CreateRawExtractorManager(ServiceProvider provider, CancellationTokenSource source = null)
        {
            var managerConfig = provider.GetRequiredService<HighAvailabilityConfig>();
            var destination = provider.GetRequiredService<CogniteDestination>();
            var logger = provider.GetRequiredService<ILogger<RawHighAvailabilityManager>>();
            if (source == null) source = new CancellationTokenSource();
            var scheduler = new PeriodicScheduler(source.Token);
            var inactivityThreshold = new TimeSpan(0, 0, 10);

            RawHighAvailabilityManager extractorManager = new RawHighAvailabilityManager(managerConfig, destination, logger, scheduler, source, inactivityThreshold: inactivityThreshold);

            return extractorManager;
        }
    }

    class MyTestConfig : BaseConfig
    {
        public HighAvailabilityConfig HighAvailability { get; set; }
    }
}
