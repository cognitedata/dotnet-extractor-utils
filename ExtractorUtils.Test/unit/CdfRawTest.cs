using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;
using Cognite.Extractor.Utils;
using Xunit.Abstractions;
using Cognite.Extractor.Testing;
using Cognite.Extractor.Testing.Mock;

namespace ExtractorUtils.Test.Unit
{
    public class CdfRawTest
    {
        private const string _project = "someProject";
        private const string _host = "https://test.cognitedata.com";
        private const string _dbName = "testDb";
        private const string _tableName = "testTable";

        private readonly ITestOutputHelper _output;
        public CdfRawTest(ITestOutputHelper output)
        {
            _output = output;
        }

        [Fact]
        public async Task TestInsertRow()
        {
            string path = "test-insert-raw-rows-config.yml";
            string[] lines = {  "version: 2",
                                "logger:",
                                "  console:",
                                "    level: verbose",
                                "cognite:",
                               $"  project: {_project}",
                               $"  host: {_host}",
                                "  cdf-chunking:",
                                "    raw-rows: 4",
                                "  cdf-throttling:",
                                "    raw: 2" };
            System.IO.File.WriteAllLines(path, lines);

            // Setup services
            var services = new ServiceCollection();
            services.AddConfig<BaseConfig>(path, 2);
            services.AddTestLogging(_output);
            CdfMock.RegisterHttpClient(services);
            services.AddCogniteClient("testApp");
            using (var provider = services.BuildServiceProvider())
            {
                var mock = provider.GetRequiredService<CdfMock>();
                var raw = new RawMock();
                // Expect that the endpoint is called 2 times (2 chunks of max 4 rows)
                mock.AddMatcher(raw.CreateRawRowsMatcher(Times.Exactly(2)));
                var cogniteDestination = provider.GetRequiredService<CogniteDestination>();

                var rows = new Dictionary<string, TestDto>{
                    { "A", new TestDto{ Name = "A", Number = 0} },
                    { "B", new TestDto{ Name = "B", Number = 1} },
                    { "C", new TestDto{ Name = "C", Number = 2} },
                    { "D", new TestDto{ Name = "D", Number = 3} },
                    { "E", new TestDto{ Name = "E", Number = 4} },
                    { "F", new TestDto{ Name = "F", Number = 5} }
                };

                await cogniteDestination.InsertRawRowsAsync(_dbName, _tableName, rows, CancellationToken.None);

                Assert.Single(raw.Databases);
                var table = raw.Databases[(_dbName, _tableName)];
                foreach (var kvp in rows)
                {
                    Assert.True(table.TryGetValue(kvp.Key, out var dto));
                    Assert.Equal(kvp.Value.Name, dto.Columns["name"].GetValue<string>());
                    Assert.Equal(kvp.Value.Number, dto.Columns["number"].GetValue<int>());
                }
            }

            System.IO.File.Delete(path);
        }

        [Fact]
        public async Task TestUploadQueue()
        {
            string path = "test-raw-queue-config.yml";
            string[] lines = {  "version: 2",
                                "logger:",
                                "  console:",
                                "    level: verbose",
                                "cognite:",
                               $"  project: {_project}",
                               $"  host: {_host}" };
            System.IO.File.WriteAllLines(path, lines);

            var services = new ServiceCollection();
            services.AddConfig<BaseConfig>(path, 2);
            services.AddTestLogging(_output);
            CdfMock.RegisterHttpClient(services);
            services.AddCogniteClient("testApp");

            var raw = new RawMock();

            var index = 0;
            using (var source = new CancellationTokenSource())
            using (var provider = services.BuildServiceProvider())
            {
                var mock = provider.GetRequiredService<CdfMock>();
                var rawRowsEndpoint = raw.CreateRawRowsMatcher(Times.AtMost(8));
                mock.AddMatcher(rawRowsEndpoint); // expect at most 8 calls to the raw rows endpoint

                var cogniteDestination = provider.GetRequiredService<CogniteDestination>();
                var logger = provider.GetRequiredService<ILogger<CdfRawTest>>();
                // queue with 1 sec upload interval
                await using (var queue = cogniteDestination.CreateRawUploadQueue<TestDto>(_dbName, _tableName, TimeSpan.FromSeconds(1)))
                {
                    var enqueueTask = Task.Run(async () =>
                    {
                        while (index < 13)
                        {
                            queue.EnqueueRow($"r{index}", new TestDto { Name = "Test", Number = index });
                            await Task.Delay(100, source.Token);
                            index++;
                        }
                    });
                    var uploadTask = queue.Start(source.Token);

                    // wait for either the enqueue task to finish or the upload task to fail
                    var t = Task.WhenAny(uploadTask, enqueueTask);
                    await t;
                    logger.LogInformation("Enqueueing task completed. Disposing of the upload queue");
                } // disposing the queue will upload any rows left and stop the upload loop
                logger.LogInformation("Upload queue disposed");

                // Verify that the endpoint was called at most 5 times (once per upload interval and once disposing)
                rawRowsEndpoint.AssertMatches(Times.AtMost(5));

                // queue with maximum size
                await using (var queue = cogniteDestination.CreateRawUploadQueue<TestDto>(_dbName, _tableName, TimeSpan.FromMinutes(10), 5))
                {
                    var enqueueTask = Task.Run(async () =>
                    {
                        while (index < 23)
                        {
                            queue.EnqueueRow($"r{index}", new TestDto { Name = "Test", Number = index });
                            await Task.Delay(100, source.Token);
                            index++;
                        }
                    });
                    var uploadTask = queue.Start(source.Token);

                    await enqueueTask;

                    // test cancelling the token;
                    source.Cancel();
                    await uploadTask;
                    Assert.True(uploadTask.IsCompleted);
                    logger.LogInformation("Enqueueing task cancelled. Disposing of the upload queue");
                }
                logger.LogInformation("Upload queue disposed");
                // Verify that the endpoint was called at most 3 more times (once per max size and once disposing)
                rawRowsEndpoint.AssertMatches(Times.AtMost(8));
            }

            // verify all rows were sent to CDF
            Assert.Single(raw.Databases);
            var table = raw.Databases[(_dbName, _tableName)];
            for (int i = 0; i < index; ++i)
            {
                Assert.True(table.TryGetValue($"r{i}", out var dto));
                _output.WriteLine($"Row r{i}: {dto.Columns.ToJsonString()}");
                Assert.Equal(i, dto.Columns["number"].GetValue<int>());
            }

            System.IO.File.Delete(path);
        }

        private class TestDto
        {
            public string Name { get; set; }
            public int Number { get; set; }
        }
    }
}