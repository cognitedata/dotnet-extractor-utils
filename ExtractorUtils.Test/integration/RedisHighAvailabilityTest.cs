using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;
using Cognite.Extractor.Utils;
using StackExchange.Redis;

namespace ExtractorUtils.Test.Integration
{
    public class RedisHighAvailabilityTest
    {
        private readonly ITestOutputHelper _output;
        private readonly ConnectionMultiplexer _redis;
        private readonly string _tableName = "test-table-integration";
        private readonly string _connectionString = "localhost";

        public RedisHighAvailabilityTest(ITestOutputHelper output)
        {
            _output = output;
            _redis = ConnectionMultiplexer.Connect(_connectionString);
        }

        [Fact(Timeout = 30000)]
        public async void TestRedisExtractorManagerRun()
        {
            Assert.True(_redis.IsConnected);

            // Creating configs for two different extractors.
            string configPath_0 = SetupConfig(index: 0);
            string configPath_1 = SetupConfig(index: 1);

            using var source_0 = new CancellationTokenSource();
            using var source_1 = new CancellationTokenSource();

            // Creating two extractors.
            Task extractor_0 = CreateExtractor(configPath_0, source_0.Token);
            Task extractor_1 = CreateExtractor(configPath_1, source_1.Token);

            // Turning off extractor 0 after 15s and 1 after 25s.
            Task cancel_0 = TurnOffAfterDelay(15000, source_0);
            Task cancel_1 = TurnOffAfterDelay(25000, source_1);

            try
            {
                // Testing running two extractors at the same, then turning them off one by one.
                // When the first extractor is turned off the second extractor will become active.
                Task tasks = Task.WhenAll(extractor_0, extractor_1, cancel_0, cancel_1);
                await tasks;
                Assert.True(tasks.IsCompleted);
            }
            finally
            {
                System.IO.File.Delete(configPath_0);
                System.IO.File.Delete(configPath_1);
            }
        }

        [Fact(Timeout = 45000)]
        public async void TestRedisRestartExtractor()
        {
            Assert.True(_redis.IsConnected);

            // Creating config for two extractors.
            string configPath_0 = SetupConfig(index: 0);
            string configPath_1 = SetupConfig(index: 1);

            using var source_0 = new CancellationTokenSource();
            using var source_1 = new CancellationTokenSource();
            using var source_2 = new CancellationTokenSource();

            // Creating extractor 0 and 1.
            Task extractor_0 = CreateExtractor(configPath_0, source_0.Token);
            Task extractor_1 = CreateExtractor(configPath_1, source_1.Token);
            // Creating a copy of extractor 0 that will start after 20s.
            Task restart_0 = CreateExtractor(configPath_0, source_2.Token, delay: 20000);

            //Turning off extractor 0 after 10s, 1 after 30s and the restarted 0 after 40s.
            Task cancel_0 = TurnOffAfterDelay(10000, source_0);
            Task cancel_1 = TurnOffAfterDelay(30000, source_1);
            Task cancel_2 = TurnOffAfterDelay(40000, source_2);

            try
            {
                // Running two extractors.
                // When extractor 0 is turned off, extractor 1 will start.
                // Then when extractor 0 is restarted it will go into standby.
                // Lastly when extractor 1 is turned off, 0 will start again.
                Task tasks = Task.WhenAll(extractor_0, extractor_1, cancel_0, cancel_1, cancel_2, restart_0);
                await tasks;

                Assert.True(tasks.IsCompleted);
            }
            finally
            {
                System.IO.File.Delete(configPath_0);
                System.IO.File.Delete(configPath_1);
            }
        }

        private string SetupConfig(int index)
        {
            var config = CDFTester.GetConfig(CogniteHost.BlueField);
            string[] lines = {
                        "high-availability:",
                        $"  index: {index}",
                        $"  redis:",
                        $"    connection-string: {_connectionString}",
                        $"    table-name: {_tableName}"};

            foreach (string line in lines) config = config.Append(line).ToArray();

            string path = $"test-redis-extractor-manager-{index}-config";
            System.IO.File.WriteAllLines(path, config);

            return path;
        }

        private async Task<Task> CreateExtractor(string configPath, CancellationToken token, int delay = 0)
        {
            if (delay > 0) await Task.Delay(delay);

            Task extractor = Task.Run(async () =>
                await ExtractorRunner.Run<MyTestConfig, MyExtractor>(
                configPath,
                null,
                "test-extractor-manager",
                null,
                false,
                true,
                false,
                false,
                token).ConfigureAwait(false));

            return extractor;
        }

        private Task TurnOffAfterDelay(int delay, CancellationTokenSource source)
        {
            Task cancel = Task.Run(async () =>
            {
                await Task.Delay(delay);
                source.Cancel();
            });

            return cancel;
        }
    }
}