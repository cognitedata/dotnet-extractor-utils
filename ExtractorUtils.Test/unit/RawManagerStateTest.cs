using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Moq;
using Moq.Protected;
using Xunit;
using Cognite.Extractor.Logging;
using Cognite.Extractor.Utils;
using Xunit.Abstractions;
using Cognite.Extractor.Testing;
using Cognite.Extractor.Common;

namespace ExtractorUtils.Test.Unit
{
    public class RawManagerStateTest
    {
        private const string _authTenant = "someTenant";
        private const string _project = "someProject";
        private const string _apiKey = "someApiKey";
        private const string _host = "https://test.cognitedata.com";
        private const string _dbName = "testDb";
        private const string _tableName = "testTable";

        [Fact]
        public async Task TestUploadLogToState()
        {
            string path = "test-upload-log-to-state-config.yml";
            
            string[] lines = {  "version: 2",
                                "logger:",
                                "  console:",
                                "    level: verbose",
                                "cognite:",
                               $"  project: {_project}",
                               $"  api-key: {_apiKey}",
                               $"  host: {_host}",
                                "  cdf-chunking:",
                                "    raw-rows: 4",
                                "  cdf-throttling:",
                                "    raw: 2",
                                "manager:",
                                "  index: 0",
                                $"  database-name: {_dbName}",
                                $"  table-name: {_tableName}"};
            System.IO.File.WriteAllLines(path, lines);
        
            var services = new ServiceCollection();

            var config = services.AddConfig<BaseConfig>(path, 2);
            var provider = services.BuildServiceProvider();

            RawManagerConfig managerConfig = config.Manager;

            Console.WriteLine(managerConfig.DatabaseName);

            CogniteDestination destination = provider.GetRequiredService<CogniteDestination>();
            ILogger<RawExtractorManager> logger = provider.GetRequiredService<ILogger<RawExtractorManager>>();
            CancellationTokenSource source = new CancellationTokenSource();
            PeriodicScheduler scheduler = new PeriodicScheduler(source.Token);

            RawExtractorManager extractorManager = new RawExtractorManager(managerConfig, destination, logger, scheduler, source);

            System.IO.File.Delete(path);
        }
    }
}
