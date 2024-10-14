using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extensions.DataModels.CogniteExtractorExtensions;
using Cognite.Extractor.Testing;
using Cognite.Extractor.Utils;
using CogniteSdk;
using CogniteSdk.DataModels;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Xunit.Abstractions;

namespace ExtractorUtils.Test
{
    public enum CogniteHost
    {
        GreenField,
        BlueField
    }

    class CDFTester : IDisposable
    {
        private static int _configIdx;
        public ILogger<CDFTester> Logger { get; }
        public ServiceProvider Provider { get; }
        public CogniteDestination Destination { get; }
        public CogniteDestinationWithIDM DestinationWithIDM { get; }
        public CancellationTokenSource Source { get; }
        public string Project { get; private set; }
        public string Host { get; private set; }
        public string Prefix { get; private set; }
        public BaseConfig Config { get; }
        private readonly string _configPath;
        public string SpaceId { get; private set; }

        public CDFTester(string[] config, ITestOutputHelper output)
        {
            // Thread safe increment and store
            var i = Interlocked.Increment(ref _configIdx);
            _configPath = $"test-config-{i}";
            System.IO.File.WriteAllLines(_configPath, config);
            var services = new ServiceCollection();
            Config = services.AddConfig<BaseConfig>(_configPath, 2);
            services.AddTestLogging(output);
            services.AddCogniteClient("net-extractor-utils-test", userAgent: "Utils-Tests/v1.0.0 (Test)");
            Provider = services.BuildServiceProvider();
            Logger = Provider.GetRequiredService<ILogger<CDFTester>>();
            Destination = Provider.GetRequiredService<CogniteDestination>();
            DestinationWithIDM = Provider.GetRequiredService<CogniteDestinationWithIDM>();
            Prefix = TestUtils.AlphaNumericPrefix("net-utils-test-");
            Source = new CancellationTokenSource();
            SpaceId = $"dotnet-extractor-utils-test-space{i}";
        }
        public CDFTester(CogniteHost host, ITestOutputHelper output) : this(GetConfig(host), output)
        {
            DestinationWithIDM.CogniteClient.DataModels.UpsertSpaces(new List<SpaceCreate>() { new() { Space = SpaceId } }).GetAwaiter().GetResult();
        }

        public async Task<long> GetDataSetId()
        {
            var dataSets = await Destination.CogniteClient.DataSets.RetrieveAsync(new[] { "test-dataset" }, true);
            if (!dataSets.Any())
            {
                dataSets = await Destination.CogniteClient.DataSets.CreateAsync(new[] { new DataSetCreate
                {
                    Description = ".NET utils test dataset",
                    ExternalId = "test-dataset",
                    Name = "Test dataset"
                } });
            }
            return dataSets.First().Id;
        }

        public static string[] GetConfig(CogniteHost host, bool onlyCognite = false)
        {
            var config = onlyCognite
                ? new List<string>()
                {
                    "version: 2",
                    "cognite:"
                }
                : new List<string>() {
                    "version: 2",
                    "logger:",
                    "  console:",
                    "    level: verbose",
                    "cognite:",
                };
            switch (host)
            {
                case CogniteHost.GreenField:
                    config = config.Concat(new List<String>() {
                        "  project: ${TEST_PROJECT}",
                        "  host: ${TEST_HOST}",
                        "  idp-authentication:",
                        "    client-id: ${TEST_CLIENT_ID}",
                        "    tenant: ${TEST_TENANT}",
                        "    secret: ${TEST_SECRET}",
                        "    scopes:",
                        "    - ${TEST_SCOPE}"
                    }).ToList();
                    break;
                case CogniteHost.BlueField:
                    config = config.Concat(new List<String>() {
                        "  project: ${BF_TEST_PROJECT}",
                        "  host: ${BF_TEST_HOST}",
                        "  idp-authentication:",
                        "    client-id: ${BF_TEST_CLIENT_ID}",
                        "    tenant: ${BF_TEST_TENANT}",
                        "    secret: ${BF_TEST_SECRET}",
                        "    scopes:",
                        "    - ${BF_TEST_SCOPE}"
                    }).ToList();
                    break;
            }
            config = config.Concat(new List<String>() {
                "  cdf-chunking:",
                "    time-series: 20",
                "    assets: 20",
                "    events: 20",
                "    sequences: 10",
                "    sequence-row-sequences: 10",
                "    sequence-rows: 100",
                "    data-point-time-series: 10",
                "    data-points: 100",
                "  cdf-throttling:",
                "    time-series: 2",
                "    assets: 2",
                "    events: 2",
                "    sequences: 2",
                "    data-points: 2"
            }).ToList();
            return config.ToArray();
        }

        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    System.IO.File.Delete(_configPath);
                    DestinationWithIDM.CogniteClient.DataModels.DeleteSpaces(new List<string>() { SpaceId });
                    Provider.Dispose();
                    Source.Dispose();
                }
                disposedValue = true;
            }
        }
        public void Dispose()
        {
            Dispose(true);
        }
        #endregion
    }
}
