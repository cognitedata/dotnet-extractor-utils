﻿using Cognite.Extractor.Logging;
using Cognite.Extractor.Utils;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

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
        public CancellationTokenSource Source { get; }
        public string Project { get; private set; }
        public string Host { get; private set; }
        public string Prefix { get; private set; }
        private readonly string _configPath;

        public CDFTester(string[] config)
        {
            // Thread safe increment and store
            _configPath = $"test-config-{Interlocked.Increment(ref _configIdx)}";
            System.IO.File.WriteAllLines(_configPath, config);
            var services = new ServiceCollection();
            services.AddConfig<BaseConfig>(_configPath, 2);
            services.AddLogger();
            services.AddCogniteClient("net-extractor-utils-test", userAgent: "Utils-Tests/v1.0.0 (Test)");
            Provider = services.BuildServiceProvider();
            Logger = Provider.GetRequiredService<ILogger<CDFTester>>();
            Destination = Provider.GetRequiredService<CogniteDestination>();
            const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
            Random random = new Random();
            Prefix = "net-utils-test-" + new string(Enumerable.Repeat(chars, 5)
              .Select(s => s[random.Next(s.Length)]).ToArray());
            Source = new CancellationTokenSource();
        }
        public CDFTester(CogniteHost host) : this(GetConfig(host))
        {
        }

        public static string[] GetConfig(CogniteHost host)
        {
            var config = new List<string>() {
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
                        "  api-key: ${TEST_API_KEY}",
                        "  host: ${TEST_HOST}"
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
                "  cdf-throttling:",
                "    time-series: 2",
                "    assets: 2",
                "    events: 2",
                "    sequences: 2"
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