using Cognite.Extractor.Logging;
using Cognite.Extractor.Utils;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace ExtractorUtils.Test
{
    class CDFTester : IDisposable
    {
        private static int _configIdx;
        public ServiceProvider Provider { get; }
        public CogniteDestination Destination { get; }
        public CancellationTokenSource Source { get; }
        public string Project { get; private set; }
        public string Host { get; private set; }
        public string Prefix { get; private set; }
        private readonly string _configPath;

        public CDFTester(string[] configLines)
        {
            // Thread safe increment and store
            _configPath = $"test-config-{Interlocked.Increment(ref _configIdx)}";
            System.IO.File.WriteAllLines(_configPath, configLines);
            var services = new ServiceCollection();
            services.AddConfig<BaseConfig>(_configPath, 2);
            services.AddLogger();
            services.AddCogniteClient("net-extractor-utils-test");
            Provider = services.BuildServiceProvider();
            Destination = Provider.GetRequiredService<CogniteDestination>();
            const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
            Random random = new Random();
            Prefix = new string(Enumerable.Repeat(chars, 5)
              .Select(s => s[random.Next(s.Length)]).ToArray());
            Source = new CancellationTokenSource();
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
