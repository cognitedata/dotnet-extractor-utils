using System;
using System.Threading;
using Cognite.Extractor.Common;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;

namespace Cognite.Extractor.Utils
{
    static class HighAvailabilityUtils
    {
        public static IHighAvailabilityManager? CreateHighAvailabilityManager(
            HighAvailabilityConfig config,
            IServiceProvider provider,
            PeriodicScheduler scheduler,
            CancellationTokenSource source,
            TimeSpan? interval = null,
            TimeSpan? inactivityThreshold = null)
        {
            IHighAvailabilityManager? highAvailabilityManager = null;
            
            if (config?.Raw != null)
            {
                highAvailabilityManager = new RawHighAvailabilityManager(
                    config,
                    provider.GetRequiredService<CogniteDestination>(),
                    provider.GetRequiredService<ILogger<HighAvailabilityManager>>(),
                    scheduler,
                    source,
                    interval,
                    inactivityThreshold);
            }

            return highAvailabilityManager;
        }
    }
}