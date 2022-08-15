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
            else if (config?.Redis != null)
            {
                highAvailabilityManager = new RedisHighAvailabilityManager(
                    config,
                    provider.GetRequiredService<ILogger<HighAvailabilityManager>>(),
                    scheduler,
                    source,
                    interval,
                    inactivityThreshold);
            }

            return highAvailabilityManager;
        }

        public static CronTimeSpanWrapper CreateCronWrapper(int index, TimeSpan offset, TimeSpan? interval = null)
        {
            var cronWrapper = new CronTimeSpanWrapper(true, true, "s", "1");
            int offsetValue = (int)offset.TotalSeconds * index;

            if (interval != null)
            {
                var intervalCast = (TimeSpan)interval;
                int intervalValue = (int)intervalCast.TotalSeconds;
                cronWrapper.RawValue = $"{offsetValue}/{intervalValue} * * * * *";
            }
            else
            {
                cronWrapper.RawValue = $"{offsetValue} * * * * *";
            }

            return cronWrapper;
        }
    }
}