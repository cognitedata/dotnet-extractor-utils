using Prometheus;

namespace Cognite.Extractor.Metrics
{
    /// <summary>
    /// Class containing common metrics
    /// </summary>
    public static class CommonMetrics
    {
        private static Gauge _version = Prometheus.Metrics.CreateGauge("extractor_utils_info",
            "Information about the running extractor", "name", "version");
        private static Gauge _startTime = Prometheus.Metrics.CreateGauge("extractor_utils_start_time", "Time the extractor last started");
        private static Gauge _endTime = Prometheus.Metrics.CreateGauge("extractor_utils_stop_time", "Time the extractor last stopped");

        /// <summary>
        /// Set metrics with information about extractor name and version
        /// </summary>
        /// <param name="name">Name of the extractor</param>
        /// <param name="version">Version of the extractor</param>
        public static void SetInfo(string name, string version)
        {
            _version.WithLabels(name, version).Set(0);
        }

        /// <summary>
        /// Set start time gauge to now in utc.
        /// </summary>
        public static void SetStartTime()
        {
            _startTime.SetToCurrentTimeUtc();
        }

        /// <summary>
        /// Set end time gauge to now in utc.
        /// </summary>
        public static void SetEndTime()
        {
            _endTime.SetToCurrentTimeUtc();
        }
    }
}
