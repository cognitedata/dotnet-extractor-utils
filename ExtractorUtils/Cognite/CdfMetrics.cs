using Prometheus;

namespace Cognite.Extractor.Utils
{
    static class CdfMetrics
    {
        public static Summary Assets { get; } = Prometheus.Metrics.CreateSummary("extractor_utils_cdf_asset_requests",
            "Number and duration of asset requests to CDF", "endpoint");
        public static Summary TimeSeries { get; } = Prometheus.Metrics.CreateSummary("extractor_utils_cdf_timeseries_requests",
            "Number and duration of time-series requests to CDF", "endpoint");
        public static Summary Datapoints { get; } = Prometheus.Metrics.CreateSummary("extractor_utils_cdf_datapoint_requests",
            "Number and duration of datapoint requests to CDF", "endpoint");
        public static Summary Events { get; } = Prometheus.Metrics.CreateSummary("extractor_utils_cdf_event_requests",
            "Number and duration of event requests to CDF", "endpoint");
        public static Summary Raw { get; } = Prometheus.Metrics.CreateSummary("extractor_utils_cdf_raw_requests",
            "Number and duration of raw requests to CDF", "endpoint");
        public static Summary Login { get; } = Prometheus.Metrics.CreateSummary("extractor_utils_cdf_login_requests",
            "Number and duration of login requests to CDF", "endpoint");

    }
}
