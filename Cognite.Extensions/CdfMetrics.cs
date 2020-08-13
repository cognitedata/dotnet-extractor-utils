using Prometheus;

namespace Cognite.Extensions
{
    static class CdfMetrics
    {
        public static Summary Assets { get; } = Metrics.CreateSummary("extractor_utils_cdf_asset_requests",
            "Number and duration of asset requests to CDF", "endpoint");
        public static Summary TimeSeries { get; } = Metrics.CreateSummary("extractor_utils_cdf_timeseries_requests",
            "Number and duration of time-series requests to CDF", "endpoint");
        public static Summary Datapoints { get; } = Metrics.CreateSummary("extractor_utils_cdf_datapoint_requests",
            "Number and duration of datapoint requests to CDF", "endpoint");
        public static Summary Events { get; } = Metrics.CreateSummary("extractor_utils_cdf_event_requests",
            "Number and duration of event requests to CDF", "endpoint");
        public static Summary Raw { get; } = Metrics.CreateSummary("extractor_utils_cdf_raw_requests",
            "Number and duration of raw requests to CDF", "endpoint");

        public static Counter AssetSkipped { get; } = Metrics.CreateCounter("extractor_utils_cdf_assets_skipped",
            "Number of assets skipped due to errors");
        public static Counter TimeSeriesSkipped { get; } = Metrics.CreateCounter("extractor_utils_cdf_timeseries_skipped",
            "Number of timeseries skipped due to errors");
        public static Counter EventsSkipped { get; } = Metrics.CreateCounter("extractor_utils_cdf_events_skipped",
            "Number of events skipped due to errors");
    }
}
