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
        public static Summary Sequences { get; } = Metrics.CreateSummary("extractor_utils_cdf_sequence_requests",
            "Number and duration of sequence requests to CDF", "endpoint");
        public static Summary SequenceRows { get; } = Metrics.CreateSummary("extractor_utils_cdf_sequence_row_requests",
            "Number and duration of sequence row requests to CDF", "endpoint");
        public static Counter NumberDatapoints { get; } = Metrics.CreateCounter(
            "extractor_utils_cdf_datapoints", "Number of data points uploaded to CDF");

        public static Counter AssetsSkipped { get; } = Metrics.CreateCounter("extractor_utils_cdf_assets_skipped",
            "Number of assets skipped due to errors");
        public static Counter TimeSeriesSkipped { get; } = Metrics.CreateCounter("extractor_utils_cdf_timeseries_skipped",
            "Number of timeseries skipped due to errors");
        public static Counter EventsSkipped { get; } = Metrics.CreateCounter("extractor_utils_cdf_events_skipped",
            "Number of events skipped due to errors");
        public static Counter SequencesSkipped { get; } = Metrics.CreateCounter("extractor_utils_cdf_sequences_skipped",
            "Number of sequences skipped due to errors");
        public static Counter SequenceRowsSkipped { get; } = Metrics.CreateCounter("extractor_utils_cdf_sequence_rows_skipped",
            "Number of sequence rows skipped due to errors");
        public static Counter DatapointTimeseriesSkipped { get; } = Metrics.CreateCounter("extractor_utils_datapoint_timeseries_skipped",
            "Number of whole timeseries skipped while pushing datapoints");
        public static Counter DatapointsSkipped { get; } = Metrics.CreateCounter("extractor_utils_datapoints_skipped",
            "Number of datapoints skipped due to errors");

        public static Counter AssetUpdatesSkipped { get; } = Metrics.CreateCounter("extractor_utils_cdf_asset_updates_skipped",
            "Number of asset updates skipped due to errors");
        public static Counter TimeSeriesUpdatesSkipped { get; } = Metrics.CreateCounter("extractor_utils_cdf_timeseries_updates_skipped",
            "Number of timeseries updates skipped due to errors");
    }
}
