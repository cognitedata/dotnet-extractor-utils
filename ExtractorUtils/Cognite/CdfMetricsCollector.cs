using System.Collections.Generic;
using CogniteSdk;
using Prometheus;

namespace ExtractorUtils
{
    class CdfMetricCollector : IMetrics
    {
        private Counter _counterMetricFetchInc = Metrics.CreateCounter("cognite_sdk_fetch_inc", @"Number of POST/GET/... actions performed");
        private Counter _counterMetricFetchErrorInc = Metrics.CreateCounter("cognite_sdk_fetch_error_inc", "Number of errors on actions");
        private Counter _counterMetricFetchRetryInc = Metrics.CreateCounter("cognite_sdk_fetch_retry_inc", "Number of retries on actions");
        private Counter _counterMetricDecodeErrorInc = Metrics.CreateCounter("cognite_sdk_decode_error_inc", "Decoding data errors");
        private Gauge _gaugeMetricFetchLatencyUpdate = Metrics.CreateGauge("cognite_sdk_fetch_latency_update", "Latency on actions performed");

        public void Counter(string metric, IDictionary<string, string> labels, long increase)
        {
            switch (metric)
            {
                case "MetricFetchInc":
                    _counterMetricFetchInc.Inc(increase);
                    break;
                case "MetricFetchErrorInc":
                    _counterMetricFetchErrorInc.Inc(increase);
                    break;
                case "MetricFetchRetryInc":
                    _counterMetricFetchRetryInc.Inc(increase);
                    break;
                case "MetricDecodeErrorInc":
                    _counterMetricDecodeErrorInc.Inc(increase);
                    break;
            }
        }

        public void Gauge(string metric, IDictionary<string, string> labels, double value)
        {
            switch (metric)
            {
                case "MetricFetchLatencyUpdate":
                    _gaugeMetricFetchLatencyUpdate.Set(value);
                    break;
            }
        }
    }    
}