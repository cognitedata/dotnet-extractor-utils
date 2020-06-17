using Prometheus;
using System;
using System.Collections.Generic;
using System.Text;

namespace Cognite.Extractor.StateStorage
{
    static class StateStoreMetrics
    {
        public static Counter StateRestoreStates = Prometheus.Metrics.CreateCounter(
            "extractor_utils_restore_states", "Total number of states restored from state storage");
        public static Counter StateRestoreCount = Prometheus.Metrics.CreateCounter(
            "extractor_utils_restore_count", "Number of times states have been restored from state storage");
        public static Counter StateStoreStates = Prometheus.Metrics.CreateCounter(
            "extractor_utils_store_states", "Total number of states stored to state storage");
        public static Counter StateStoreCount = Prometheus.Metrics.CreateCounter(
            "extractor_utils_store_count", "Number of times states have been stored to state storage");
    }
}
