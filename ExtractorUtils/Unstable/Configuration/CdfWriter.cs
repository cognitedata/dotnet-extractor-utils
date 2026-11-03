using System.Collections.Generic;
using System.Linq;

namespace Cognite.Extractor.Utils.Unstable.Configuration
{
    /// <summary>
    /// The <c>cognite.cdf-writer</c> config section. When enabled, writes are sent to the CDF writer,
    /// which applies the mappings registered through <c>cognite.cdf-mapping</c> and forwards the
    /// result to CDF, instead of being written directly to CDF. There is no automatic fallback.
    ///
    /// Batch sizes and parallelism are taken from <c>cognite.cdf-chunking</c> and <c>cognite.cdf-throttling</c>,
    /// and retries from <c>connection.cdf-connection.retries</c>, the same as for direct writes.
    /// </summary>
    public class CdfWriterConfig
    {
        /// <summary>
        /// Name of the <c>cdf-mapping</c> task used for datapoint inserts.
        /// </summary>
        public const string DatapointsTaskName = "datapoints";
        /// <summary>
        /// Name of the <c>cdf-mapping</c> task used for timeseries create/update.
        /// </summary>
        public const string TimeseriesTaskName = "timeseries";

        /// <summary>
        /// Master switch. When false, writes go directly to CDF.
        /// </summary>
        public bool Enabled { get; set; }

        /// <summary>
        /// Validate the config section. When enabled, the <c>cdf-mapping</c> configuration must be present.
        /// </summary>
        /// <param name="config">Config to validate, may be null.</param>
        /// <param name="mapping">The <c>cdf-mapping</c> config, may be null if no mapping is configured.</param>
        /// <returns>An error message if the config is invalid, null otherwise.</returns>
        public static string? Validate(CdfWriterConfig? config, CdfMappingConfig? mapping)
        {
            if (config == null || !config.Enabled) return null;

            if (mapping == null)
            {
                return "cognite.cdf-writer requires cognite.cdf-mapping to be configured";
            }

            return null;
        }
    }
}
