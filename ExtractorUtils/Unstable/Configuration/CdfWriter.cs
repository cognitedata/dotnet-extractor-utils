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
        /// Validate the config section. When enabled, the <c>cdf-mapping</c> must contain items for both
        /// <see cref="DatapointsTaskName"/> and <see cref="TimeseriesTaskName"/>.
        /// </summary>
        /// <param name="config">Config to validate, may be null.</param>
        /// <param name="mapping">The <c>cdf-mapping</c> items, may be null if no mapping is configured.</param>
        /// <returns>An error message if the config is invalid, null otherwise.</returns>
        public static string? Validate(CdfWriterConfig? config, IEnumerable<CdfMappingItem?>? mapping)
        {
            if (config == null || !config.Enabled) return null;

            var tasks = new HashSet<string>((mapping ?? Enumerable.Empty<CdfMappingItem?>())
                .Select(item => item?.TaskName)
                .OfType<string>());
            foreach (var task in new[] { DatapointsTaskName, TimeseriesTaskName })
            {
                if (!tasks.Contains(task))
                {
                    return $"cognite.cdf-writer requires a cognite.cdf-mapping item with task-name \"{task}\"";
                }
            }
            return null;
        }
    }
}
