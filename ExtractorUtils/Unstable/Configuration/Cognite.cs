using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cognite.Extractor.Utils;

namespace Cognite.Extractor.Utils.Unstable.Configuration
{
    /// <summary>
    /// Base class for cognite config.
    /// </summary>
    public class BaseCogniteConfig
    {
        /// <summary>
        /// Chunking sizes towards CDF 
        /// </summary>
        public ChunkingConfig CdfChunking { get => _cdfChunking; set { _cdfChunking = value ?? _cdfChunking; } }
        private ChunkingConfig _cdfChunking = new ChunkingConfig();

        /// <summary>
        /// Throttling of requests to CDF
        /// </summary>
        public ThrottlingConfig CdfThrottling { get => _cdfThrottling; set { _cdfThrottling = value ?? _cdfThrottling; } }
        private ThrottlingConfig _cdfThrottling = new ThrottlingConfig();
        /// <summary>
        /// Optional replacement for non-finite double values in datapoints
        /// </summary>
        public double? NanReplacement { get; set; }
    }
}