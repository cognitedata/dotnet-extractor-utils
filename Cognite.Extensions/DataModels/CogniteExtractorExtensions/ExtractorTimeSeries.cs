using System.Collections.Generic;
using CogniteSdk.DataModels.Core;

namespace Cognite.Extensions.DataModels.CogniteExtractorExtensions
{
    /// <summary>
    /// CDM TimeSeries extended for use with extractors
    /// </summary>
    public class CogniteExtractorTimeSeries : CogniteTimeSeriesBase
    {
        /// <summary>
        /// Unstructured metadata extracted from the source system.
        /// </summary>
        public Dictionary<string, string>? extractedData { get; set; }

        /// <summary>
        /// Empty Constructor.
        /// </summary>
        public CogniteExtractorTimeSeries() : base() { }
    }
}