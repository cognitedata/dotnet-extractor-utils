using System.Collections.Generic;
using CogniteSdk.DataModels.Core;

namespace Cognite.Extensions.DataModels.CogniteExtractorExtensions
{
    /// <summary>
    /// Generic base class for CDM TimeSeries extended for use with extractors.
    /// </summary>
    /// <typeparam name="T">Type of the extracted data value.</typeparam>
    public class CogniteExtractorTimeSeriesBase<T> : CogniteTimeSeriesBase
    {
        /// <summary>
        /// Unstructured metadata extracted from the source system.
        /// </summary>
        public Dictionary<string, T>? extractedData { get; set; }

        /// <summary>
        /// Empty Constructor.
        /// </summary>
        public CogniteExtractorTimeSeriesBase() : base() { }
    }
}
