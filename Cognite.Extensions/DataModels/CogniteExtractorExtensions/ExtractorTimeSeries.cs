using CogniteSdk.DataModels.Core;

namespace Cognite.Extensions.DataModels.CogniteExtractorExtensions
{
    /// <summary>
    /// CDM TimeSeries extended for use with extractors
    /// </summary>
    public class CogniteExtractorTimeSeries : CogniteExtractorTimeSeriesBase<string>
    {
        /// <summary>
        /// Empty Constructor.
        /// </summary>
        public CogniteExtractorTimeSeries() : base() { }
    }
}