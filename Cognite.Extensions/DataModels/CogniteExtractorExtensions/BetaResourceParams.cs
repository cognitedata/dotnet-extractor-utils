using System;
namespace Cognite.Extensions.DataModels.CogniteExtractorExtensions
{
    /// <summary>
    /// Bundles the chunking, throttling, retry, and sanitation parameters shared by the
    /// beta CDM state-set and time-series extension methods (<see cref="BetaStateSetsExtensions"/>).
    /// </summary>
    public sealed class BetaResourceParams
    {
        /// <summary>
        /// Maximum number of items per request.
        /// </summary>
        public int ChunkSize { get; }

        /// <summary>
        /// Maximum number of parallel requests.
        /// </summary>
        public int ThrottleSize { get; }

        /// <summary>
        /// How to handle failed requests.
        /// </summary>
        public RetryMode RetryMode { get; }

        /// <summary>
        /// The type of sanitation to apply before sending requests.
        /// </summary>
        public SanitationMode SanitationMode { get; }

        /// <summary>
        /// Creates a new <see cref="BetaResourceParams"/>.
        /// </summary>
        /// <param name="chunkSize">Maximum number of items per request</param>
        /// <param name="throttleSize">Maximum number of parallel requests</param>
        /// <param name="retryMode">How to handle failed requests</param>
        /// <param name="sanitationMode">The type of sanitation to apply before sending requests</param>
        public BetaResourceParams(int chunkSize, int throttleSize, RetryMode retryMode, SanitationMode sanitationMode)
        {
            if (chunkSize <= 0) throw new ArgumentOutOfRangeException(nameof(chunkSize), "Chunk size must be greater than zero.");
            if (throttleSize <= 0) throw new ArgumentOutOfRangeException(nameof(throttleSize), "Throttle size must be greater than zero.");
            ChunkSize = chunkSize;
            ThrottleSize = throttleSize;
            RetryMode = retryMode;
            SanitationMode = sanitationMode;
        }
    }
}
