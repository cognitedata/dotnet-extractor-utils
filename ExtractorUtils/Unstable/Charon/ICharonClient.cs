using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.Extractor.Utils.Unstable.Charon
{
    /// <summary>
    /// Low-level typed client for the Charon CDF-writer wire contract.
    ///
    /// Charon is a POC with a changing contract; this interface is the single isolation seam, so a
    /// wire-format change only needs to touch the implementation.
    /// </summary>
    public interface ICharonClient
    {
        /// <summary>
        /// Register the complete task set for an integration (<c>POST /cdfwriter/setup</c>).
        /// This is a full replace: tasks not present are deleted on Charon's side.
        /// </summary>
        /// <param name="request">Setup request with the full task set.</param>
        /// <param name="token">Cancellation token.</param>
        /// <exception cref="CharonSetupException">Thrown on a 422 validation failure, listing every failure.</exception>
        /// <exception cref="CharonException">Thrown on other non-2xx responses.</exception>
        Task SetupAsync(CharonSetupRequest request, CancellationToken token);

        /// <summary>
        /// Send one payload batch (<c>POST /cdfwriter/payload</c>). The caller must size each task's
        /// items within CDF limits, since Charon does no chunking.
        /// </summary>
        /// <param name="request">Payload request with items grouped by task.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>Per-task downstream results (from a 200 or 207 response).</returns>
        /// <exception cref="CharonPayloadValidationException">Thrown on a 422 validation failure.</exception>
        /// <exception cref="CharonException">Thrown on other non-2xx responses (401/404/408/5xx).</exception>
        Task<CharonPayloadResponse> SendPayloadAsync(CharonPayloadRequest request, CancellationToken token);
    }

    /// <summary>
    /// Base exception for Charon client failures. Never carries bearer tokens or full item bodies.
    /// </summary>
    public class CharonException : Exception
    {
        /// <summary>HTTP status code of the failing response, or 0 if the call never completed.</summary>
        public int Status { get; }

        /// <summary>Create a Charon exception.</summary>
        /// <param name="message">Error message.</param>
        /// <param name="status">HTTP status code, if any.</param>
        /// <param name="inner">Inner exception, if any.</param>
        public CharonException(string message, int status = 0, Exception? inner = null) : base(message, inner)
        {
            Status = status;
        }
    }

    /// <summary>
    /// Thrown when <c>/cdfwriter/setup</c> returns 422. Lists every per-item failure.
    /// </summary>
    public class CharonSetupException : CharonException
    {
        /// <summary>Per-item setup failures reported by Charon.</summary>
        public IReadOnlyList<CharonItemError> Errors { get; }

        /// <summary>Create a setup validation exception.</summary>
        /// <param name="message">Summary message.</param>
        /// <param name="errors">Per-item failures.</param>
        public CharonSetupException(string message, IReadOnlyList<CharonItemError> errors) : base(message, 422)
        {
            Errors = errors;
        }
    }

    /// <summary>
    /// Thrown when <c>/cdfwriter/payload</c> returns 422 (phase-1 validation). Nothing was forwarded.
    /// </summary>
    public class CharonPayloadValidationException : CharonException
    {
        /// <summary>Per-item validation failures reported by Charon.</summary>
        public IReadOnlyList<CharonItemError> Errors { get; }

        /// <summary>Create a payload validation exception.</summary>
        /// <param name="message">Summary message.</param>
        /// <param name="errors">Per-item failures.</param>
        public CharonPayloadValidationException(string message, IReadOnlyList<CharonItemError> errors) : base(message, 422)
        {
            Errors = errors;
        }
    }
}
