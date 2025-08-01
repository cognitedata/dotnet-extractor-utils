using System.Collections.Generic;
using System.Text.Json.Nodes;

namespace Cognite.Extractor.Testing.Mock
{
    /// <summary>
    /// Cognite error object used in the mock server.
    /// </summary>
    public class CogniteError
    {
        /// <summary>
        /// Error code, e.g. 404, 500, etc.
        /// </summary>
        public int Code { get; set; }
        /// <summary>
        /// Error message.
        /// </summary>
        public string? Message { get; set; }
        /// <summary>
        /// Missing values.
        /// </summary>
        public IEnumerable<Dictionary<string, CogniteSdk.MultiValue>>? Missing { get; set; }
        /// <summary>
        /// Duplicated values.
        /// </summary>
        public IEnumerable<Dictionary<string, CogniteSdk.MultiValue>>? Duplicated { get; set; }
        /// <summary>
        /// Complex extras object.
        /// </summary>
        public JsonNode? Extra { get; set; }

        /// <summary>
        /// Empty constructor for deserialization.
        /// </summary>
        public CogniteError()
        {
        }

        /// <summary>
        /// Constructor for creating a Cognite error object.
        /// </summary>
        /// <param name="code">Error code, e.g. 404, 500, etc.</param>
        /// <param name="message">Error message.</param>
        public CogniteError(int code, string? message = null)
        {
            Code = code;
            Message = message;
        }
    }

    /// <summary>
    /// Wrapper for Cognite error responses.
    /// </summary>
    public class CogniteErrorWrapper
    {
        /// <summary>
        /// Inner error object.
        /// </summary>
        public CogniteError Error { get; set; }

        /// <summary>
        /// Constructor for creating a Cognite error wrapper.
        /// </summary>
        /// <param name="error">Inner error object.</param>
        public CogniteErrorWrapper(CogniteError error)
        {
            Error = error;
        }
    }

}