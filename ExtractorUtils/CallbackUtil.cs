using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.Extractor.Utils
{
    /// <summary>
    /// Config class for functions.
    /// </summary>
    public class FunctionCallConfig
    {
        /// <summary>
        /// Function externalId
        /// </summary>
        public string? ExternalId { get; set; }
        /// <summary>
        /// Function internalId
        /// </summary>
        public long? Id { get; set; }
    }


    /// <summary>
    /// Wrapper class for calling an existing function in CDF.
    /// </summary>
    public class FunctionCallWrapper<T>
    {
        private readonly string? _externalId;
        private long? _id;

        private readonly CogniteDestination _destination;
        private readonly ILogger _log;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="destination">Cognite destination</param>
        /// <param name="config">Function configuration</param>
        /// <param name="logger">Optional logger</param>
        public FunctionCallWrapper(CogniteDestination destination, FunctionCallConfig config, ILogger? logger)
        {
            if (config == null) throw new ArgumentNullException(nameof(config));
            if (destination == null) throw new ArgumentNullException(nameof(destination));
            _log = logger ?? new NullLogger<FunctionCallWrapper<T>>();
            _destination = destination;
            _externalId = config.ExternalId;
            _id = config.Id;
        }

        /// <summary>
        /// Call the configured function, optionally fetching its internalId if it is not found.
        /// </summary>
        /// <param name="arguments"></param>
        /// <param name="token"></param>
        /// <returns>True if call was successful</returns>
        public Task<bool> TryCall(T arguments, CancellationToken token)
        {
            if (!_id.HasValue && string.IsNullOrEmpty(_externalId))
            {
                _log.LogWarning("Missing function configuration, not calling");
                return Task.FromResult(false);
            }

            _log.LogWarning("Function calls are currently broken in the extractor. Nothing will happen");

            /* if (!_id.HasValue)
            {
                try
                {
                    var funcs = await _destination.CogniteClient.Playground.Functions
                        .RetrieveAsync(new[] { Identity.Create(_externalId) }, token)
                        .ConfigureAwait(false);
                    _id = funcs.First().Id;
                }
                catch (Exception ex)
                {
                    _log.LogError("Failed to retrieve function from CDF: {Message}", ex.Message);
                    return false;
                }
            }

            try
            {
                await _destination.CogniteClient.Playground.FunctionCalls.CallFunction(_id.Value, arguments, token)
                    .ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _log.LogError("Failed to call function in CDF: {Message}", ex.Message);
                return false;
            } */

            return Task.FromResult(true);
        }
    }
}
