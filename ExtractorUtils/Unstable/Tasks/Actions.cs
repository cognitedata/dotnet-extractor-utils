using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using CogniteSdk;
using CogniteSdk.Alpha;

namespace Cognite.Extractor.Utils.Unstable.Tasks
{
    /// <summary>
    /// Naming convention for the auto-generated Start/Stop actions of an actionable task.
    ///
    /// Centralized here so that both the code that advertises these names (<see
    /// cref="BaseExtractor{TConfig}.GetStartupRequest"/>) and the code that later routes an
    /// incoming action name back to a task (the dispatch engine) construct the exact same
    /// strings -- duplicating this formatting in two places would risk them silently drifting
    /// apart.
    /// </summary>
    internal static class ActionNaming
    {
        public const string StartPrefix = "Start ";
        public const string StopPrefix = "Stop ";

        public static string StartActionName(string taskName) => StartPrefix + taskName;
        public static string StopActionName(string taskName) => StopPrefix + taskName;
    }

    /// <summary>
    /// Exception type for expected, structured failures raised from within a custom action's
    /// target callback.
    ///
    /// Unlike an arbitrary exception, an <see cref="ActionError"/> carries a caller-chosen
    /// <see cref="ErrorType"/> and optional <see cref="Details"/>, which the dispatcher surfaces
    /// as structured <c>resultMetadata</c> on the action's terminal <c>failed</c> update, in
    /// addition to using <see cref="Exception.Message"/> as the result message.
    /// </summary>
    public class ActionError : Exception
    {
        /// <summary>
        /// Short, caller-defined category for this failure (e.g. "invalid_parameter",
        /// "missing_parameter"). Surfaced in the terminal action update's result metadata under
        /// the key "errorType".
        /// </summary>
        public string ErrorType { get; }

        /// <summary>
        /// Optional longer-form detail about the failure, distinct from the short human-readable
        /// <see cref="Exception.Message"/>. Surfaced in the terminal action update's result
        /// metadata under the key "errorDetail", if set.
        /// </summary>
        public string? Details { get; }

        /// <summary>
        /// Structured metadata for this error, suitable for use as an <see cref="ActionUpdate.ResultMetadata"/>.
        /// </summary>
        public IReadOnlyDictionary<string, string> ResultMetadata
        {
            get
            {
                var metadata = new Dictionary<string, string> { ["errorType"] = ErrorType };
                if (Details != null) metadata["errorDetail"] = Details;
                return metadata;
            }
        }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="errorType">Short, caller-defined category for this failure.</param>
        /// <param name="message">Human-readable message, used as the action's result message.</param>
        /// <param name="details">Optional longer-form detail about the failure.</param>
        public ActionError(string errorType, string message, string? details = null) : base(message)
        {
            if (string.IsNullOrEmpty(errorType)) throw new ArgumentException("Error type must be set", nameof(errorType));
            ErrorType = errorType;
            Details = details;
        }
    }

    /// <summary>
    /// Context passed to a custom action's target callback, giving it access to the extractor's
    /// configuration and CDF client, information about the specific action invocation it's
    /// handling, and the means to report progress and a final result.
    /// </summary>
    /// <typeparam name="TConfig">Extractor configuration type.</typeparam>
    public sealed class ActionContext<TConfig>
    {
        /// <summary>
        /// The extractor's application configuration.
        /// </summary>
        public TConfig ApplicationConfig { get; }

        /// <summary>
        /// CDF client, if the extractor is configured with one.
        /// </summary>
        public CogniteDestination? CdfClient { get; }

        /// <summary>
        /// External ID of the integration this action was triggered against.
        /// </summary>
        public string IntegrationExternalId { get; }

        /// <summary>
        /// External ID of this specific action invocation. Used when reporting progress or a
        /// final result back via <see cref="ReportProgress"/>/<see cref="SetResult"/>.
        /// </summary>
        public string ExternalId { get; }

        /// <summary>
        /// Caller-supplied metadata provided when this action was triggered, if any.
        /// </summary>
        public IReadOnlyDictionary<string, string>? CallMetadata { get; }

        private readonly IIntegrationSink _sink;
        private readonly object _lock = new object();
        private bool _resultSet;

        /// <summary>
        /// The result message recorded via <see cref="SetResult"/>, or <c>null</c> if it was
        /// never called. Read by the dispatcher once the target callback returns without
        /// throwing, to build the action's terminal <c>succeeded</c> update -- this property
        /// only records state, it does not itself queue anything.
        /// </summary>
        internal string? ResultMessage { get; private set; }

        /// <summary>
        /// The result metadata recorded via <see cref="SetResult"/>, or <c>null</c> if it was
        /// never called or no metadata was provided.
        /// </summary>
        internal IReadOnlyDictionary<string, string>? ResultMetadata { get; private set; }

        /// <summary>
        /// Constructor.
        /// </summary>
        internal ActionContext(
            TConfig applicationConfig,
            CogniteDestination? cdfClient,
            string integrationExternalId,
            string externalId,
            IReadOnlyDictionary<string, string>? callMetadata,
            IIntegrationSink sink)
        {
            ApplicationConfig = applicationConfig;
            CdfClient = cdfClient;
            IntegrationExternalId = integrationExternalId ?? throw new ArgumentNullException(nameof(integrationExternalId));
            ExternalId = externalId ?? throw new ArgumentNullException(nameof(externalId));
            CallMetadata = callMetadata;
            _sink = sink ?? throw new ArgumentNullException(nameof(sink));
        }

        /// <summary>
        /// Record the final result of this action, to be reported once the target callback
        /// returns. May only be called once per action -- this is a deliberate guard, since an
        /// action can only have one final result.
        ///
        /// This does not itself send anything -- the terminal update is queued by the dispatcher
        /// after the target callback returns without throwing an exception. If the target never
        /// calls this, the dispatcher still reports a terminal `succeeded` update, just with an
        /// empty result message.
        /// </summary>
        /// <param name="message">Human-readable result message.</param>
        /// <param name="metadata">Optional structured result metadata.</param>
        /// <exception cref="InvalidOperationException">If called more than once.</exception>
        public void SetResult(string message, IReadOnlyDictionary<string, string>? metadata = null)
        {
            lock (_lock)
            {
                if (_resultSet) throw new InvalidOperationException("SetResult has already been called for this action");
                _resultSet = true;
                ResultMessage = message;
                ResultMetadata = metadata;
            }
        }

        /// <summary>
        /// Report intermediate progress on this action. May be called any number of times, and
        /// does not interact with the "call once" guard on <see cref="SetResult"/>.
        ///
        /// Unlike <see cref="SetResult"/>, this queues a `running`-status update immediately --
        /// there is no separate "progress" status on the wire, `running` is reused for both the
        /// initial pickup acknowledgement and every progress update.
        /// </summary>
        /// <param name="message">Human-readable progress message.</param>
        public void ReportProgress(string message)
        {
            if (message == null) throw new ArgumentNullException(nameof(message));
            _sink.QueueActionUpdate(new ActionUpdate
            {
                ExternalId = ExternalId,
                Status = ActionStatus.running,
                ResultMessage = message,
            });
        }
    }

    /// <summary>
    /// A custom action registered by an extractor via <see cref="BaseExtractor{TConfig}.RegisterAction"/>.
    /// </summary>
    /// <typeparam name="TConfig">Extractor configuration type.</typeparam>
    public sealed class CustomAction<TConfig>
    {
        /// <summary>
        /// Name of the action, must be unique among all actions (custom and auto-generated
        /// Start/Stop) registered on the extractor.
        /// </summary>
        public string Name { get; }

        /// <summary>
        /// Human-readable description of what the action does, reported to integrations during
        /// startup.
        /// </summary>
        public string? Description { get; }

        /// <summary>
        /// The callback invoked when this action is triggered.
        /// </summary>
        public Func<ActionContext<TConfig>, CancellationToken, Task> Target { get; }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="name">Name of the action, must be unique per extractor.</param>
        /// <param name="target">Callback invoked when this action is triggered.</param>
        /// <param name="description">Human-readable description of what the action does.</param>
        public CustomAction(string name, Func<ActionContext<TConfig>, CancellationToken, Task> target, string? description = null)
        {
            if (string.IsNullOrEmpty(name)) throw new ArgumentException("Action name must be set", nameof(name));
            Name = name;
            Target = target ?? throw new ArgumentNullException(nameof(target));
            Description = description;
        }
    }
}
