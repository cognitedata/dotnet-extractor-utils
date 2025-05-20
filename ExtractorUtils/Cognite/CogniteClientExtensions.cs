using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Prometheus;
using CogniteSdk;
using CogniteSdk.Login;
using CogniteSdk.Token;
using Cognite.Extractor.Common;
using Cognite.Extensions;

namespace Cognite.Extractor.Utils
{
    /// <summary>
    /// Extension utility methods for <see cref="Client"/>
    /// </summary>
    public static class CogniteClientExtensions
    {
        private static Summary tokenSummary { get; } = Prometheus.Metrics.CreateSummary(
            "extractor_utils_cdf_token_requests",
            "Number and duration of token requests to CDF", "endpoint");

        /// <summary>
        /// Verifies that the <paramref name="client"/> configured according to <paramref name="config"/>
        /// can access Cognite Data Fusion
        /// </summary>
        /// <param name="client">Cognite SDK client</param>
        /// <param name="config">Configuration object</param>
        /// <param name="token">Cancellation token</param>
        /// <exception cref="CogniteUtilsException">Thrown when credentials are invalid
        /// or the client cannot be used to access CDF resources</exception>
        public async static Task TestCogniteConfig(this Client client, CogniteConfig config, CancellationToken token)
        {
            if (config == null)
            {
                throw new CogniteUtilsException("Cognite configuration missing");
            }

            await TestCogniteConfig(client, config.Project!, token).ConfigureAwait(false);
        }

        /// <summary>
        /// Verifies that the <paramref name="client"/> configured with <paramref name="project"/>
        /// can access Cognite Data Fusion
        /// </summary>
        /// <param name="client">Cognite SDK client</param>
        /// <param name="project">Configured project</param>
        /// <param name="token">Cancellation token</param>
        /// <param name="checkProjectOwnership">If true, check if the token has access to the project. Note
        /// that this check requires projects:list and groups:list.</param>
        /// <exception cref="CogniteUtilsException">Thrown when credentials are invalid
        /// or the client cannot be used to access CDF resources</exception>
        public async static Task TestCogniteConfig(this Client client, string project, CancellationToken token, bool checkProjectOwnership = true)
        {
            if (project?.TrimToNull() == null)
            {
                throw new CogniteUtilsException("CDF project is not configured");
            }

            TokenInspect tokenInspect;
            using (tokenSummary.WithLabels("inspect").NewTimer())
            {
                tokenInspect = await client.Token.InspectAsync(token).ConfigureAwait(false);
            }
            if (checkProjectOwnership && (tokenInspect.Projects == null || !tokenInspect.Projects.Any(p => p.ProjectUrlName == project)))
            {
                throw new CogniteUtilsException($"CDF credentials are not associated with project {project}, or the token lacks projects:list and groups:list permissions");
            }
        }
    }
}