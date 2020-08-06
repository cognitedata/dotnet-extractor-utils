using System.Threading;
using System.Threading.Tasks;
using CogniteSdk;
using Cognite.Extractor.Common;
using Prometheus;
using CogniteSdk.Login;
using Cognite.Extensions;

namespace Cognite.Extractor.Utils
{
    /// <summary>
    /// Extension utility methods for <see cref="Client"/>
    /// </summary>
    public static class CogniteClientExtensions
    {
        private static Summary login { get; } = Prometheus.Metrics.CreateSummary("extractor_utils_cdf_login_requests",
            "Number and duration of login requests to CDF", "endpoint");

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
            if (config == null) {
                throw new CogniteUtilsException("Cognite configuration missing");
            }
            
            if (config?.Project?.TrimToNull() == null)
            {
                throw new CogniteUtilsException("CDF project is not configured");
            }

            LoginStatus loginStatus;
            using (login.WithLabels("status").NewTimer())
            {
                loginStatus = await client.Login.StatusAsync(token);
            }
            if (!loginStatus.LoggedIn)
            {
                throw new CogniteUtilsException("CDF credentials are invalid");
            }
            if (!loginStatus.Project.Equals(config.Project))
            {
                throw new CogniteUtilsException($"CDF credentials are not associated with project {config.Project}");
            }
        }
    }
}