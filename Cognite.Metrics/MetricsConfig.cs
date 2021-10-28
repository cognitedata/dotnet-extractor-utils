using System.Collections.Generic;

namespace Cognite.Extractor.Metrics
{

    /// <summary>
    /// Metrics configuration object
    /// </summary>
    public class MetricsConfig
    {
        /// <summary>
        /// Start a metrics server in the extractor for Prometheus scrape (optional)
        /// </summary>
        /// <value>A <see cref="MetricsServerConfig"/> config object</value>
        public MetricsServerConfig? Server { get; set; }
#pragma warning disable CA2227 // Collection properties should be read only

        /// <summary>
        /// A list of Prometheus push gateway destinations (optional)
        /// </summary>
        /// <value>A list <see cref="PushGatewayConfig"/> of config objects</value>
        public IList<PushGatewayConfig>? PushGateways { get; set; }
#pragma warning restore CA2227 // Collection properties should be read only

    }

    /// <summary>
    /// Metrics server configuration
    /// </summary>
    public class MetricsServerConfig
    {
        /// <summary>
        /// Host name
        /// </summary>
        /// <value>Host name for the server. Example: localhost (without scheme and port)</value>
        public string? Host { get; set; }
        /// <summary>
        /// Port
        /// </summary>
        /// <value>Server port</value>
        public int Port { get; set; }
    }

    /// <summary>
    /// Push gateway configuration
    /// </summary>
    public class PushGatewayConfig
    {
        /// <summary>
        /// Gateway host
        /// </summary>
        /// <value>Absolute Uri for the host. Example: http://localhost:9091</value>
        public string? Host { get; set; }
        /// <summary>
        /// Job name
        /// </summary>
        /// <value>Name of the job</value>
        public string? Job { get; set; }
        /// <summary>
        /// Username for basic authentication (optional)
        /// </summary>
        /// <value>Username</value>
        public string? Username { get; set; }
        /// <summary>
        /// Password for basic authentication (optional)
        /// </summary>
        /// <value>Password</value>
        public string? Password { get; set; }
        /// <summary>
        /// Interval in seconds for pushing data to the gateway.
        /// </summary>
        /// <value></value>
        public int PushInterval { get; internal set; } = 1;
    }

}
