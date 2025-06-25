using System;
using System.Collections.Generic;
using Cognite.Extensions.Unstable;
using Cognite.Extractor.Common;
using Cognite.Extractor.Configuration;
using Microsoft.Extensions.Logging;

namespace Cognite.Extractor.Utils.Unstable.Configuration
{
    /// <summary>
    /// Configuration for connecting to CDF.
    /// </summary>
    public class ConnectionConfig : VersionedConfig
    {
        /// <summary>
        /// The project name
        /// </summary>
        /// <value>project name</value>
        public string? Project { get; set; }

        /// <summary>
        /// API base URL
        /// </summary>
        /// <value>Absolute Uri for the host. Default: https://api.cognitedata.com</value>
        public string BaseUrl { get; set; } = "https://api.cognitedata.com";

        /// <summary>
        /// Authentication config.
        /// </summary>
        public BaseAuthenticationConfig? Authentication { get; set; }

        /// <summary>
        /// ID of integration in CDF, required.
        /// </summary>
        public IntegrationConfig? Integration { get; set; }

        /// <summary>
        /// Configuration for the connection to CDF.
        /// </summary>
        public CdfConnectionConfig CdfConnection { get => _cdfConnection; set { _cdfConnection = value ?? _cdfConnection; } }

        private CdfConnectionConfig _cdfConnection = new CdfConnectionConfig();

        /// <summary>
        /// Register any necessary yaml converters.
        /// </summary>
        public static void RegisterConverters(YamlConfigBuilder builder)
        {
            if (builder is null) throw new ArgumentNullException(nameof(builder));

            builder.AddDiscriminatedType<BaseAuthenticationConfig>("type", BaseAuthenticationConfig.Variants());
        }

        /// <inheritdoc />
        public override void GenerateDefaults()
        {
        }
    }

    /// <summary>
    /// Configure automatic retries on requests to CDF.
    /// </summary>
    public class RetryConfig
    {
        /// <summary>
        /// Timeout in milliseconds for each individual try. Less than or equal to zero for no timeout.
        /// </summary>
        public string Timeout { get => TimeoutValue.RawValue; set => TimeoutValue.RawValue = value; }
        /// <summary>
        /// Value of the timeout parameter.
        /// </summary>
        public TimeSpanWrapper TimeoutValue { get; } = new TimeSpanWrapper(false, "ms", "80s");
        /// <summary>
        /// Maximum number of retries. Less than 0 retries forever.
        /// </summary>
        public int MaxRetries { get; set; } = 5;

        /// <summary>
        /// Max backoff in ms between each retry. Base delay is calculated according to 125*2^retry ms.
        /// If less than 0, there is no maximum.
        /// </summary>
        public string MaxBackoff { get => MaxBackoffValue.RawValue; set => MaxBackoffValue.RawValue = value; }
        /// <summary>
        /// Value of the max-backoff parameter.
        /// </summary>
        public TimeSpanWrapper MaxBackoffValue { get; } = new TimeSpanWrapper(true, "ms", "80s");
    }

    /// <summary>
    /// Shared configuration for configuring the connection to CDF.
    /// </summary>
    public class CdfConnectionConfig
    {
        /// <summary>
        /// Configuration for retries of failed requests to CDF.
        /// </summary>
        public RetryConfig Retries { get => _retries; set { _retries = value ?? _retries; } }
        private RetryConfig _retries = new RetryConfig();

        /// <summary>
        /// Configuration for details around verification of SSL certificates.
        /// </summary>
        public CertificateConfig SslCertificates { get => _sslCertificates; set { _sslCertificates = value ?? _sslCertificates; } }
        private CertificateConfig _sslCertificates = new CertificateConfig();
    }

    /// <summary>
    /// Configure options relating to SSL certificates.
    /// </summary>
    public class CertificateConfig
    {
        /// <summary>
        /// False to disable SSL verification. This must be considered a security risk in most circumstances.
        /// The default value is true.
        /// </summary>
        public bool Verify { get; set; } = true;
        /// <summary>
        /// List of certificate thumbprints to manually allow. This is much safer.
        /// </summary>
        public IEnumerable<string>? AllowList { get; set; }
    }

    /// <summary>
    /// Configuration for setting the integration in CDF.
    /// </summary>
    public class IntegrationConfig
    {
        /// <summary>
        /// External ID of the integration.
        /// </summary>
        public string? ExternalId { get; set; }
    }

    /// <summary>
    /// Configuration for logging information from the SDK.
    /// </summary>
    public class SdkLoggingConfig
    {
        /// <summary>
        /// Disables Sdk logging 
        /// </summary>
        /// <value></value>
        public bool Enabled { get; set; } = true;

        /// <summary>
        /// Cognite Sdk logs are diplayed using this level.
        /// </summary>
        /// <value>One of the <see cref="LogLevel"/> levels, case insensitive</value>
        public LogLevel Level { get; set; } = LogLevel.Debug;

        /// <summary>
        /// Format of the log message.
        /// Default is <c>"CDF ({Message}): {HttpMethod} {Url} {ResponseHeader[X-Request-ID]} - {Elapsed} ms"</c>
        /// </summary>
        /// <returns>String format</returns>
        public string Format { get; set; } = "CDF ({Message}): {HttpMethod} {Url} {ResponseHeader[X-Request-ID]} - {Elapsed} ms";

    }

}