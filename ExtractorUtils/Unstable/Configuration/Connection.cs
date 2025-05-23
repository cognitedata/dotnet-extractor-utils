using System;
using System.Collections.Generic;
using Cognite.Extensions.Unstable;
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
        public string? Integration { get; set; }

        /// <summary>
        /// Configuration for retries of failed requests to CDF.
        /// </summary>
        public RetryConfig CdfRetries { get => _cdfRetries; set { _cdfRetries = value ?? _cdfRetries; } }
        private RetryConfig _cdfRetries = new RetryConfig();

        /// <summary>
        /// Enables logging of Cognite Sdk operations. Enabled by default.
        /// </summary>
        public SdkLoggingConfig SdkLogging { get => _sdkLogging; set { _sdkLogging = value ?? _sdkLogging; } }
        private SdkLoggingConfig _sdkLogging = new SdkLoggingConfig();

        /// <summary>
        /// Configuration for handling SSL certificates.
        /// </summary>
        public CertificateConfig? Certificates { get; set; }

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