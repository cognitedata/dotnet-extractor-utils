using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Serilog;
using Serilog.Core;
using Serilog.Events;
using Serilog.Extensions.Logging;
using Serilog.Sinks.GoogleCloudLogging;

namespace ExtractorUtils {
    /// <summary>
    /// Utility class for configuring extractor loggers.
    /// The logging framework used is <see href="https://serilog.net/">Serilog</see>.
    /// Loggers are created according to a <see cref="LoggerConfig"/> configuration object.
    /// Log messages contain UTC timestamps.
    /// </summary>
    public static class Logging {
        
        /// <summary>
        /// Configure Serilog's shared logger according to the configuration in <paramref name="config"/>.
        /// </summary>
        /// <param name="config">Configuration object</param>
        public static void Configure(LoggerConfig config)
        {
            var logToConsole = Enum.TryParse(config.Console?.Level, true, out LogEventLevel consoleLevel);
            var logToFile = Enum.TryParse(config.File?.Level, true, out LogEventLevel fileLevel);
            var logToStackdriver = config.Stackdriver?.Credentials != null;

            var logConfig = new LoggerConfiguration();
            logConfig
                .Enrich.With<UtcTimestampEnricher>()
                .MinimumLevel.Verbose()
                .MinimumLevel.Override("System", LogEventLevel.Error)
                .MinimumLevel.Override("Microsoft", LogEventLevel.Error);

            var outputTemplate = "[{UtcTimestamp:HH:mm:ss} {Level:u3}] {Message:lj}{NewLine}{Exception}";
            var outputTemplateDebug = "[{UtcTimestamp:yyyy-MM-dd HH:mm:ss.fff zzz} {Level:u3}] [{SourceContext}] {Message:lj}{NewLine}{Exception}";

            if (logToConsole)
            {
                logConfig.WriteTo.Console(consoleLevel, consoleLevel <= LogEventLevel.Debug 
                    ? outputTemplateDebug : outputTemplate);
            }

            if (logToFile && config.File.Path != null)
            {
                RollingInterval ri = RollingInterval.Day;
                if (config.File.RollingInterval == "hour") {
                    ri = RollingInterval.Hour;
                }
                logConfig.WriteTo.Async(p => p.File(
                    config.File.Path,
                    rollingInterval: ri,
                    retainedFileCountLimit: config.File.RetentionLimit,
                    restrictedToMinimumLevel: fileLevel,
                    outputTemplate: fileLevel <= LogEventLevel.Debug ? outputTemplateDebug : outputTemplate));
            }

            if (logToStackdriver)
            {
                using (StreamReader r = new StreamReader(config.Stackdriver.Credentials))
                {
                    string json = r.ReadToEnd();
                    var jsonObj = JsonSerializer.Deserialize<GcpCredentials>(json);

                    var resourceLabels = new Dictionary<string, string>
                    {
                        { "email_id", jsonObj.ClientEmail },
                        { "unique_id", jsonObj.ClientId }
                    };

                    var gcConfig = new GoogleCloudLoggingSinkOptions(
                        jsonObj.ProjectId,
                        jsonObj.ResourceType,
                        config.Stackdriver.LogName,
                        resourceLabels: resourceLabels,
                        useJsonOutput: true,
                        googleCredentialJson: json);
                    logConfig.WriteTo.GoogleCloudLogging(gcConfig);
                }
            }

            Log.Logger = logConfig.CreateLogger();
        }

        /// <summary>
        /// Create a default console logger and returns it.
        /// </summary>
        /// <returns>A <see cref="Microsoft.Extensions.Logging.ILogger"/> logger with default properties</returns>
        public static Microsoft.Extensions.Logging.ILogger GetDefault() {
            var loggerFactory = new LoggerFactory();
            loggerFactory.AddSerilog(GetSerilogDefault(), true);
            return loggerFactory.CreateLogger("default");
        }

        /// <summary>
        /// Create a default Serilog console logger and returns it.
        /// </summary>
        /// <returns>A <see cref="Serilog.ILogger"/> logger with default properties</returns>
        public static Serilog.ILogger GetSerilogDefault() {
            return new LoggerConfiguration().WriteTo.Console().CreateLogger();
        }

#pragma warning disable CA1812 // Internal class
        private class GcpCredentials
#pragma warning restore CA1812 // Internal class
        {
            [JsonPropertyName("project_id")]
            public string ProjectId { get; set; }

            [JsonPropertyName("type")]
            public string ResourceType { get; set; }

            [JsonPropertyName("client_email")]
            public string ClientEmail { get; set; }

            [JsonPropertyName("client_id")]
            public string ClientId { get; set; }
        }        
    }

    // Enricher that creates a property with UTC timestamp.
    // See: https://github.com/serilog/serilog/issues/1024#issuecomment-338518695
    class UtcTimestampEnricher : ILogEventEnricher {
        public void Enrich(LogEvent logEvent, ILogEventPropertyFactory lepf) {
            logEvent.AddPropertyIfAbsent(
                lepf.CreateProperty("UtcTimestamp", logEvent.Timestamp.UtcDateTime));
        }
    }

    /// <summary>
    /// Extension utilities for logging
    /// </summary>
    public static class LoggingExtensions {
        
        /// <summary>
        /// Adds a configured Serilog logger as singletons of the <see cref="Microsoft.Extensions.Logging.ILogger"/> and
        /// <see cref="Serilog.ILogger"/> types to the <paramref name="services"/> collection.
        /// A configuration object of type <see cref="BaseConfig"/> is required, and should have been added to the
        /// collection as well.
        /// </summary>
        /// <param name="services">The service collection</param>
        public static void AddLogger(this IServiceCollection services) {
            services.AddSingleton<Serilog.ILogger>(s => {
                var config = s.GetRequiredService<BaseConfig>();
                if (config.Logger == null) {
                    // No logging configuration
                    var defLog = Logging.GetSerilogDefault();
                    defLog.Warning("No Logging configuration found. Using default logger");
                    return defLog;
                }
                Logging.Configure(config.Logger);
                return Log.Logger;
            });
            services.AddLogging(loggingBuilder => {
                loggingBuilder.Services.AddSingleton<ILoggerProvider, SerilogLoggerProvider>(s => 
                {
                    var logger = s.GetRequiredService<Serilog.ILogger>();
                    return new SerilogLoggerProvider(logger, true);
                });
                loggingBuilder.AddFilter<SerilogLoggerProvider>(null, LogLevel.Trace);
            });
        }
    }
}