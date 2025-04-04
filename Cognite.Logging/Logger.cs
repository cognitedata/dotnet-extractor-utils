using System;
using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Serilog;
using Serilog.Core;
using Serilog.Events;
using Serilog.Extensions.Logging;

namespace Cognite.Extractor.Logging
{

    /// <summary>
    /// Utility class for configuring extractor loggers.
    /// The logging framework used is <see href="https://serilog.net/">Serilog</see>.
    /// Loggers are created according to a <see cref="LoggerConfig"/> configuration object.
    /// Log messages contain UTC timestamps.
    /// </summary>
    public static class LoggingUtils
    {
        /// <summary>
        /// Default logging template without context.
        /// </summary>
        public const string LogTemplate = "[{UtcTimestamp:yyyy-MM-dd HH:mm:ss.fff} {Level:u3}] {Message:lj}{NewLine}{Exception}";

        /// <summary>
        /// Default logging template with context.
        /// </summary>
        public const string LogTemplateWithContext = "[{UtcTimestamp:yyyy-MM-dd HH:mm:ss.fff} {Level:u3}] [{SourceContext}] {Message:lj}{NewLine}{Exception}";

        /// <summary>
        /// Configure Serilog's shared logger according to the configuration in <paramref name="config"/>.
        /// Use this method only when the static <see cref="Serilog.Log"/> is used by the application for logging
        /// </summary>
        /// <param name="config">Configuration object</param>
        public static void Configure(LoggerConfig config)
        {
            Log.Logger = GetConfiguredLogger(config);
        }

        /// <summary>
        /// Creates <see cref="LoggerConfiguration" /> according to the configuration in <paramref name="config"/>
        /// </summary>
        /// <param name="config">Configuration object of <see cref="LoggerConfig"/> type</param>
        /// <returns>A configured logger</returns>
        public static LoggerConfiguration GetConfiguration(LoggerConfig config)
        {
            if (config == null)
            {
                throw new ArgumentNullException(nameof(config));
            }
            var logToConsole = Enum.TryParse(config.Console?.Level, true, out LogEventLevel consoleLevel);
            var logToFile = Enum.TryParse(config.File?.Level, true, out LogEventLevel fileLevel);
            var logToStderr = Enum.TryParse(config.Console?.StderrLevel, true, out LogEventLevel stderrLevel);

            var logConfig = new LoggerConfiguration();
            logConfig
                .Enrich.With<UtcTimestampEnricher>()
                .MinimumLevel.Verbose()
                .MinimumLevel.Override("System", LogEventLevel.Error)
                .MinimumLevel.Override("Microsoft", LogEventLevel.Error);

            if (logToConsole)
            {
                logConfig.WriteTo.Console(consoleLevel,
                    consoleLevel <= LogEventLevel.Debug ? LogTemplateWithContext : LogTemplate,
                    standardErrorFromLevel: logToStderr ? (LogEventLevel?)stderrLevel : null);
            }

            if (logToFile && config.File!.Path != null)
            {
                RollingInterval ri = RollingInterval.Day;
                if (config.File.RollingInterval == "hour")
                {
                    ri = RollingInterval.Hour;
                }
                logConfig.WriteTo.Async(p => p.File(
                    config.File.Path,
                    rollingInterval: ri,
                    retainedFileCountLimit: config.File.RetentionLimit,
                    restrictedToMinimumLevel: fileLevel,
                    outputTemplate: fileLevel <= LogEventLevel.Debug ? LogTemplateWithContext : LogTemplate));
            }

            return logConfig;
        }

        /// <summary>
        /// Creates a <see cref="Serilog.ILogger"/> logger according to the configuration in <paramref name="config"/>
        /// </summary>
        /// <param name="config">Configuration object of <see cref="LoggerConfig"/> type</param>
        /// <returns>A configured logger</returns>
        public static Serilog.ILogger GetConfiguredLogger(LoggerConfig config)
        {
            var logConfig = GetConfiguration(config);
            return logConfig.CreateLogger();
        }

        /// <summary>
        /// Create a default console logger and returns it.
        /// </summary>
        /// <returns>A <see cref="Microsoft.Extensions.Logging.ILogger"/> logger with default properties</returns>
        public static Microsoft.Extensions.Logging.ILogger GetDefault()
        {
            using (var loggerFactory = new LoggerFactory())
            {
                loggerFactory.AddSerilog(GetSerilogDefault(), true);
                return loggerFactory.CreateLogger("default");
            }
        }

        /// <summary>
        /// Create a default Serilog console logger and returns it.
        /// </summary>
        /// <returns>A <see cref="Serilog.ILogger"/> logger with default properties</returns>
        public static Serilog.ILogger GetSerilogDefault()
        {
            return new LoggerConfiguration()
                .Enrich.With<UtcTimestampEnricher>()
                .WriteTo.Console(LogEventLevel.Information, LogTemplate)
                .CreateLogger();
        }

    }

    // Enricher that creates a property with UTC timestamp.
    // See: https://github.com/serilog/serilog/issues/1024#issuecomment-338518695
    class UtcTimestampEnricher : ILogEventEnricher
    {
        public void Enrich(LogEvent logEvent, ILogEventPropertyFactory lepf)
        {
            logEvent.AddPropertyIfAbsent(
                lepf.CreateProperty("UtcTimestamp", logEvent.Timestamp.UtcDateTime));
        }
    }

    /// <summary>
    /// This class implements a <see cref="TraceListener"/> that, when configured, writes trace messages to the injected 
    /// logger
    /// </summary>
    public class LoggerTraceListener : TraceListener
    {
        private readonly ILogger<LoggerTraceListener> _logger;
        private readonly LogEventLevel? _level;

        /// <summary>
        /// Creates a new listener using the logger and configuration passed as parameters
        /// </summary>
        /// <param name="logger">Logger</param>
        /// <param name="config">Logger configuration</param>
        public LoggerTraceListener(ILogger<LoggerTraceListener> logger, LoggerConfig config)
        {
            if (config == null)
            {
                throw new ArgumentNullException(nameof(config));
            }
            if (logger == null)
            {
                throw new ArgumentNullException(nameof(logger));
            }
            _logger = logger;
            if (Enum.TryParse(config.TraceListener?.Level, true, out LogEventLevel le))
            {
                _level = le;
            }
        }

        /// <summary>
        /// Writes a trace message using the configured logger
        /// </summary>
        /// <param name="message">Trace message</param>
        public override void Write(string? message)
        {
            WriteLine(message);
        }

        /// <summary>
        /// Writes a trace message using the configured logger
        /// </summary>
        /// <param name="message">Trace message</param>
        public override void WriteLine(string? message)
        {
            if (_level is null) return;
            switch (_level)
            {
                case LogEventLevel.Verbose:
                    _logger.LogTrace(message);
                    break;
                case LogEventLevel.Debug:
                    _logger.LogDebug(message);
                    break;
                case LogEventLevel.Information:
                    _logger.LogInformation(message);
                    break;
                case LogEventLevel.Warning:
                    _logger.LogWarning(message);
                    break;
                case LogEventLevel.Error:
                    _logger.LogError(message);
                    break;
                case LogEventLevel.Fatal:
                    _logger.LogCritical(message);
                    break;
                default:
                    break;
            }
        }

        /// <summary>
        /// Enable this Trace listener, so that trace messages are outputted by the logger
        /// </summary>
        public void Enable()
        {
            if (_level != null)
            {
                Trace.Listeners.Add(this);
                _logger.LogDebug("Outputting {Trace} messages as {level} logs", typeof(Trace).Name, _level);
            }
        }
    }

    /// <summary>
    /// Extension utilities for logging
    /// </summary>
    public static class LoggingExtensions
    {

        /// <summary>
        /// Adds a configured Serilog logger as singletons of the <see cref="Microsoft.Extensions.Logging.ILogger"/> and
        /// <see cref="Serilog.ILogger"/> types to the <paramref name="services"/> collection.
        /// A configuration object of type <see cref="LoggerConfig"/> is required, and should have been added to the
        /// collection as well.
        /// </summary>
        /// <param name="services">The service collection</param>
        /// <param name="buildLogger">Method to build the logger.
        /// <param name="alternativeLogger">True to allow alternative loggers, i.e. allow config.Console and config.File to be null</param>
        /// This defaults to <see cref="LoggingUtils.GetConfiguredLogger(LoggerConfig)"/>,
        /// which creates logging configuration for file and console using
        /// <see cref="LoggingUtils.GetConfiguration(LoggerConfig)"/></param>
        public static void AddLogger(this IServiceCollection services, Func<LoggerConfig, Serilog.ILogger>? buildLogger = null, bool alternativeLogger = false)
        {
            buildLogger ??= LoggingUtils.GetConfiguredLogger;
            services.AddSingleton<LoggerTraceListener>();
            services.AddSingleton<Serilog.ILogger>(p =>
            {
                var config = p.GetService<LoggerConfig>();
                if (config == null || !alternativeLogger && (config.Console == null && config.File == null))
                {
                    // No logging configuration
                    var defLog = LoggingUtils.GetSerilogDefault();
                    defLog.Warning("No Logging configuration found. Using default logger");
                    return defLog;
                }
                return buildLogger(config);
            });
            services.AddLogging(loggingBuilder =>
            {
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