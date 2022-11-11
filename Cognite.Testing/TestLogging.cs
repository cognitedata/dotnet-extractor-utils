using Cognite.Extractor.Logging;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Serilog;
using Serilog.Configuration;
using Serilog.Core;
using Serilog.Events;
using Serilog.Formatting;
using Serilog.Formatting.Display;
using System;
using System.IO;
using System.Runtime.CompilerServices;
using Xunit.Abstractions;

namespace Cognite.Extractor.Testing
{
    /// <summary>
    /// Serilog sink for ITestOutputHelper
    /// </summary>
    public class TestOutputSink : ILogEventSink
    {
        /// <summary>
        /// Current test output helper.
        /// </summary>
        public ITestOutputHelper? Output { get; set; }

        private readonly ITextFormatter _format;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="output">Test output to write to</param>
        /// <param name="format">Text formatter from serilog</param>
        public TestOutputSink(ITestOutputHelper output, ITextFormatter format)
        {
            Output = output;
            _format = format;
        }

        /// <summary>
        /// Constructor without initially setting the test output helper.
        /// </summary>
        /// <param name="format">Text formatter from serilog</param>
        public TestOutputSink(ITextFormatter format)
        {
            _format = format;
        }

        /// <summary>
        /// Write logEvent to the test output helper.
        /// </summary>
        /// <param name="logEvent">LogEvent to write</param>
        public void Emit(LogEvent logEvent)
        {
            using var writer = new StringWriter();
            _format.Format(logEvent, writer);
            Output?.WriteLine(writer.ToString().Trim());
        }
    }

    /// <summary>
    /// Utils for logging during tests.
    /// </summary>
    public static class TestLogging
    {
        /// <summary>
        /// Add a serilog output writing to an xunit test output.
        /// </summary>
        /// <param name="sinkConfig">Serilog config</param>
        /// <param name="output">Test output to write to</param>
        /// <param name="restrictedToMinimumLevel">Write to this sink only if above this level</param>
        /// <param name="template">Logging template</param>
        /// <param name="formatProvider">Logging formatter</param>
        /// <param name="levelSwitch">Optional level switch to allow changing the log level at runtime</param>
        /// <returns></returns>
        public static LoggerConfiguration TestOutputHelper(
            this LoggerSinkConfiguration sinkConfig,
            ITestOutputHelper output,
            LogEventLevel restrictedToMinimumLevel = LevelAlias.Minimum,
            string template = LoggingUtils.LogTemplateWithContext,
            IFormatProvider? formatProvider = null,
            LoggingLevelSwitch? levelSwitch = null)
        {
            var formatter = new MessageTemplateTextFormatter(template, formatProvider);

            return sinkConfig.Sink(new TestOutputSink(output, formatter), restrictedToMinimumLevel, levelSwitch);
        }

        /// <summary>
        /// Register a logger that also writes to a test output helper.
        /// </summary>
        /// <param name="services">Service collection to add to</param>
        /// <param name="output">Test output</param>
        public static void AddTestLogging(
            this ServiceCollection services,
            ITestOutputHelper output)
        {
            services.AddLogger(cfg =>
            {
                return LoggingUtils.GetConfiguration(cfg)
                    .WriteTo.TestOutputHelper(output)
                    .CreateLogger();
            }, true);
        }

        /// <summary>
        /// Register a logger with external sink that can be modified on test start.
        /// This is intended for use with collection or class fixtures. This will _not_ work if tests are run in parellel.
        /// To use: replace the test output when instantiating the test class.
        /// </summary>
        /// <param name="services"></param>
        /// <param name="restrictedToMinimumLevel"></param>
        /// <param name="template"></param>
        /// <param name="formatProvider"></param>
        /// <param name="levelSwitch"></param>
        /// <returns>The configured sink</returns>
        public static TestOutputSink AddMultiTestLogging(
            this ServiceCollection services,
            LogEventLevel restrictedToMinimumLevel = LevelAlias.Minimum,
            string template = LoggingUtils.LogTemplateWithContext,
            IFormatProvider? formatProvider = null,
            LoggingLevelSwitch? levelSwitch = null)
        {
            var formatter = new MessageTemplateTextFormatter(template, formatProvider);
            var sink = new TestOutputSink(formatter);

            services.AddLogger(cfg =>
            {
                return LoggingUtils.GetConfiguration(cfg).WriteTo
                    .Sink(sink, restrictedToMinimumLevel, levelSwitch)
                    .CreateLogger();
            });

            return sink;
        }

        /// <summary>
        /// Return a simple test logger.
        /// </summary>
        /// <returns></returns>
        public static ILogger<T> GetTestLogger<T>(ITestOutputHelper output)
        {
            var services = new ServiceCollection();
            services.AddSingleton(new LoggerConfig { Console = new ConsoleConfig() });
            services.AddTestLogging(output);
            return services.BuildServiceProvider().GetRequiredService<ILogger<T>>();
        }
    }

    /// <summary>
    /// Simple abstract base class for fixtures containing a log sink.
    /// </summary>
    public abstract class LoggingTestFixture
    {
        /// <summary>
        /// Output sink.
        /// </summary>
        protected TestOutputSink? Sink { get; private set; }

        /// <summary>
        /// Initialize the output sink with a given output helper.
        /// </summary>
        /// <param name="output"></param>
        /// <exception cref="InvalidOperationException">If sink is null.</exception>
        public virtual void Init(ITestOutputHelper output)
        {
            if (Sink == null) throw new InvalidOperationException("Sink must be set before calling Init");
            Sink.Output = output;
        }

        /// <summary>
        /// Configure the service, setting the sink.
        /// </summary>
        /// <param name="services">Servicecollection to add loggign to.</param>
        /// <param name="minimumLevel">Write to this sink only if above this level</param>
        protected void Configure(ServiceCollection services, LogEventLevel minimumLevel = LogEventLevel.Debug)
        {
            Sink = services.AddMultiTestLogging(minimumLevel);
        }
    }
}
