using Cognite.Extractor.Logging;
using Microsoft.Extensions.DependencyInjection;
using Serilog;
using Serilog.Configuration;
using Serilog.Core;
using Serilog.Events;
using Serilog.Formatting;
using Serilog.Formatting.Display;
using System;
using System.IO;
using Xunit.Abstractions;

namespace Cognite.Extractor.Testing
{
    /// <summary>
    /// Serilog sink for ITestOutputHelper
    /// </summary>
    public class TestOutputSink : ILogEventSink
    {
        private readonly ITextFormatter _format;
        private readonly ITestOutputHelper _output;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="output">Test output to write to</param>
        /// <param name="format">Text formatter from serilog</param>
        public TestOutputSink(ITestOutputHelper output, ITextFormatter format)
        {
            _output = output;
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
            _output.WriteLine(writer.ToString().Trim());
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
            });
        }

    }
}
