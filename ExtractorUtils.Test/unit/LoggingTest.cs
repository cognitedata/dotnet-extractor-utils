using System.Diagnostics;
using System;
using System.IO;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Xunit;
using Cognite.Extractor.Logging;
using Cognite.Extractor.Utils;

namespace ExtractorUtils.Test.Unit
{
    class TestLoggingConfig : BaseConfig
    {
        public string Foo { get; set; } = "";
    }

    public sealed class LoggingTest
    {

        [Fact]
        public static void TestLogging()
        {
            // To log messages before a logger configured, the default console logger can be used.
            var l1 = LoggingUtils.GetDefault();
            Assert.NotNull(l1);
            l1.LogInformation("Testing logger injection");

            string path = "test-logging-config.yml";
            string[] lines = {  "version: 2",
                                "logger:",
                                "  console:",
                                "    level: verbose",
                                "  file:",
                                "    level: information",
                               @"    path: logs/log.txt",
                                "    rolling-interval: day",
                                "  trace-listener:",
                                "    level: warning" };
            File.WriteAllLines(path, lines);

            l1.LogInformation("Adding Configuration and Logging services...");
            var services = new ServiceCollection();
            services.AddConfig<TestLoggingConfig>(path, 2);
            services.AddLogger();

            using (var provider = services.BuildServiceProvider())
            {
                l1.LogInformation("Getting configuration singleton object");
                var config = provider.GetRequiredService<TestLoggingConfig>();
                Assert.Equal(2, config.Version);
                Assert.Equal("information", config.Logger.File.Level);
                Assert.Equal(@"logs/log.txt", config.Logger.File.Path);

                l1.LogInformation("Getting logger implementations");
                var serilogLogger = provider.GetRequiredService<Serilog.ILogger>();
                Assert.NotNull(serilogLogger);
                var microsoftLogger = provider.GetRequiredService<Microsoft.Extensions.Logging.ILogger<LoggingTest>>();
                Assert.NotNull(microsoftLogger);

                l1.LogInformation("Printing log messages");
                serilogLogger.Information("This is a information log from {LogType} logger", serilogLogger.GetType());
                microsoftLogger.LogInformation("This is a information log from {LogType} logger", microsoftLogger.GetType());

                serilogLogger.Verbose("This log message is not in the log file"); // but should be seen in Console
                microsoftLogger.LogTrace("This log message is not in the log file");

                var traceListener = provider.GetService<LoggerTraceListener>();
                traceListener.Enable();

                Trace.WriteLine("This is a trace message");
            }

            l1.LogInformation("Verifying that the log file exists and contains INFO logs");
            var logfile = $@"logs/log{DateTime.Now:yyyyMMdd}.txt";
            Assert.True(File.Exists(logfile));
            using (StreamReader r = new StreamReader(logfile))
            {
                string line1 = r.ReadLine();
                Assert.Contains(" INF] ", line1);
                Assert.Contains("This is a information log from Serilog.Core.Logger", line1);
                string line2 = r.ReadLine();
                Assert.Contains(" INF] ", line2);
                Assert.Contains("This is a information log from Microsoft.Extensions.Logging.Logger", line2);
                string line3 = r.ReadLine();
                Assert.Contains(" WRN] ", line3);
                Assert.Contains("This is a trace message", line3);
                Assert.Null(r.ReadLine()); // only information level and above should be printed
            }
            Directory.Delete("logs", true);
            File.Delete(path);
        }
    }
}