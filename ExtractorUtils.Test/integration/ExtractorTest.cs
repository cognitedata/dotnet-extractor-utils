using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extensions;
using Cognite.Extractor.Testing;
using Cognite.Extractor.Utils;
using CogniteSdk;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Xunit;
using Xunit.Abstractions;

namespace ExtractorUtils.Test.Integration
{
    class TestRawItem
    {
        public string Field { get; set; }
    }

    class MyConfig : BaseConfig
    {
        public string Prefix { get; set; }
    }

    class TestExtractor : BaseExtractor<MyConfig>
    {
        public string TSId { get; private set; }
        public List<string> CreatedEvents { get; private set; }
        public string DBName { get; private set; }
        public string TableName { get; private set; }
        private string _prefix;
        public TestExtractor(MyConfig config, CogniteDestination destination, IServiceProvider provider) : base(config, provider, destination)
        {
            _prefix = config.Prefix;
        }

        protected override async Task Start()
        {
            DBName = $"{_prefix}-test-db";
            TableName = $"{_prefix}-test-table";
            CreateEventQueue(10, TimeSpan.FromSeconds(1), null);
            CreateTimeseriesQueue(10, TimeSpan.FromSeconds(1), null);
            CreateRawQueue<TestRawItem>(DBName, TableName, 10, TimeSpan.FromSeconds(1), null);

            TSId = $"{_prefix}-test-db";

            await Destination.EnsureTimeSeriesExistsAsync(new[]
            {
                new TimeSeriesCreate
                {
                    ExternalId = TSId,
                    Name = "BaseExtractor test TS",
                    IsString = false
                }
            }, RetryMode.OnError, SanitationMode.Clean, Source.Token);

            await Destination.CogniteClient.Raw.CreateTablesAsync(DBName, new[]
            {
                new RawTable { Name = TableName }
            }, true);

            int counter = 0;
            int evtCounter = 0;

            CreatedEvents = new List<string>();

            ScheduleRawRun("raw", DBName, TableName, TimeSpan.FromMilliseconds(100), token =>
            {
                var row = ($"row-{counter}", new TestRawItem { Field = $"field-{counter}" });
                counter++;
                return Task.FromResult<IEnumerable<(string, TestRawItem)>>(new[] { row });
            });

            ScheduleEventsRun("events", TimeSpan.FromMilliseconds(100), token =>
            {
                var evt = new EventCreate
                {
                    ExternalId = $"{_prefix}-event-{evtCounter++}"
                };
                CreatedEvents.Add(evt.ExternalId);
                return Task.FromResult<IEnumerable<EventCreate>>(new[] { evt });
            });

            ScheduleDatapointsRun("datapoints", TimeSpan.FromMilliseconds(100), token =>
            {
                var dp = (Identity.Create(TSId), new Datapoint(DateTime.UtcNow, Math.Sin(DateTime.UtcNow.Ticks)));
                return Task.FromResult<IEnumerable<(Identity, Datapoint)>>(new[] { dp });
            });
        }

        protected override async Task OnStop()
        {
            if (DBName != null)
            {
                await Destination.CogniteClient.Raw.DeleteDatabasesAsync(new RawDatabaseDelete
                {
                    Items = new[]
                {
                        new RawDatabase { Name = DBName }
                    },
                    Recursive = true
                });
            }
            if (TSId != null)
            {
                await Destination.CogniteClient.TimeSeries.DeleteAsync(new TimeSeriesDelete
                {
                    IgnoreUnknownIds = true,
                    Items = new[]
                    {
                            Identity.Create(TSId)
                        }
                });
            }

            if (CreatedEvents.Any())
            {
                await Destination.CogniteClient.Events.DeleteAsync(new EventDelete
                {
                    Items = CreatedEvents.Select(Identity.Create),
                    IgnoreUnknownIds = true
                });
            }
        }
    }

    class NoCdfExtractor : BaseExtractor<MyConfig>
    {
        public bool Started { get; private set; }
        public int Iter { get; private set; }
        public NoCdfExtractor(MyConfig config, IServiceProvider provider) : base(config, provider)
        {
        }

        protected override Task Start()
        {
            Started = true;
            Scheduler.SchedulePeriodicTask(null, TimeSpan.FromMilliseconds(100), token => Iter++);
            Assert.Null(Destination);
            return Task.CompletedTask;
        }
    }

    public class ExtractorTest
    {
        private readonly ITestOutputHelper _output;
        public ExtractorTest(ITestOutputHelper output)
        {
            _output = output;
        }

        [Fact(Timeout = 30000)]
        public async Task TestExtractorRun()
        {
            var configPath = "test-config-base-extractor";
            var config = CDFTester.GetConfig(CogniteHost.BlueField);

            const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
            Random random = new();
            var prefix = "net-utils-test-" + new string(Enumerable.Repeat(chars, 5)
              .Select(s => s[random.Next(s.Length)]).ToArray());

            config = config.Append($"prefix: {prefix}").ToArray();

            System.IO.File.WriteAllLines(configPath, config);

            using var source = new CancellationTokenSource();

            TestExtractor extractor = null;
            CogniteDestination destination = null;

            var services = new ServiceCollection();

            var task = ExtractorRunner.Run<MyConfig, TestExtractor>(
                configPath,
                null,
                "base-extractor-test-utils",
                null,
                false,
                true,
                false,
                false,
                source.Token,
                (dest, ext) =>
                {
                    destination = dest;
                    extractor = ext;
                });

            try
            {
                bool eventsOk = true;
                bool timeseriesOk = true;
                bool rawOk = true;
                for (int i = 0; i < 10; i++)
                {
                    eventsOk = true;
                    timeseriesOk = true;
                    rawOk = true;

                    if (extractor?.CreatedEvents?.Count >= 10)
                    {
                        var evts = new List<string>();
                        for (int j = 0; j < 10; j++) evts.Add(extractor.CreatedEvents[j]);
                        var events = await destination.CogniteClient.Events.RetrieveAsync(evts, true);
                        if (events.Count() != 10) eventsOk = false;

                        var dps = await destination.CogniteClient.DataPoints.ListAsync(new DataPointsQuery
                        {
                            Items = new[]
                            {
                            new DataPointsQueryItem
                            {
                                ExternalId = extractor.TSId
                            }
                        }
                        });

                        if ((dps.Items.First().NumericDatapoints?.Datapoints?.Count ?? 0) < 10) timeseriesOk = false;

                        var rows = await destination.CogniteClient.Raw.ListRowsAsync<JsonElement>(extractor.DBName, extractor.TableName);
                        if (rows.Items.Count() < 10) rawOk = false;
                    }
                    else
                    {
                        eventsOk = timeseriesOk = rawOk = false;
                    }
                    if (eventsOk && timeseriesOk && rawOk) break;

                    await Task.Delay(1000);
                }

                Assert.True(eventsOk && timeseriesOk && rawOk, $"{eventsOk}, {rawOk}, {timeseriesOk}");


            }
            finally
            {
                source.Cancel();
                await Task.WhenAny(task, Task.Delay(10000));

                System.IO.File.Delete(configPath);
                Assert.True(task.IsCompleted);
            }

        }

        [Theory]
        [InlineData(CogniteHost.GreenField)]
        [InlineData(CogniteHost.BlueField)]
        public async Task TestExtractionPipeline(CogniteHost host)
        {
            var tester = new CDFTester(host, _output);
            // We just need some dataset
            var datasets = await tester.Destination.CogniteClient.DataSets.ListAsync(new DataSetQuery
            {
                Limit = 1
            });
            var dataset = datasets.Items.First();

            var id = $"{tester.Prefix} ext-pipe";

            await tester.Destination.CogniteClient.ExtPipes.CreateAsync(new[]
            {
                new ExtPipeCreate
                {
                    DataSetId = dataset.Id,
                    ExternalId = id,
                    Name = "utils-test-pipeline"
                }
            });

            try
            {
                var newRun = new ExtractionRun(new ExtractionRunConfig
                {
                    Frequency = 1,
                    PipelineId = id
                }, tester.Destination, tester.Provider.GetService<ILogger<ExtractionRun>>());
                newRun.Continuous = true;
                newRun.Start();
                await Task.Delay(1000);

                ItemsWithCursor<ExtPipeRun> runs = null;
                for (int i = 0; i < 10; i++)
                {
                    runs = await tester.Destination.CogniteClient.ExtPipes.ListRunsAsync(new ExtPipeRunQuery
                    {
                        Filter = new ExtPipeRunFilter
                        {
                            ExternalId = id
                        }
                    });
                    if (runs.Items.Count() > 1 && runs.Items.Any(item => item.Status == ExtPipeRunStatus.success)) break;
                    await Task.Delay(1000);
                }

                Assert.True(runs.Items.Count() > 1 && runs.Items.Any(item => item.Status == ExtPipeRunStatus.success));

                await newRun.Report(ExtPipeRunStatus.failure, false, "Some failure");
                await newRun.DisposeAsync();

                runs = null;
                for (int i = 0; i < 10; i++)
                {
                    runs = await tester.Destination.CogniteClient.ExtPipes.ListRunsAsync(new ExtPipeRunQuery
                    {
                        Filter = new ExtPipeRunFilter
                        {
                            ExternalId = id
                        }
                    });
                    if (runs.Items.Count() > 3 && runs.Items.Any(run => run.Status == ExtPipeRunStatus.failure)
                        && runs.Items.Count(run => run.Status == ExtPipeRunStatus.success) > 1) break;
                    await Task.Delay(1000);
                }

                Assert.True(runs.Items.Count() > 3 && runs.Items.Any(run => run.Status == ExtPipeRunStatus.failure)
                        && runs.Items.Any(run => run.Status == ExtPipeRunStatus.success));
            }
            finally
            {
                await tester.Destination.CogniteClient.ExtPipes.DeleteAsync(new ExtPipeDelete
                {
                    Items = new[] { Identity.Create(id) }
                });
            }
        }

        [Fact]
        public async Task TestExtractorWithoutCDF()
        {
            var cfg = new MyConfig();
            cfg.GenerateDefaults();
            cfg.Logger.Console = new Cognite.Extractor.Logging.ConsoleConfig { Level = "debug" };

            NoCdfExtractor ext = null;

            using var source = new CancellationTokenSource();

            var task = ExtractorRunner.Run<MyConfig, NoCdfExtractor>(
                null,
                null,
                null,
                null,
                true,
                true,
                true,
                false,
                source.Token,
                (dest, extractor) =>
                {
                    Assert.Null(dest);
                    ext = extractor;
                },
                null,
                null,
                null,
                cfg,
                false
                );

            await Task.Delay(500);

            if (task.IsFaulted)
            {
                throw task.Exception;
            }

            Assert.NotNull(ext);
            Assert.True(ext.Started);
            Assert.True(ext.Iter > 2);

            source.Cancel();

            await task;
        }
    }
}
