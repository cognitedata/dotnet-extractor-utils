using Cognite.Extensions;
using Cognite.Extractor.Utils;
using CogniteSdk;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

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

    class TestExtractor : BaseExtractor
    {
        public string TSId { get; private set; }
        public List<string> CreatedEvents { get; private set; }
        public string DBName { get; private set; }
        public string TableName { get; private set; }
        private string _prefix;
        public TestExtractor(MyConfig config, CogniteDestination destination) : base(config, destination)
        {
            _prefix = config.Prefix;
        }

        protected override async Task Start()
        {
            DBName = $"{_prefix}-test-db";
            TableName = $"{_prefix}-test-table";
            CreateEventQueue(10, TimeSpan.FromSeconds(1), null);
            CreateTimeseriesQueue(10, TimeSpan.FromSeconds(1), null);
            CreateRawQueue<TestRawItem>(DBName, TableName, 
                10, TimeSpan.FromSeconds(1), null);

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
                return Task.FromResult<IEnumerable<(Identity, Datapoint)>>(new [] { dp });
            });
        }
    }


    public class ExtractorTest
    {
        [Fact]
        public async Task TestExtractorRun()
        {
            var configPath = "test-config-base-extractor";
            var config = CDFTester.GetConfig(CogniteHost.BlueField);

            const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
            Random random = new Random();
            var prefix = "net-utils-test-" + new string(Enumerable.Repeat(chars, 5)
              .Select(s => s[random.Next(s.Length)]).ToArray());

            config = config.Append($"prefix: {prefix}").ToArray();

            System.IO.File.WriteAllLines(configPath, config);

            using var source = new CancellationTokenSource();

            TestExtractor extractor = null;
            CogniteDestination destination = null;

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

                        if (dps.Items.First().NumericDatapoints.Datapoints.Count < 10) timeseriesOk = false;

                        var rows = await destination.CogniteClient.Raw.ListRowsAsync(extractor.DBName, extractor.TableName);
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

                if (extractor.DBName != null)
                {
                    await destination.CogniteClient.Raw.DeleteDatabasesAsync(new RawDatabaseDelete
                    {
                        Items = new[]
                    {
                        new RawDatabase { Name = extractor.DBName }
                    },
                        Recursive = true
                    });
                }
                if (extractor.TSId != null)
                {
                    await destination.CogniteClient.TimeSeries.DeleteAsync(new TimeSeriesDelete
                    {
                        IgnoreUnknownIds = true,
                        Items = new[]
                        {
                            Identity.Create(extractor.TSId)
                        }
                    });
                }
                
                if (extractor.CreatedEvents.Any())
                {
                    await destination.CogniteClient.Events.DeleteAsync(new EventDelete
                    {
                        Items = extractor.CreatedEvents.Select(Identity.Create),
                        IgnoreUnknownIds = true
                    });
                }

                Assert.True(task.IsCompleted);
            }
            
        }
    }
}
