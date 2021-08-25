using Cognite.Extensions;
using CogniteSdk;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace ExtractorUtils.Test.Integration
{
    public class RawIntegrationTest
    {
        private class TestDto
        {
            public string Name { get; set; }
            public int Number { get; set; }
        }


        [Theory]
        [InlineData(CogniteHost.GreenField)]
        [InlineData(CogniteHost.BlueField)]
        public async Task TestInsertRow(CogniteHost host)
        {
            using var tester = new CDFTester(host);

            string dbName = $"{tester.Prefix}-Db";
            string tableName = $"{tester.Prefix}-Table";
            
            var columns = new Dictionary<string, TestDto>{
                { "A", new TestDto{ Name = "A", Number = 0} },
                { "B", new TestDto{ Name = "B", Number = 1} },
                { "C", new TestDto{ Name = "C", Number = 2} },
                { "D", new TestDto{ Name = "D", Number = 3} },
                { "E", new TestDto{ Name = "E", Number = 4} },
                { "F", new TestDto{ Name = "F", Number = 5} }
            };

            try
            {
                await tester.Destination.InsertRawRowsAsync(dbName, tableName, columns, tester.Source.Token);
                
                var rows = await tester.Destination.CogniteClient.Raw.ListRowsAsync(dbName, tableName, tester.Source.Token);
                Assert.Equal(columns.Count, rows.Items.Count());
                Assert.All(rows.Items, (i) =>{
                    Assert.True(columns.ContainsKey(i.Key));
                    var name = i.Columns["Name"].ToString();
                    Assert.True(i.Columns["Number"].TryGetInt32(out var number));
                    Assert.Equal(name, columns[i.Key].Name);
                    Assert.Equal(number, columns[i.Key].Number);
                });

                if (host != CogniteHost.BlueField)
                {
                    // TODO: deleting rows is not working in Bluefield. Enable this once it works.
                    await tester.Destination.DeleteRowsAsync(dbName, tableName, columns.Keys, tester.Source.Token);
                    rows = await tester.Destination.CogniteClient.Raw.ListRowsAsync(dbName, tableName, tester.Source.Token);
                    Assert.Empty(rows.Items);
                }
            }
            finally
            {
                await tester.Destination.CogniteClient.Raw.DeleteDatabasesAsync(new List<string>(){ dbName }, true, tester.Source.Token);
            }
        }

        [Theory]
        [InlineData(CogniteHost.GreenField)]
        [InlineData(CogniteHost.BlueField)]
        public async Task TestUploadQueue(CogniteHost host)
        {
            using var tester = new CDFTester(host);

            string dbName = $"{tester.Prefix}-Db";
            string tableName = $"{tester.Prefix}-Table";

            var totalUploaded = 0;
            try {
                using (var queue = tester.Destination.CreateRawUploadQueue<TestDto>(dbName, tableName, TimeSpan.FromSeconds(1), 0, res => {
                    var numUploaded = res.Uploaded?.Count() ?? 0;
                    totalUploaded += numUploaded;
                    tester.Logger.LogInformation("Sent {Num} raw rows to CDF", numUploaded);
                    return Task.CompletedTask;
                }))
                {
                   var enqueueTask = Task.Run(async () => {
                        for (int i = 0; i < 20; ++i)
                        {
                            queue.EnqueueRow($"r{i}", new TestDto {Name = $"Test {i}", Number = i});
                            await Task.Delay(100, tester.Source.Token);
                        }
                    });
                    var uploadTask = queue.Start(tester.Source.Token);

                    var t = Task.WhenAny(uploadTask, enqueueTask);
                    await t;

                    tester.Logger.LogInformation("Enqueueing task completed. Disposing of the upload queue");
                }
                Assert.Equal(20, totalUploaded);

                var rows = await tester.Destination.CogniteClient.Raw.ListRowsAsync(dbName, tableName, tester.Source.Token);
                Assert.Equal(20, rows.Items.Count());

                var indexes = Enumerable.Range(0, 20).Select(i => $"r{i}").ToList();
                Assert.All(rows.Items, (i) =>{

                    var name = i.Columns["Name"].ToString();
                    Assert.True(i.Columns["Number"].TryGetInt32(out var number));
                    Assert.Contains(i.Key, indexes);
                    Assert.Equal(name, $"Test {number}");
                });

            }
            finally 
            {
                await tester.Destination.CogniteClient.Raw.DeleteDatabasesAsync(new List<string>(){ dbName }, true, tester.Source.Token);
            }
        }
    }
}
