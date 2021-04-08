using Cognite.Extensions;
using CogniteSdk;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace ExtractorUtils.Test
{
    public class RawIntegrationTest
    {
        private class TestDto
        {
            public string Name { get; set; }
            public int Number { get; set; }
        }


        [Theory]
        //[InlineData(CogniteHost.GreenField)]
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

                await tester.Destination.DeleteRowsAsync(dbName, tableName, columns.Keys, tester.Source.Token);
                rows = await tester.Destination.CogniteClient.Raw.ListRowsAsync(dbName, tableName, tester.Source.Token);
                Assert.Empty(rows.Items);
            }
            finally
            {
                await tester.Destination.CogniteClient.Raw.DeleteDatabasesAsync(new List<string>(){ dbName }, true, tester.Source.Token);
            }
        }
    }
}
