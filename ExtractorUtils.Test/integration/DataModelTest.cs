using Cognite.Extensions.DataModels;
using Cognite.Extensions.DataModels.QueryBuilder;
using Cognite.Extractor.Testing;
using CogniteSdk.Beta.DataModels;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace ExtractorUtils.Test.integration
{
    public class DataModelTest : IClassFixture<DataModelTestFixture>
    {
        private readonly DataModelTestFixture _tester;
        public DataModelTest(DataModelTestFixture tester, ITestOutputHelper output)
        {
            _tester = tester;
            _tester.Init(output);
        }

        [Fact]
        public void TestFilterBuilder()
        {
            var filter = Filter.Not(Filter.And(
                Filter.Equal("test", "node", "externalId"),
                Filter.Or(
                    Filter.ExternalId("test2"),
                    Filter.Space("space", true),
                    Filter.In(new[] { (Value)"test", "test2" }, "some", "property", "value"),
                    Filter.HasData(new[] { new ContainerIdentifier("space", "id") })
                ),
                Filter.Nested(Filter.Equal(123L, "some", "long", "prop"), "some", "scope")
            ));

            var json = JsonSerializer.Serialize(filter, Oryx.Cognite.Common.jsonOptions);
            Assert.Equal(
                @"{""not"":{""and"":[{""equals"":{""property"":[""node"",""externalId""],""value"":""test""}},"
                + @"{""or"":[{""equals"":{""property"":[""node"",""externalId""],""value"":""test2""}},"
                + @"{""equals"":{""property"":[""node"",""space""],""value"":""space""}},"
                + @"{""in"":{""property"":[""some"",""property"",""value""],""values"":[""test"",""test2""]}},"
                + @"{""hasData"":{""models"":[{""type"":""container"",""space"":""space"",""externalId"":""id""}]}}]},"
                + @"{""nested"":{""scope"":[""some"",""scope""],""filter"":"
                 + @"{""equals"":{""property"":[""some"",""long"",""prop""],""value"":123}}}}]}}",
                json);
        }

        [Fact]
        public async Task TestFilterPagination()
        {
            var prefix = TestUtils.AlphaNumericPrefix("extractor-utils-test-");

            var edges = Enumerable.Range(0, 10).Select(i => new EdgeWrite
            {
                StartNode = new DirectRelationIdentifier(_tester.Space, $"{prefix}-start-{i}"),
                EndNode = new DirectRelationIdentifier(_tester.Space, $"{prefix}-end-{i}"),
                ExternalId = $"{prefix}{i}",
                Type = new DirectRelationIdentifier(_tester.Space, $"{prefix}-type"),
                Space = _tester.Space
            });

            await _tester.Destination.CogniteClient.Beta.DataModels.UpsertInstances(new InstanceWriteRequest
            {
                AutoCreateEndNodes = true,
                AutoCreateStartNodes = true,
                Items = edges,
                Replace = true
            });

            var query = new QueryBuilder()
                .WithQuery("startNodes", new NodeQueryBuilderItem()
                    .WithLimit(5)
                    .WithFilter(Filter.And(
                        Filter.Space(_tester.Space),
                        Filter.Prefix($"{prefix}-start-", "node", "externalId")
                    )))
                .WithQuery("edges", new EdgeQueryBuilderItem()
                    .WithLimit(5)
                    .WithFrom("startNodes"))
                .WithQuery("endNodes", new NodeQueryBuilderItem()
                    .WithLimit(5)
                    .WithFrom("edges"))
                .Build();

            var result = await _tester.Destination.CogniteClient.Beta.DataModels.QueryPaginated<JsonNode>(
                query, Enumerable.Empty<string>(), _tester.Source.Token);

            Assert.Equal(3, result.Count);
            Assert.Equal(10, result["startNodes"].Count());
            Assert.Equal(10, result["edges"].Count());
            Assert.Equal(10, result["endNodes"].Count());
        }

        [Fact]
        public void TestContainerToView()
        {
            var container = new ContainerCreate
            {
                Description = "desc",
                ExternalId = "test",
                Name = "name",
                Properties = new Dictionary<string, ContainerPropertyDefinition>
                {
                    { "prop", new ContainerPropertyDefinition
                    {
                        Name = "prop",
                        Type = BasePropertyType.Text()
                    } },
                    { "relProp", new ContainerPropertyDefinition
                    {
                        Name = "relProp",
                        Type = BasePropertyType.Direct(new ContainerIdentifier("space", "test2"))
                    } }
                },
                Space = "space"
            };

            var view = container.ToView("2", new ViewIdentifier("space", "base", "1"));
            Assert.Equal("desc", view.Description);
            Assert.Equal("test", view.ExternalId);
            Assert.Equal("2", view.Version);
            Assert.Equal("name", view.Name);
            Assert.Equal(2, view.Properties.Count);
            var prop = view.Properties["prop"] as ViewPropertyCreate;
            Assert.Equal("prop", prop.Name);
            Assert.Equal("prop", prop.ContainerPropertyIdentifier);
            Assert.Equal("test", prop.Container.ExternalId);
            Assert.Equal("space", prop.Container.Space);
            var idf = (view.Properties["relProp"] as ViewPropertyCreate).Source;
            Assert.Equal("space", idf.Space);
            Assert.Equal("test2", idf.ExternalId);
            Assert.Equal("2", idf.Version);
            Assert.Single(view.Implements);
        }
    }
}
