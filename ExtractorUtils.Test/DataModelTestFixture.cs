using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extensions.DataModels;
using Cognite.Extensions.DataModels.QueryBuilder;
using Cognite.Extractor.Configuration;
using Cognite.Extractor.Testing;
using Cognite.Extractor.Utils;
using CogniteSdk;
using CogniteSdk.DataModels;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Xunit;

namespace ExtractorUtils.Test
{
    public class DataModelTestFixture : LoggingTestFixture, IAsyncLifetime
    {
        private readonly string _configPath;
        private static int _configIdx;
        public ILogger<DataModelTestFixture> Logger { get; }
        public ServiceProvider Provider { get; }
        public CogniteDestination Destination { get; }
        public CancellationTokenSource Source { get; }
        public string Project { get; private set; }
        public string Host { get; private set; }
        public string Prefix { get; private set; }
        public BaseConfig Config { get; }


        public DataModel Model { get; private set; }
        public string Space { get; private set; }

        public DataModelTestFixture()
        {
            var config = new List<string>
            {
                "version: 2",
                "logger:",
                "  console:",
                "    level: verbose",
                "cognite:",
                "  project: ${TEST_PROJECT}",
                "  host: ${TEST_HOST}",
                "  idp-authentication:",
                "    client-id: ${TEST_CLIENT_ID}",
                "    tenant: ${TEST_TENANT}",
                "    secret: ${TEST_SECRET}",
                "    scopes:",
                "    - ${TEST_SCOPE}"
            };
            _configPath = $"test-config-dm-{Interlocked.Increment(ref _configIdx)}";
            System.IO.File.WriteAllLines(_configPath, config);
            var services = new ServiceCollection();
            Config = services.AddConfig<BaseConfig>(_configPath, 2);
            Configure(services);
            services.AddCogniteClient("net-extractor-utils-test", userAgent: "Utils-Tests/v1.0.0 (Test)");
            Provider = services.BuildServiceProvider();
            Logger = Provider.GetRequiredService<ILogger<DataModelTestFixture>>();
            Destination = Provider.GetRequiredService<CogniteDestination>();
            Prefix = TestUtils.AlphaNumericPrefix("net-utils-test-");
            Source = new CancellationTokenSource();
        }


        async ValueTask IAsyncDisposable.DisposeAsync()
        {
            await DeleteSpace(Space);
            GC.SuppressFinalize(this);
        }

        async ValueTask IAsyncLifetime.InitializeAsync()
        {
            await EnsureDataModel();

            Space = Prefix + "-space";
            await Destination.CogniteClient.DataModels.UpsertSpaces(new[]
            {
                new SpaceCreate
                {
                    Description = ".NET utils test space",
                    Name = Space,
                    Space = Space
                }
            });
        }

        public async Task DeleteSpace(string space)
        {
            if (space == null) return;

            // Delete edges
            string cursor = null;
            do
            {
                var edges = await Destination.CogniteClient.DataModels.FilterInstances<JsonNode>(new InstancesFilter
                {
                    Cursor = cursor,
                    Filter = Filter.Space(space, false),
                    IncludeTyping = false,
                    Limit = 1000,
                    InstanceType = InstanceType.edge
                }, Source.Token);
                if (edges.Items.Any())
                {
                    int i = 0;
                    while (true)
                    {
                        try
                        {
                            await Destination.CogniteClient.DataModels.DeleteInstances(
                                edges.Items.Select(e => new InstanceIdentifierWithType(InstanceType.edge, e.Space, e.ExternalId)),
                                Source.Token);
                            break;
                        }
                        catch
                        {
                            if (i++ == 5)
                            {
                                throw;
                            }
                            await Task.Delay(500);
                        }
                    }
                }
                cursor = edges.NextCursor;
            } while (cursor != null);

            // Delete nodes
            cursor = null;
            do
            {
                var nodes = await Destination.CogniteClient.DataModels.FilterInstances<JsonNode>(new InstancesFilter
                {
                    Cursor = cursor,
                    Filter = Filter.Space(space, true),
                    IncludeTyping = false,
                    Limit = 1000,
                    InstanceType = InstanceType.node
                }, Source.Token);
                if (nodes.Items.Any())
                {
                    int i = 0;
                    while (true)
                    {
                        try
                        {
                            await Destination.CogniteClient.DataModels.DeleteInstances(
                                nodes.Items.Select(e => new InstanceIdentifierWithType(InstanceType.node, e.Space, e.ExternalId)),
                                Source.Token);
                            break;
                        }
                        catch
                        {
                            if (i++ == 5)
                            {
                                throw;
                            }
                            await Task.Delay(500);
                        }
                    }


                }
                cursor = nodes.NextCursor;
            } while (cursor != null);

            // Delete space

            await Destination.CogniteClient.DataModels.DeleteSpaces(new[]
            {
                space
            });
        }

        private async Task EnsureDataModel()
        {
            var version = "1";
            var modelSpace = "utils-test-space";

            var retModels = await Destination.CogniteClient.DataModels.RetrieveDataModels(new[]
            {
                new FDMExternalId("TestModel", modelSpace, version)
            }, false, Source.Token);
            if (retModels.Any())
            {
                Model = retModels.First();
                return;
            }

            var containers = new[]
            {
                new ContainerCreate
                {
                    ExternalId = "TestType",
                    Description = "Test type for .NET utils",
                    Name = "Test Type",
                    Space = modelSpace,
                    UsedFor = UsedFor.node,
                    Properties = new Dictionary<string, ContainerPropertyDefinition>
                    {
                        { "propLong", new ContainerPropertyDefinition
                        {
                            Description = "Long prop",
                            Nullable = true,
                            Type = BasePropertyType.Create(PropertyTypeVariant.int64)
                        } },
                        { "propText", new ContainerPropertyDefinition
                        {
                            Description = "Text prop",
                            Nullable = true,
                            Type = BasePropertyType.Text()
                        } },
                        { "propRel", new ContainerPropertyDefinition
                        {
                            Description = "Relation prop",
                            Nullable = true,
                            Type = BasePropertyType.Direct()
                        } },
                    }
                }
            };

            var views = containers.Select(c => c.ToView(version)).ToArray();

            var model = new DataModelCreate
            {
                Description = "Test data model for .NET utils",
                ExternalId = "TestModel",
                Name = "Test Model",
                Space = modelSpace,
                Version = version,
                Views = views
            };

            await Destination.CogniteClient.DataModels.UpsertSpaces(new[]
            {
                new SpaceCreate
                {
                    Description = "Test space for .NET utils",
                    Name = "TestSpace",
                    Space = modelSpace
                }
            }, Source.Token);
            await Destination.CogniteClient.DataModels.UpsertContainers(containers, Source.Token);
            await Destination.CogniteClient.DataModels.UpsertViews(views, Source.Token);
            var modelRes = await Destination.CogniteClient.DataModels.UpsertDataModels(new[] { model }, Source.Token);

            Model = modelRes.First();
        }
    }
}
