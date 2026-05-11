using Cognite.Extensions;
using Cognite.Extensions.DataModels.CogniteExtractorExtensions;
using Cognite.Extractor.Common;
using Cognite.Extractor.Utils;
using CogniteSdk;
using CogniteSdk.DataModels;
using CogniteSdk.DataModels.Core;
using CogniteSdk.Resources.DataModels;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace ExtractorUtils.Test.Integration
{
    public class IDMTimeSeriesIntegrationTest
    {
        private readonly ITestOutputHelper _output;
        public IDMTimeSeriesIntegrationTest(ITestOutputHelper output)
        {
            _output = output;
        }

        private static SourcedNodeWrite<T> GetWritableTS<T>(CDFTester tester, string externalId, string spaceId, TimeSeriesType? tsType = TimeSeriesType.Numeric, Func<SourcedNodeWrite<T>, SourcedNodeWrite<T>> extraPopulateMethod = null) where T : CogniteTimeSeriesBase, new()
        {
            var ret = new SourcedNodeWrite<T>
            {
                Space = spaceId,
                ExternalId = externalId,
                Properties = new T() { Type = tsType }
            };
            if (extraPopulateMethod != null)
            {
                ret = extraPopulateMethod(ret);
            }
            return ret;
        }

        private static async Task<(string space, List<string> externalIds)> CreateTestTimeSeries<T>(CDFTester tester, int count = 3, ViewIdentifier view = null, int offset = 0) where T : CogniteTimeSeriesBase, new()
        {
            var spaceId = await tester.GetSpaceId();
            var timeseries = Enumerable.Range(offset + 1, count).Select(i => GetWritableTS<T>(
                tester,
                $"{tester.Prefix}utils-test-ts-{i}",
                spaceId,
                i % 2 == 0 ? TimeSeriesType.String : TimeSeriesType.Numeric,
                x => { x.Properties.Name = $"utils-test-ts-{i}"; return x; }
            ));

            var result = await tester.DestinationWithIDM.CogniteClient.CoreDataModel.TimeSeries<T>(view ?? CogniteDestinationWithIDM.IDMViewIdentifier).UpsertAsync(
                timeseries,
                new UpsertOptions(),
                tester.Source.Token
            );
                
            return (spaceId, result.Select(ts => ts.ExternalId).ToList());
        }

        private static List<Identity> CreateIdentities(string space, IEnumerable<string> externalIds)
        {
            return externalIds.Select(extId => new Identity(new InstanceIdentifier(space, extId))).ToList();
        }

        private static async Task DeleteTimeseries(CDFTester tester, string space, IEnumerable<string> externalIds)
        {
            await tester.DestinationWithIDM.CogniteClient.DataModels.DeleteInstances(externalIds.Select(x => new InstanceIdentifierWithType(InstanceType.node, space, x)), tester.Source.Token);
        }

        [Theory]
        [InlineData(CogniteHost.GreenField)]
        [InlineData(CogniteHost.BlueField)]
        public async Task TestCreateTimeSeries(CogniteHost host)
        {
            using var tester = new CDFTester(host, _output);
            var spaceId = await tester.GetSpaceId();
            var ids = new[] {
                $"{tester.Prefix} ts-1",
                $"{tester.Prefix} ts-2",
                $"{tester.Prefix} ts-3",
                $"{tester.Prefix} ts-4",
                $"{tester.Prefix} ts-5",
            }.Select(x => new InstanceIdentifier(spaceId, x));

            var timeseries1 = new[] { $"{tester.Prefix} ts-1", $"{tester.Prefix} ts-2", $"{tester.Prefix} ts-3" }.Select(x => GetWritableTS<CogniteExtractorTimeSeries>(tester, x, spaceId));

            try
            {
                var result = await tester.DestinationWithIDM.EnsureTimeSeriesExistsAsync(timeseries1, RetryMode.None, SanitationMode.None, tester.Source.Token);
                Assert.True(result.IsAllGood);
                Assert.Equal(3, result.Results.Count());

                var timeseries2 = new[] { GetWritableTS<CogniteExtractorTimeSeries>(tester, $"{tester.Prefix} ts-1", spaceId), GetWritableTS<CogniteExtractorTimeSeries>(tester, $"{tester.Prefix} ts-4", spaceId) };

                result = await tester.DestinationWithIDM.EnsureTimeSeriesExistsAsync(timeseries2, RetryMode.OnError, SanitationMode.None, tester.Source.Token);

                Assert.Empty(result.Errors);
                Assert.Equal(2, result.Results.Count());

                result = await tester.DestinationWithIDM.GetOrCreateTimeSeriesAsync(ids, toCreate =>
                {
                    Assert.Single(toCreate);
                    Assert.Equal($"{tester.Prefix} ts-5", toCreate.First().ExternalId);
                    return new[] { GetWritableTS<CogniteExtractorTimeSeries>(tester, $"{tester.Prefix} ts-5", spaceId) };
                }, RetryMode.OnError, SanitationMode.None, tester.Source.Token);

                Assert.Equal(5, result.Results.Count());
                Assert.Equal(Enumerable.Range('1', 5), result.Results.Select(res => (int)res.ExternalId.Last()).OrderBy(v => v));
            }
            finally
            {
                await DeleteTimeseries(tester, spaceId, ids.Select(x => x.ExternalId));
            }
        }

        [Theory]
        [InlineData(CogniteHost.GreenField)]
        [InlineData(CogniteHost.BlueField)]
        public async Task TestSanitation(CogniteHost host)
        {
            using var tester = new CDFTester(host, _output);
            var spaceId = await tester.GetSpaceId();

            var timeseries = new[] {
                GetWritableTS<CogniteExtractorTimeSeries>(tester, tester.Prefix + new string('æ', 300), spaceId, extraPopulateMethod: x => {
                   x.Properties.Name = new string('ø', 1000);
                   x.Properties.Description = new string('æ', 1000);
                   x.Properties.SourceUnit = new string('æ', 1000);
                   x.Properties.extractedData = Enumerable.Range(0, 100).ToDictionary(i => $"key{i:000}{new string('æ', 100)}", i => new string('æ', 200));
                   return x;
                }),
                GetWritableTS<CogniteExtractorTimeSeries>(tester, $"{tester.Prefix} test-duplicate-externalId", spaceId, extraPopulateMethod: x => { x.Properties.Name = "test-duplicate-externalId"; return x; }),
                GetWritableTS<CogniteExtractorTimeSeries>(tester, $"{tester.Prefix} test-duplicate-externalId", spaceId, extraPopulateMethod: x => { x.Properties.Name = "test-duplicate-externalId"; return x; }),
            };

            IEnumerable<SourcedNode<CogniteExtractorTimeSeries>> created = null;

            try
            {
                var result = await tester.DestinationWithIDM.EnsureTimeSeriesExistsAsync(timeseries, RetryMode.OnError, SanitationMode.Clean, tester.Source.Token);
                tester.Logger.LogResult(result, RequestType.UpsertInstances, false);
                created = result.Results;

                Assert.Single(result.Errors);
                var error1 = result.Errors.First();
                Assert.Equal(ErrorType.ItemDuplicated, error1.Type);
                Assert.Equal(ResourceType.InstanceId, error1.Resource);
                Assert.Equal(2, result.Results.Count());
                Assert.Equal((tester.Prefix + new string('æ', 255 - tester.Prefix.Length)).TruncateBytes(256)!, result.Results.First().ExternalId);
                Assert.Equal($"{tester.Prefix} test-duplicate-externalId", result.Results.Last().ExternalId);
            }
            finally
            {
                if (created != null)
                {
                    await DeleteTimeseries(tester, spaceId, created.Select(x => x.ExternalId));
                }
            }
        }

        [Theory]
        [InlineData(CogniteHost.GreenField)]
        [InlineData(CogniteHost.BlueField)]
        public async Task TestUpdateTimeSeries(CogniteHost host)
        {
            using var tester = new CDFTester(host, _output);
            var spaceId = await tester.GetSpaceId();

            var tss = await CreateTestTimeSeries<CogniteExtractorTimeSeries>(tester);
            var identities = CreateIdentities(tss.space, tss.externalIds);

            var updates = tss.externalIds.Select(x => GetWritableTS<CogniteExtractorTimeSeries>(tester, x, spaceId, null, x => { x.Properties.Description = "new description"; return x; }));

            try
            {
                var result = await tester.DestinationWithIDM.UpsertTimeSeriesAsync(updates, RetryMode.None, SanitationMode.None, tester.Source.Token);
                tester.Logger.LogResult(result, RequestType.UpsertInstances, false);
                result.Throw();

                Assert.Equal(3, result.Results.Count());
                Assert.True(result.IsAllGood);

                var retrieved = await tester.DestinationWithIDM.GetTimeSeriesByIdsIgnoreErrors<CogniteExtractorTimeSeries>(identities, tester.Source.Token);
                Assert.All(retrieved, ts => Assert.Equal("new description", ts.Properties.Description));
            }
            finally
            {
                await DeleteTimeseries(tester, spaceId, tss.externalIds);
            }
        }

        [Theory]
        [InlineData(CogniteHost.GreenField)]
        [InlineData(CogniteHost.BlueField)]
        public async Task TestUpdateSanitation(CogniteHost host)
        {
            using var tester = new CDFTester(host, _output);

            var spaceId = await tester.GetSpaceId();
            var tss = await CreateTestTimeSeries<CogniteExtractorTimeSeries>(tester);

            var meta = Enumerable.Range(0, 100)
                    .ToDictionary(i => $"key{i:000}{new string('æ', 100)}", i => new string('æ', 200));

            var updates = new[]
            {
                GetWritableTS<CogniteExtractorTimeSeries>(tester, tss.externalIds[0], spaceId, null, x => {
                    x.Properties.Description = new string('æ', 2000);
                    x.Properties.extractedData = meta;
                    x.Properties.Name = new string('æ', 300);
                    x.Properties.SourceUnit = new string('æ', 200);
                    return x;
                }),
                GetWritableTS<CogniteExtractorTimeSeries>(tester, tss.externalIds[1], spaceId, null, x => {x.Properties.Name = "name"; return x;}),
                GetWritableTS<CogniteExtractorTimeSeries>(tester, tss.externalIds[1], spaceId, null, x => {x.Properties.Name = "name"; return x;})
            };

            try
            {
                var result = await tester.DestinationWithIDM.UpsertTimeSeriesAsync(updates, RetryMode.OnError, SanitationMode.Clean, tester.Source.Token);
                tester.Logger.LogResult(result, RequestType.UpsertInstances, false);
                result.ThrowOnFatal();

                Assert.Equal(2, result.Results.Count());

                var errs = result.Errors.ToArray();
                Assert.Single(errs);

                Assert.Equal(ErrorType.ItemDuplicated, errs[0].Type);
                Assert.Equal(ResourceType.InstanceId, errs[0].Resource);
                Assert.Single(errs[0].Values);
            }
            finally
            {
                await DeleteTimeseries(tester, spaceId, tss.externalIds);
            }
        }

        //This feature depends on fields currently not present on DM, ex: https://api-docs.cognite.com/20230101/tag/Time-series/operation/alterTimeSeries#!c=422&path=error/duplicated&t=response
        //[Theory]
        //[InlineData(CogniteHost.GreenField)]
        //[InlineData(CogniteHost.BlueField)]
        //public async Task TestUpdateErrorHandling(CogniteHost host)
        //{
        //    using var tester = new CDFTester(host, _output);

        //    var tss = await CreateTestTimeSeries<CogniteExtractorTimeSeries>(tester);

        //    Func<SourcedNodeWrite<CogniteExtractorTimeSeries>, SourcedNodeWrite<CogniteExtractorTimeSeries>> upd = x => { x.Properties.Description = "new description"; return x; };

        //    var updates1 = new[]
        //    {
        //        // Existing timeseries
        //        GetWritableTS<CogniteExtractorTimeSeries>(tester, tss.externalIds[0], tss.space, TimeSeriesType.String, x => {x.ExternalId =tss.externalIds[1]; return x;}),
        //        // Update OK
        //        GetWritableTS<CogniteExtractorTimeSeries>(tester, tss.externalIds[1], tss.space, TimeSeriesType.Numeric),
        //        // Missing by external id
        //        GetWritableTS<CogniteExtractorTimeSeries>(tester, "missing-ts", tss.space, TimeSeriesType.String, upd),
        //    };

        //    try
        //    {
        //        var result1 = await tester.DestinationWithIDM.UpsertTimeSeriesAsync(updates1, RetryMode.OnError, SanitationMode.None, tester.Source.Token);
        //        result1.ThrowOnFatal();

        //        var errs = result1.Errors.ToArray();
        //        Assert.Equal(2, errs.Length);

        //        var err = errs.First(e => e.Type == ErrorType.ItemExists && e.Resource == ResourceType.ExternalId);
        //        Assert.Single(err.Skipped);
        //        Assert.Contains(err.Values, e => e.ExternalId == tss.externalIds[1]);

        //        err = errs.First(e => e.Type == ErrorType.ItemMissing && e.Resource == ResourceType.AssetId);
        //        Assert.Single(err.Skipped);
        //        Assert.Contains(err.Values, e => e.Id == 123);
        //    }
        //    finally
        //    {
        //        await DeleteTimeseries(tester, tss.space, tss.externalIds);
        //    }
        //}

        [Theory]
        [InlineData(CogniteHost.GreenField)]
        [InlineData(CogniteHost.BlueField)]
        public async Task TestCreateDataPoints(CogniteHost host)
        {
            using var tester = new CDFTester(host, _output);

            var tss = await CreateTestTimeSeries<CogniteExtractorTimeSeries>(tester);
            var identities = CreateIdentities(tss.space, tss.externalIds);

            var dps = new Dictionary<Identity, IEnumerable<Datapoint>>()
            {
                { identities[0], Enumerable.Range(0, 10)
                    .Select(i => new Datapoint(DateTime.UtcNow.AddSeconds(i), i)).ToList() },
                { identities[1], Enumerable.Range(0, 10)
                    .Select(i => new Datapoint(DateTime.UtcNow.AddSeconds(i), $"value{i}")).ToList() },
                { identities[2], Enumerable.Range(0, 10)
                    .Select(i => new Datapoint(DateTime.UtcNow.AddSeconds(i), i)).ToList() }
            };

            tester.Config.Cognite.CdfChunking.DataPointTimeSeries = 2;
            tester.Config.Cognite.CdfChunking.DataPoints = 5;

            try
            {
                var result = await tester.DestinationWithIDM.InsertDataPointsIDMAsync(dps, SanitationMode.None, RetryMode.None, tester.Source.Token);
                tester.Logger.LogResult(result, RequestType.CreateDatapoints, false);
                Assert.Empty(result.Errors);

                int[] counts = new int[3];
                for (int i = 0; i < 10; i++)
                {
                    var foundDps = await tester.DestinationWithIDM.CogniteClient.DataPoints.ListAsync(new DataPointsQuery
                    {
                        Items = tss.externalIds.Select(ts => new DataPointsQueryItem
                        {
                            InstanceId = new InstanceIdentifier(tss.space, ts),
                            End = DateTime.UtcNow.AddDays(1).ToUnixTimeMilliseconds().ToString()
                        }).ToArray()
                    });
                    if (foundDps.Items.Count == 3)
                    {
                        counts[0] = foundDps.Items[0]?.NumericDatapoints?.Datapoints?.Count ?? 0;
                        counts[1] = foundDps.Items[1]?.StringDatapoints?.Datapoints?.Count ?? 0;
                        counts[2] = foundDps.Items[2]?.NumericDatapoints?.Datapoints?.Count ?? 0;
                        if (counts.All(cnt => cnt == 10)) break;
                    }
                    await Task.Delay(1000);
                }
                Assert.All(counts, cnt => Assert.Equal(10, cnt));
            }
            finally
            {
                await DeleteTimeseries(tester, tss.space, tss.externalIds);
            }
        }
        [Theory]
        [InlineData(CogniteHost.GreenField)]
        [InlineData(CogniteHost.BlueField)]
        public async Task TestDataPointsSanitation(CogniteHost host)
        {
            using var tester = new CDFTester(host, _output);

            var tss = await CreateTestTimeSeries<CogniteExtractorTimeSeries>(tester);
            var identities = CreateIdentities(tss.space, tss.externalIds);

            Dictionary<Identity, IEnumerable<Datapoint>> GetCreates()
            {
                return new Dictionary<Identity, IEnumerable<Datapoint>>()
                {
                    { identities[0], new []
                    {
                        new Datapoint(DateTime.UtcNow, 1.0),
                        new Datapoint(DateTime.MaxValue, 2.0),
                        new Datapoint(DateTime.UtcNow.AddSeconds(1), double.NaN),
                        new Datapoint(DateTime.MinValue, 3.0),
                        new Datapoint(DateTime.UtcNow.AddSeconds(2), double.PositiveInfinity),
                        new Datapoint(DateTime.UtcNow.AddSeconds(3), double.NegativeInfinity),
                        new Datapoint(DateTime.UtcNow.AddSeconds(4), 1E101),
                        new Datapoint(DateTime.UtcNow.AddSeconds(5), -1E101),
                    } },
                    { identities[1], new []
                    {
                        new Datapoint(DateTime.UtcNow, new string('æ', CogniteUtils.TimeSeriesStringBytesMax + 1)),
                        new Datapoint(DateTime.UtcNow.AddSeconds(1), "test"),
                        new Datapoint(DateTime.UtcNow, null)
                    } },
                    { identities[2], new[]
                    {
                        new Datapoint(DateTime.UtcNow, double.NaN)
                    } }
                };
            }

            try
            {
                var result = await tester.DestinationWithIDM.InsertDataPointsIDMAsync(GetCreates(), SanitationMode.Remove, RetryMode.None, tester.Source.Token);
                tester.Logger.LogResult(result, RequestType.CreateDatapoints, false);

                var errs = result.Errors.ToArray();
                Assert.Equal(2, errs.Length);

                var err = errs[0];
                Assert.Equal(ResourceType.DataPointTimestamp, err.Resource);
                Assert.Equal(ErrorType.SanitationFailed, err.Type);
                Assert.Single(err.Skipped);
                var iErr = err.Skipped.OfType<DataPointInsertError>().First();
                Assert.Equal(2, iErr.DataPoints.Count());
                Assert.Equal(identities[0].ToString(), ((Identity)iErr.Id).ToString());

                err = errs[1];
                Assert.Equal(ResourceType.DataPointValue, err.Resource);
                Assert.Equal(ErrorType.SanitationFailed, err.Type);
                Assert.Equal(3, err.Skipped.Count());
                var insertErrs = err.Skipped.OfType<DataPointInsertError>().ToArray();

                iErr = insertErrs[0];
                Assert.Equal(5, iErr.DataPoints.Count());
                Assert.Equal(identities[0].ToString(), ((Identity)iErr.Id).ToString());
                iErr = insertErrs[1];
                Assert.Equal(2, iErr.DataPoints.Count());
                Assert.Equal(identities[1].ToString(), ((Identity)iErr.Id).ToString());
                iErr = insertErrs[2];
                Assert.Single(iErr.DataPoints);
                Assert.Equal(identities[2].ToString(), ((Identity)iErr.Id).ToString());

                typeof(CogniteDestination).GetField("_nanReplacement", BindingFlags.NonPublic | BindingFlags.Instance).SetValue(tester.DestinationWithIDM, new double?(123));

                result = await tester.DestinationWithIDM.InsertDataPointsIDMAsync(GetCreates(), SanitationMode.Clean, RetryMode.None, tester.Source.Token);

                errs = result.Errors.ToArray();
                Assert.Single(errs);

                err = errs[0];
                Assert.Equal(ResourceType.DataPointTimestamp, err.Resource);
                Assert.Equal(ErrorType.SanitationFailed, err.Type);
                Assert.Single(err.Skipped);
                iErr = err.Skipped.OfType<DataPointInsertError>().First();
                Assert.Equal(2, iErr.DataPoints.Count());
                Assert.Equal(identities[0].ToString(), ((Identity)iErr.Id).ToString());
            }
            finally
            {
                await DeleteTimeseries(tester, tss.space, tss.externalIds);
            }
        }
        [Theory]
        [InlineData(CogniteHost.GreenField)]
        [InlineData(CogniteHost.BlueField)]
        public async Task TestDataPointsErrorHandling(CogniteHost host)
        {
            using var tester = new CDFTester(host, _output);

            var spaceId = await tester.GetSpaceId();
            var tss = await CreateTestTimeSeries<CogniteExtractorTimeSeries>(tester);
            var identities = CreateIdentities(tss.space, tss.externalIds);

            var dps = new Dictionary<Identity, IEnumerable<Datapoint>>()
            {
                // All mismatched
                { identities[0], new [] {
                    new Datapoint(DateTime.UtcNow, "test"),
                    new Datapoint(DateTime.UtcNow.AddSeconds(1), "test2")
                } },
                // Some mismatched datapoints
                { identities[1], new[]
                {
                    new Datapoint(DateTime.UtcNow, 1.0),
                    new Datapoint(DateTime.UtcNow.AddSeconds(1), 2.0),
                    new Datapoint(DateTime.UtcNow.AddSeconds(2), "test")
                } },
                { identities[2], new[]
                {
                    new Datapoint(DateTime.UtcNow, 1.0),
                    new Datapoint(DateTime.UtcNow.AddSeconds(1), "test"),
                    new Datapoint(DateTime.UtcNow.AddSeconds(2), "test2")
                } },
                { Identity.Create(new InstanceIdentifier(spaceId, "missing-ts-1")), new[] { new Datapoint(DateTime.UtcNow, 1.0) } },
            };

            try
            {
                var result = await tester.DestinationWithIDM.InsertDataPointsIDMAsync(dps, SanitationMode.None, RetryMode.OnError, tester.Source.Token);
                tester.Logger.LogResult(result, RequestType.CreateDatapoints, false);

                var errs = result.Errors.ToArray();
                // Greenfield reports missing twice, once for each id type.
                CogniteError<DataPointInsertError> err;

                Assert.Equal(2, errs.Length);
                err = errs[0];
                Assert.Equal(ResourceType.Id, err.Resource);
                Assert.Equal(ErrorType.ItemMissing, err.Type);
                Assert.Single(err.Values);
                err = errs[1];

                Assert.Equal(ResourceType.DataPointValue, err.Resource);
                Assert.Equal(ErrorType.MismatchedType, err.Type);
                Assert.Equal(3, err.Skipped.Count());
                var insertErrs = err.Skipped.ToArray();
                Assert.Equal(2, insertErrs[0].DataPoints.Count());
                Assert.Equal(2, insertErrs[1].DataPoints.Count());
                Assert.Equal(2, insertErrs[2].DataPoints.Count());
            }
            finally
            {
                await DeleteTimeseries(tester, spaceId, tss.externalIds);
            }
        }
        [Theory]
        [InlineData(CogniteHost.GreenField)]
        [InlineData(CogniteHost.BlueField)]
        public async Task TestDataPointsCreateMissing(CogniteHost host)
        {
            using var tester = new CDFTester(host, _output);

            var spaceId = await tester.GetSpaceId();
            var tss = await CreateTestTimeSeries<CogniteExtractorTimeSeries>(tester);

            var dps = new Dictionary<Identity, IEnumerable<Datapoint>>()
            {
                { Identity.Create(new InstanceIdentifier(tss.space, tss.externalIds.First())), new[]
                {
                    new Datapoint(DateTime.UtcNow, 1.0),
                    new Datapoint(DateTime.UtcNow.AddSeconds(1), 2.0),
                    new Datapoint(DateTime.UtcNow.AddSeconds(2), 3.0)
                } },
                { Identity.Create(new InstanceIdentifier(spaceId, $"{tester.Prefix} utils-test-ts-missing-1")), new[] { new Datapoint(DateTime.UtcNow, 1.0) } },
                { Identity.Create(new InstanceIdentifier(spaceId, $"{tester.Prefix} utils-test-ts-missing-2")), new[] { new Datapoint(DateTime.UtcNow, "test") } },
            };

            try
            {
                var (dpResult, tsResult) = await tester.DestinationWithIDM
                    .InsertDataPointsCreateMissingAsync(dps, SanitationMode.Clean, RetryMode.OnError, tester.Source.Token);
                tester.Logger.LogResult(dpResult, RequestType.CreateDatapoints, false);
                tester.Logger.LogResult(tsResult, RequestType.CreateTimeSeries, false);

                Assert.Single(dpResult.Errors);
                var err = dpResult.Errors.First();
                Assert.Equal(ErrorType.ItemMissing, err.Type);

                Assert.True(tsResult.Errors == null || !tsResult.Errors.Any());
                var createdCount = tsResult.Results.Count();
                // Not perfectly consistent, since FDM isn't immediately consistent.
                Assert.True(createdCount == 2 || createdCount == 3);
            }
            finally
            {
                await DeleteTimeseries(tester, spaceId, tss.externalIds.Concat([$"{tester.Prefix} utils-test-ts-missing-1", $"{tester.Prefix} utils-test-ts-missing-2"]));
            }
        }

        [Theory]
        // [InlineData(CogniteHost.GreenField)]
        [InlineData(CogniteHost.BlueField)]
        public async Task TestGetOrCreate(CogniteHost host)
        {
            using var tester = new CDFTester(host, _output);

            var tss = await CreateTestTimeSeries<CogniteTimeSeriesBase>(tester, 5, CoreTimeSeriesResource<CogniteTimeSeriesBase>.DefaultView);
            var newItemExtId = tester.Prefix + "new-item";
            var existingGood = (await CreateTestTimeSeries<CogniteExtractorTimeSeries>(tester, 1, offset: 5)).externalIds[0];

            var allCreated = tss.externalIds.Append(newItemExtId).Append(existingGood);

            async Task<IEnumerable<SourcedNodeWrite<CogniteExtractorTimeSeries>>> createFunction(IEnumerable<InstanceIdentifier> ids)
            {
                await Task.CompletedTask;
                var missingIds = ids.ToHashSet();
                var filtered = allCreated.Where(extId => missingIds.Contains(new InstanceIdentifier(tss.space, extId)));

                Assert.Equal(6, filtered.Count());

                return filtered.Select((extId, index) => new SourcedNodeWrite<CogniteExtractorTimeSeries>
                {
                    Space = tss.space,
                    ExternalId = extId,
                    Properties = new CogniteExtractorTimeSeries()
                    {
                        // Match type in first, type conflict on the rest of the existing ones.
                        Type = index == 0 ? TimeSeriesType.Numeric : (index % 2 == 0 ? TimeSeriesType.String : TimeSeriesType.Numeric),
                        Name = $"utils-test-ts-updated-{index + 1}",
                        extractedData = new Dictionary<string, string>() { { "test", "value" } },
                    }
                });
            }

            try
            {
                var result = await tester.DestinationWithIDM.GetOrCreateTimeSeriesAsync<CogniteExtractorTimeSeries>(
                    allCreated.Select(x => new InstanceIdentifier(tss.space, x)),
                    createFunction,
                    RetryMode.OnError,
                    SanitationMode.Remove,
                    tester.Source.Token
                );

                Assert.Single(result.Errors);
                var error = result.Errors.First();
                Assert.StartsWith("Cannot update immutable property 'cdf_cdm.CogniteTimeSeries.type'", error.Message);
                Assert.Equal(ResourceType.InstanceProperty, error.Resource);
                Assert.Equal(ErrorType.IllegalItem, error.Type);
                Assert.Equal(400, error.Status);

                var identitiesToBeSkipped = tss.externalIds.Skip(1).Take(4).Select(x => new Identity(new InstanceIdentifier(tss.space, x))).ToHashSet();
                Assert.Equal(identitiesToBeSkipped, error.Values.ToHashSet());
                Assert.Equal(identitiesToBeSkipped, error.Skipped.Select(x => new Identity(new InstanceIdentifier(x.Space, x.ExternalId))).ToHashSet());

                var resultsDict = new Dictionary<string, SourcedNode<CogniteExtractorTimeSeries>>(
                    result.Results.Select(x => new KeyValuePair<string, SourcedNode<CogniteExtractorTimeSeries>>(x.ExternalId, x))
                );

                Assert.Equal(3, resultsDict.Count);
                var existingItemUpgraded = resultsDict[tss.externalIds[0]];
                var newItem = resultsDict[newItemExtId];
                var existingGoodItem = resultsDict[existingGood];

                Assert.Equal(tss.space, existingItemUpgraded.Space);
                Assert.Equal("utils-test-ts-updated-1", existingItemUpgraded.Properties.Name);
                Assert.Equal(TimeSeriesType.Numeric, existingItemUpgraded.Properties.Type);
                Assert.Equal("value", existingItemUpgraded.Properties.extractedData["test"]);

                Assert.Equal(tss.space, newItem.Space);
                Assert.Equal("utils-test-ts-updated-6", newItem.Properties.Name);
                Assert.Equal(TimeSeriesType.Numeric, newItem.Properties.Type);
                Assert.Equal("value", newItem.Properties.extractedData["test"]);

                Assert.Equal(tss.space, existingGoodItem.Space);
                Assert.Equal("utils-test-ts-6", existingGoodItem.Properties.Name);
                Assert.Equal(TimeSeriesType.String, existingGoodItem.Properties.Type);
                Assert.Null(existingGoodItem.Properties.extractedData);
            }
            finally
            {
                await DeleteTimeseries(tester, tss.space, allCreated);
            }
        }
    }
}
