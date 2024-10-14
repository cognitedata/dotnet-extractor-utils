using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cognite.Extensions;
using Cognite.Extensions.DataModels;
using Cognite.Extensions.DataModels.CogniteExtractorExtensions;
using Cognite.Extractor.Common;
using CogniteSdk;
using CogniteSdk.DataModels;
using CogniteSdk.DataModels.Core;
using Com.Cognite.V1.Timeseries.Proto;
using Microsoft.Extensions.Logging;
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

        private static SourcedNodeWrite<CogniteExtractorTimeSeries> GetWritableTS(CDFTester tester, string externalId, TimeSeriesType? tsType = TimeSeriesType.Numeric, Func<SourcedNodeWrite<CogniteExtractorTimeSeries>, SourcedNodeWrite<CogniteExtractorTimeSeries>> extraPopulateMethod = null)
        {
            var ret = new SourcedNodeWrite<CogniteExtractorTimeSeries>
            {
                Space = tester.SpaceId,
                ExternalId = externalId,
                Properties = new CogniteExtractorTimeSeries() { Type = tsType }
            };
            if (extraPopulateMethod != null)
            {
                ret = extraPopulateMethod(ret);
            }
            return ret;
        }

        private static async Task<InstanceIdentifier[]> CreateTestTimeSeries(CDFTester tester)
        {
            var timeseries = new[]
            {
                GetWritableTS(tester, "utils-test-ts-1", extraPopulateMethod: x => {x.Properties.Name = "utils-test-ts-1"; return x;}),
                GetWritableTS(tester, "utils-test-ts-2", TimeSeriesType.String, x => {x.Properties.Name = "utils-test-ts-2"; return x;}),
                GetWritableTS(tester, "utils-test-ts-3", extraPopulateMethod: x => {x.Properties.Name = "utils-test-ts-3"; return x;}),
            };

            var result = await tester.DestinationWithIDM.EnsureTimeSeriesExistsAsync(timeseries, RetryMode.None, SanitationMode.None, tester.Source.Token);
            return result.Results.Select(ts => new InstanceIdentifier(ts.Space, ts.ExternalId)).ToArray();
        }

        [Theory]
        [InlineData(CogniteHost.GreenField)]
        [InlineData(CogniteHost.BlueField)]
        public async Task TestCreateTimeSeries(CogniteHost host)
        {
            using var tester = new CDFTester(host, _output);
            var ids = new[] {
                $"{tester.Prefix} ts-1",
                $"{tester.Prefix} ts-2",
                $"{tester.Prefix} ts-3",
                $"{tester.Prefix} ts-4",
                $"{tester.Prefix} ts-5",
            }.Select(x => new InstanceIdentifier(tester.SpaceId, x));

            var timeseries1 = new[] { $"{tester.Prefix} ts-1", $"{tester.Prefix} ts-2", $"{tester.Prefix} ts-3" }.Select(x => GetWritableTS(tester, x));

            try
            {
                var result = await tester.DestinationWithIDM.EnsureTimeSeriesExistsAsync(timeseries1, RetryMode.None, SanitationMode.None, tester.Source.Token);
                Assert.True(result.IsAllGood);
                Assert.Equal(3, result.Results.Count());

                var timeseries2 = new[] { GetWritableTS(tester, $"{tester.Prefix} ts-1"), GetWritableTS(tester, $"{tester.Prefix} ts-4") };

                result = await tester.DestinationWithIDM.EnsureTimeSeriesExistsAsync(timeseries2, RetryMode.OnError, SanitationMode.None, tester.Source.Token);

                Assert.Empty(result.Errors);
                Assert.Equal(2, result.Results.Count());

                result = await tester.DestinationWithIDM.GetOrCreateTimeSeriesAsync(ids, toCreate =>
                {
                    Assert.Single(toCreate);
                    Assert.Equal($"{tester.Prefix} ts-5", toCreate.First().ExternalId);
                    return new[] { GetWritableTS(tester, $"{tester.Prefix} ts-5") };
                }, RetryMode.OnError, SanitationMode.None, tester.Source.Token);

                Assert.Equal(5, result.Results.Count());
                Assert.Equal(Enumerable.Range('1', 5), result.Results.Select(res => (int)res.ExternalId.Last()).OrderBy(v => v));
            }
            finally
            {
                await tester.DestinationWithIDM.CogniteClient.DataModels.DeleteInstances(ids.Select(x => new InstanceIdentifierWithType(InstanceType.node, x)), tester.Source.Token);
            }
        }

        [Theory]
        [InlineData(CogniteHost.GreenField)]
        [InlineData(CogniteHost.BlueField)]
        public async Task TestSanitation(CogniteHost host)
        {
            using var tester = new CDFTester(host, _output);

            var timeseries = new[] {
                GetWritableTS(tester, tester.Prefix + new string('æ', 300), extraPopulateMethod: x => {
                   x.Properties.Name = new string('ø', 1000);
                   x.Properties.Description = new string('æ', 1000);
                   x.Properties.SourceUnit = new string('æ', 1000);
                   x.Properties.extractedData = Enumerable.Range(0, 100).ToDictionary(i => $"key{i:000}{new string('æ', 100)}", i => new string('æ', 200));
                   return x;
                }),
                GetWritableTS(tester, $"{tester.Prefix} test-duplicate-externalId", extraPopulateMethod: x => { x.Properties.Name = "test-duplicate-externalId"; return x; }),
                GetWritableTS(tester, $"{tester.Prefix} test-duplicate-externalId", extraPopulateMethod: x => { x.Properties.Name = "test-duplicate-externalId"; return x; }),
            };

            IEnumerable<SourcedNode<CogniteExtractorTimeSeries>> created = null;

            try
            {
                var result = await tester.DestinationWithIDM.EnsureTimeSeriesExistsAsync(timeseries, RetryMode.OnError, SanitationMode.Clean, tester.Source.Token);
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
                    var ids = created?.Select(x => new InstanceIdentifier(tester.SpaceId, x.ExternalId));
                    await tester.DestinationWithIDM.CogniteClient.DataModels.DeleteInstances(ids.Select(x => new InstanceIdentifierWithType(InstanceType.node, x)), tester.Source.Token);
                }
            }
        }

        [Theory]
        [InlineData(CogniteHost.GreenField)]
        [InlineData(CogniteHost.BlueField)]
        public async Task TestUpdateTimeSeries(CogniteHost host)
        {
            using var tester = new CDFTester(host, _output);

            var tss = await CreateTestTimeSeries(tester);

            var updates = tss.Select(x => GetWritableTS(tester, x.ExternalId, null, x => { x.Properties.Description = "new description"; return x; }));

            try
            {
                var result = await tester.DestinationWithIDM.UpsertTimeSeriesAsync(updates, RetryMode.None, SanitationMode.None, tester.Source.Token);
                result.Throw();

                Assert.Equal(3, result.Results.Count());
                Assert.True(result.IsAllGood);

                var retrieved = await tester.DestinationWithIDM.GetTimeSeriesByIdsIgnoreErrors<CogniteExtractorTimeSeries>(tss.Select(x => new Identity(x)), tester.Source.Token);
                Assert.All(retrieved, ts => Assert.Equal("new description", ts.Properties.Description));
            }
            finally
            {
                await tester.DestinationWithIDM.CogniteClient.DataModels.DeleteInstances(tss.Select(x => new InstanceIdentifierWithType(InstanceType.node, x)), tester.Source.Token);
            }
        }

        [Theory]
        [InlineData(CogniteHost.GreenField)]
        [InlineData(CogniteHost.BlueField)]
        public async Task TestUpdateSanitation(CogniteHost host)
        {
            using var tester = new CDFTester(host, _output);

            var tss = await CreateTestTimeSeries(tester);

            var meta = Enumerable.Range(0, 100)
                    .ToDictionary(i => $"key{i:000}{new string('æ', 100)}", i => new string('æ', 200));

            var updates = new[]
            {
                GetWritableTS(tester, tss[0].ExternalId, null, x => {
                    x.Properties.Description = new string('æ', 2000);
                    x.Properties.extractedData = meta;
                    x.Properties.Name = new string('æ', 300);
                    x.Properties.SourceUnit = new string('æ', 200);
                    return x;
                }),
                GetWritableTS(tester, tss[1].ExternalId, null, x => {x.Properties.Name = "name"; return x;}),
                GetWritableTS(tester, tss[1].ExternalId, null, x => {x.Properties.Name = "name"; return x;})
            };

            try
            {
                var result = await tester.DestinationWithIDM.UpsertTimeSeriesAsync(updates, RetryMode.OnError, SanitationMode.Clean, tester.Source.Token);
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
                await tester.DestinationWithIDM.CogniteClient.DataModels.DeleteInstances(tss.Select(x => new InstanceIdentifierWithType(InstanceType.node, x)), tester.Source.Token);
            }
        }

        // This feature depends on fields currently not present on DM, ex: https://api-docs.cognite.com/20230101/tag/Time-series/operation/alterTimeSeries#!c=422&path=error/duplicated&t=response
        //[Theory]
        //[InlineData(CogniteHost.GreenField)]
        //[InlineData(CogniteHost.BlueField)]
        //public async Task TestUpdateErrorHandling(CogniteHost host)
        //{
        //    using var tester = new CDFTester(host, _output);

        //    var tss = await CreateTestTimeSeries(tester);

        //    Func<SourcedNodeWrite<CogniteExtractorTimeSeries>, SourcedNodeWrite<CogniteExtractorTimeSeries>> upd = x => { x.Properties.Description = "new description"; return x; };

        //    var updates1 = new[]
        //    {
        //        // Existing timeseries
        //        GetWritableTS(tester, tss[0].ExternalId, null, x => {x.ExternalId =tss[1].ExternalId; return x;}),
        //        // Update OK
        //        GetWritableTS(tester, tss[1].ExternalId, null, upd),
        //        // Missing by external id
        //        GetWritableTS(tester, "missing-ts", null, upd),
        //    };

        //    try
        //    {
        //        var result1 = await tester.DestinationWithIDM.UpsertTimeSeriesAsync(updates1, RetryMode.OnError, SanitationMode.None, tester.Source.Token);
        //        result1.ThrowOnFatal();

        //        var errs = result1.Errors.ToArray();
        //        Assert.Equal(2, errs.Length);

        //        var err = errs.First(e => e.Type == ErrorType.ItemExists && e.Resource == ResourceType.ExternalId);
        //        Assert.Single(err.Skipped);
        //        Assert.Contains(err.Values, e => e.ExternalId == tss[1].ExternalId);

        //        err = errs.First(e => e.Type == ErrorType.ItemMissing && e.Resource == ResourceType.AssetId);
        //        Assert.Single(err.Skipped);
        //        Assert.Contains(err.Values, e => e.Id == 123);
        //    }
        //    finally
        //    {
        //        await tester.DestinationWithIDM.CogniteClient.DataModels.DeleteInstances(tss.Select(x => new InstanceIdentifierWithType(InstanceType.node, x)), tester.Source.Token);
        //    }
        //}

        [Theory]
        [InlineData(CogniteHost.GreenField)]
        [InlineData(CogniteHost.BlueField)]
        public async Task TestCreateDataPoints(CogniteHost host)
        {
            using var tester = new CDFTester(host, _output);

            var tss = await CreateTestTimeSeries(tester);

            var dps = new Dictionary<Identity, IEnumerable<Datapoint>>()
            {
                { new Identity(tss[0]), Enumerable.Range(0, 10)
                    .Select(i => new Datapoint(DateTime.UtcNow.AddSeconds(i), i)).ToList() },
                { new Identity(tss[1]), Enumerable.Range(0, 10)
                    .Select(i => new Datapoint(DateTime.UtcNow.AddSeconds(i), $"value{i}")).ToList() },
                { new Identity(tss[2]), Enumerable.Range(0, 10)
                    .Select(i => new Datapoint(DateTime.UtcNow.AddSeconds(i), i)).ToList() }
            };

            tester.Config.Cognite.CdfChunking.DataPointTimeSeries = 2;
            tester.Config.Cognite.CdfChunking.DataPoints = 5;

            try
            {
                var result = await tester.DestinationWithIDM.InsertDataPointsAsync(dps, SanitationMode.None, RetryMode.None, tester.Source.Token);
                Assert.Empty(result.Errors);

                int[] counts = new int[3];
                for (int i = 0; i < 10; i++)
                {
                    var foundDps = await tester.DestinationWithIDM.CogniteClient.DataPoints.ListAsync(new DataPointsQuery
                    {
                        Items = tss.Select(ts => new DataPointsQueryItem
                        {
                            InstanceId = ts,
                            End = DateTime.UtcNow.AddDays(1).ToUnixTimeMilliseconds().ToString()
                        }).ToArray()
                    });
                    if (foundDps.Items.Count() == 3)
                    {
                        counts[0] = foundDps.Items[0]?.NumericDatapoints?.Datapoints?.Count() ?? 0;
                        counts[1] = foundDps.Items[1]?.StringDatapoints?.Datapoints?.Count() ?? 0;
                        counts[2] = foundDps.Items[2]?.NumericDatapoints?.Datapoints?.Count() ?? 0;
                        if (counts.All(cnt => cnt == 10)) break;
                    }
                    await Task.Delay(1000);
                }
                Assert.All(counts, cnt => Assert.Equal(10, cnt));
            }
            finally
            {
                await tester.DestinationWithIDM.CogniteClient.DataModels.DeleteInstances(tss.Select(x => new InstanceIdentifierWithType(InstanceType.node, x)), tester.Source.Token);
            }
        }
        [Theory]
        [InlineData(CogniteHost.GreenField)]
        [InlineData(CogniteHost.BlueField)]
        public async Task TestDataPointsSanitation(CogniteHost host)
        {
            using var tester = new CDFTester(host, _output);

            var tss = await CreateTestTimeSeries(tester);

            Dictionary<Identity, IEnumerable<Datapoint>> GetCreates()
            {
                return new Dictionary<Identity, IEnumerable<Datapoint>>()
                {
                    { Identity.Create(tss[0]), new []
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
                    { Identity.Create(tss[1]), new []
                    {
                        new Datapoint(DateTime.UtcNow, new string('æ', 400)),
                        new Datapoint(DateTime.UtcNow.AddSeconds(1), "test"),
                        new Datapoint(DateTime.UtcNow, null)
                    } },
                    { Identity.Create(tss[2]), new[]
                    {
                        new Datapoint(DateTime.UtcNow, double.NaN)
                    } }
                };
            }

            try
            {
                var result = await tester.DestinationWithIDM.InsertDataPointsAsync(GetCreates(), SanitationMode.Remove, RetryMode.None, tester.Source.Token);

                var errs = result.Errors.ToArray();
                Assert.Equal(2, errs.Length);

                var err = errs[0];
                Assert.Equal(ResourceType.DataPointTimestamp, err.Resource);
                Assert.Equal(ErrorType.SanitationFailed, err.Type);
                Assert.Single(err.Skipped);
                var iErr = err.Skipped.OfType<DataPointInsertError>().First();
                Assert.Equal(2, iErr.DataPoints.Count());
                Assert.Equal(tss[0].ToString(), ((Identity)iErr.Id).ToString());

                err = errs[1];
                Assert.Equal(ResourceType.DataPointValue, err.Resource);
                Assert.Equal(ErrorType.SanitationFailed, err.Type);
                Assert.Equal(3, err.Skipped.Count());
                var insertErrs = err.Skipped.OfType<DataPointInsertError>().ToArray();

                iErr = insertErrs[0];
                Assert.Equal(5, iErr.DataPoints.Count());
                Assert.Equal(tss[0].ToString(), ((Identity)iErr.Id).ToString());
                iErr = insertErrs[1];
                Assert.Equal(2, iErr.DataPoints.Count());
                Assert.Equal(tss[1].ToString(), ((Identity)iErr.Id).ToString());
                iErr = insertErrs[2];
                Assert.Single(iErr.DataPoints);
                Assert.Equal(tss[2].ToString(), ((Identity)iErr.Id).ToString());

                tester.Config.Cognite.NanReplacement = 123;

                result = await tester.DestinationWithIDM.InsertDataPointsAsync(GetCreates(), SanitationMode.Clean, RetryMode.None, tester.Source.Token);

                errs = result.Errors.ToArray();
                Assert.Single(errs);

                err = errs[0];
                Assert.Equal(ResourceType.DataPointTimestamp, err.Resource);
                Assert.Equal(ErrorType.SanitationFailed, err.Type);
                Assert.Single(err.Skipped);
                iErr = err.Skipped.OfType<DataPointInsertError>().First();
                Assert.Equal(2, iErr.DataPoints.Count());
                Assert.Equal(tss[0].ToString(), ((Identity)iErr.Id).ToString());
            }
            finally
            {
                await tester.DestinationWithIDM.CogniteClient.DataModels.DeleteInstances(tss.Select(x => new InstanceIdentifierWithType(InstanceType.node, x)), tester.Source.Token);
            }
        }
        [Theory]
        [InlineData(CogniteHost.GreenField)]
        [InlineData(CogniteHost.BlueField)]
        public async Task TestDataPointsErrorHandling(CogniteHost host)
        {
            using var tester = new CDFTester(host, _output);

            var tss = await CreateTestTimeSeries(tester);

            var dps = new Dictionary<Identity, IEnumerable<Datapoint>>()
            {
                // All mismatched
                { Identity.Create(tss[0]), new [] {
                    new Datapoint(DateTime.UtcNow, "test"),
                    new Datapoint(DateTime.UtcNow.AddSeconds(1), "test2")
                } },
                // Some mismatched datapoints
                { Identity.Create(tss[1]), new[]
                {
                    new Datapoint(DateTime.UtcNow, 1.0),
                    new Datapoint(DateTime.UtcNow.AddSeconds(1), 2.0),
                    new Datapoint(DateTime.UtcNow.AddSeconds(2), "test")
                } },
                { Identity.Create(tss[2]), new[]
                {
                    new Datapoint(DateTime.UtcNow, 1.0),
                    new Datapoint(DateTime.UtcNow.AddSeconds(1), "test"),
                    new Datapoint(DateTime.UtcNow.AddSeconds(2), "test2")
                } },
                { Identity.Create(new InstanceIdentifier(tester.SpaceId, "missing-ts-1")), new[] { new Datapoint(DateTime.UtcNow, 1.0) } },
            };

            try
            {
                var result = await tester.DestinationWithIDM.InsertDataPointsAsync(dps, SanitationMode.None, RetryMode.OnError, tester.Source.Token);

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
                await tester.DestinationWithIDM.CogniteClient.DataModels.DeleteInstances(tss.Select(x => new InstanceIdentifierWithType(InstanceType.node, x)), tester.Source.Token);
            }
        }
        [Theory]
        [InlineData(CogniteHost.GreenField)]
        [InlineData(CogniteHost.BlueField)]
        public async Task TestDataPointsCreateMissing(CogniteHost host)
        {
            using var tester = new CDFTester(host, _output);

            var tss = await CreateTestTimeSeries(tester);

            var dps = new Dictionary<Identity, IEnumerable<Datapoint>>()
            {
                { Identity.Create(tss[0]), new[]
                {
                    new Datapoint(DateTime.UtcNow, 1.0),
                    new Datapoint(DateTime.UtcNow.AddSeconds(1), 2.0),
                    new Datapoint(DateTime.UtcNow.AddSeconds(2), 3.0)
                } },
                { Identity.Create(new InstanceIdentifier(tester.SpaceId, $"{tester.Prefix} utils-test-ts-missing-1")), new[] { new Datapoint(DateTime.UtcNow, 1.0) } },
                { Identity.Create(new InstanceIdentifier(tester.SpaceId, $"{tester.Prefix} utils-test-ts-missing-2")), new[] { new Datapoint(DateTime.UtcNow, "test") } },
            };

            try
            {
                var (dpResult, tsResult) = await tester.DestinationWithIDM
                    .InsertDataPointsCreateMissingAsync(dps, SanitationMode.Clean, RetryMode.OnError, tester.Source.Token);

                Assert.Single(dpResult.Errors);
                var err = dpResult.Errors.First();
                Assert.Equal(ErrorType.ItemMissing, err.Type);

                Assert.True(tsResult.Errors == null || !tsResult.Errors.Any());
                Assert.Equal(2, tsResult.Results.Count());
            }
            finally
            {
                var toDel = tss
                    .Select(ts => ts.ExternalId)
                    .Concat(new[] { $"{tester.Prefix} utils-test-ts-missing-1", $"{tester.Prefix} utils-test-ts-missing-2" })
                    .Select(x => new InstanceIdentifier(tester.SpaceId, x));
                await tester.DestinationWithIDM.CogniteClient.DataModels.DeleteInstances(toDel.Select(x => new InstanceIdentifierWithType(InstanceType.node, x)), tester.Source.Token);
            }
        }
    }
}
