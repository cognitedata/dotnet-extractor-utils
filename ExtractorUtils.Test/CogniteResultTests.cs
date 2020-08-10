using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extensions;
using CogniteSdk;
using Xunit;

namespace ExtractorUtils.Test
{
    public class CogniteResultTests
    {
        [Fact]
        public void ParseUnknownException()
        {
            var ex = new Exception("Some error");
            var error = ResultHandlers.ParseException(ex, RequestType.CreateAssets);
            Assert.Equal(0, error.Status);
            Assert.Equal(ex, error.Exception);
            Assert.Equal("Some error", error.Message);
        }
        [Fact]
        public void ParseResponseException()
        {
            var ex = new ResponseException("Something bad happened")
            {
                Code = 500
            };
            var error = ResultHandlers.ParseException(ex, RequestType.CreateAssets);
            Assert.Equal(500, error.Status);
            Assert.Equal(ex, error.Exception);
            Assert.Equal("Something bad happened", error.Message);
        }

        [Fact]
        public async Task CleanAssetRequest()
        {
            var assets = new[]
            {
                new AssetCreate
                {
                    ExternalId = "duplicate",
                },
                new AssetCreate
                {
                    ExternalId = "duplicate2"
                },
                new AssetCreate
                {
                    DataSetId = 123
                },
                new AssetCreate
                {
                    DataSetId = 1234
                },
                new AssetCreate
                {
                    ParentExternalId = "missing"
                },
                new AssetCreate
                {
                    ParentExternalId = "missing"
                },
                new AssetCreate
                {
                    ParentId = 123
                },
                new AssetCreate
                {
                    ParentId = 1234
                },
                new AssetCreate
                {
                    ExternalId = "ImOk"
                }
            };
            var exceptions = new[]
            {
                new ResponseException
                {
                    Duplicated = new [] { new Dictionary<string, MultiValue> {
                        { "externalId", MultiValue.Create("duplicate") } },
                    new Dictionary<string, MultiValue> {
                        { "externalId", MultiValue.Create("duplicate2") } }},
                    Code = 409
                },
                new ResponseException("Invalid dataSetIds: 123, 1234") { Code = 400 },
                new ResponseException("Reference to unknown parent with externalId missing") { Code = 400 },
                new ResponseException("The given parent ids do not exist: 123, 1234") { Code = 400 }
            };
            var errors = new List<CogniteError>();
            for (int i = 0; i < exceptions.Length; i++)
            {
                var error = ResultHandlers.ParseException(exceptions[i], RequestType.CreateAssets);
                assets = (await ResultHandlers.CleanFromError(null, error, assets, 1000, 1, CancellationToken.None))
                    .ToArray();
                Assert.Equal(9 - i * 2 - 2, assets.Count());
                errors.Add(error);
            }

            Assert.Single(assets);
            Assert.Equal("ImOk", assets[0].ExternalId);
            Assert.Equal(4, errors.Count);
            for (int i = 0; i < exceptions.Length; i++)
            {
                Assert.Equal(errors[i].Exception, exceptions[i]);
                Assert.Equal(exceptions[i].Message, errors[i].Message);
            }

        }
    }
}
