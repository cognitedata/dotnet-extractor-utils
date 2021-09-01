using Cognite.Extensions;
using CogniteSdk;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace ExtractorUtils.Test.Integration
{
    public class SequenceIntegrationTest
    {
        private static async Task SafeDelete(string[] ids, CDFTester tester)
        {
            var toDelete = ids;

            while (toDelete.Any())
            {
                try
                {
                    await tester.Destination.CogniteClient.Sequences.DeleteAsync(ids, tester.Source.Token);
                    break;
                }
                catch (ResponseException ex)
                {
                    if (!ex.Missing.Any()) break;
                    int cnt = toDelete.Length;
                    toDelete = toDelete
                        .Where(id => !ex.Missing.Any(dict => dict.TryGetValue("externalId", out var val)
                               && (val as MultiValue.String)?.Value == id))
                        .ToArray();
                    if (toDelete.Length == cnt) break;
                }
            }
        }

        [Theory]
        [InlineData(CogniteHost.GreenField)]
        [InlineData(CogniteHost.BlueField)]
        public async Task TestCreateSequences(CogniteHost host)
        {
            using var tester = new CDFTester(host);
            var ids = new[]
            {
                $"{tester.Prefix} seq-1",
                $"{tester.Prefix} seq-2",
                $"{tester.Prefix} seq-3",
                $"{tester.Prefix} seq-4",
                $"{tester.Prefix} seq-5"
            };

            var columns = new[]
            {
                new SequenceColumnWrite
                {
                    ExternalId = "col"
                }
            };

            var sequences1 = new[]
            {
                new SequenceCreate
                {
                    ExternalId = ids[0],
                    Name = ids[0],
                    Columns = columns
                },
                new SequenceCreate
                {
                    ExternalId = ids[1],
                    Name = ids[1],
                    Columns = columns
                },
                new SequenceCreate
                {
                    ExternalId = ids[2],
                    Name = ids[2],
                    Columns = columns
                }
            };

            try
            {
                var result = await tester.Destination.EnsureSequencesExistsAsync(sequences1, RetryMode.None, SanitationMode.None, tester.Source.Token);
                Assert.True(result.IsAllGood);
                Assert.Equal(3, result.Results.Count());

                var sequences2 = new[]
                {
                    sequences1[0],
                    new SequenceCreate
                    {
                        ExternalId = ids[3],
                        Name = ids[3],
                        Columns = columns
                    }
                };

                result = await tester.Destination.EnsureSequencesExistsAsync(sequences2, RetryMode.OnError, SanitationMode.None, tester.Source.Token);

                Assert.Single(result.Errors);
                var error = result.Errors.First();
                Assert.Equal(ErrorType.ItemExists, error.Type);
                Assert.Equal(ResourceType.ExternalId, error.Resource);

                result = await tester.Destination.GetOrCreateSequencesAsync(ids, toCreate =>
                {
                    Assert.Single(toCreate);
                    Assert.Equal(ids[4], toCreate.First());
                    return new[]
                    {
                        new SequenceCreate
                        {
                            ExternalId = ids[4],
                            Name = ids[4],
                            Columns = columns
                        }
                    };
                }, RetryMode.OnError, SanitationMode.None, tester.Source.Token);
                Assert.Equal(5, result.Results.Count());
                Assert.Equal(Enumerable.Range('1', 5), result.Results.Select(res => (int)res.ExternalId.Last()).OrderBy(v => v));
            }
            finally
            {
                await SafeDelete(ids, tester);
            }
        }
        [Theory]
        [InlineData(CogniteHost.GreenField)]
        [InlineData(CogniteHost.BlueField)]
        public async Task TestSanitation(CogniteHost host)
        {
            using var tester = new CDFTester(host);

            var columns = new[] { new SequenceColumnWrite
            {
                ExternalId = "col"
            } };

            var sequences = new[]
            {
                new SequenceCreate
                {
                    ExternalId = tester.Prefix + new string('æ', 300),
                    Name = new string('æ', 1000),
                    Metadata = Enumerable.Range(0, 200)
                        .ToDictionary(i => $"key{i.ToString("000")}{new string('æ', 100)}", i => new string('æ', 600)),
                    Description = new string('æ', 2000),
                    AssetId = -123,
                    DataSetId = -123,
                    Columns = Enumerable.Range(0, 20).Select(i =>
                        new SequenceColumnWrite
                        {
                            ExternalId = i + new string('æ', 300),
                            Name = new string('æ', 300),
                            Description = new string('æ', 2000),
                            MetaData = Enumerable.Range(0, 200)
                                    .ToDictionary(i => $"key{i.ToString("000")}{new string('æ', 100)}", i => new string('æ', 600)),
                        }).ToArray()
                },
                new SequenceCreate
                {
                    ExternalId = $"{tester.Prefix} test-no-columns",
                    Columns = null
                },
                new SequenceCreate
                {
                    ExternalId = $"{tester.Prefix} test-duplicate-colums",
                    Columns = new [] { columns[0], columns[0] }
                },
                new SequenceCreate
                {
                    ExternalId = $"{tester.Prefix} test-duplicate-id",
                    Columns = columns
                },
                new SequenceCreate
                {
                    ExternalId = $"{tester.Prefix} test-duplicate-id",
                    Columns = columns
                }
            };

            try
            {
                var result = await tester.Destination.EnsureSequencesExistsAsync(sequences, RetryMode.OnError, SanitationMode.Clean, tester.Source.Token);


                var errs = result.Errors.ToList();
                Assert.Equal(ErrorType.SanitationFailed, errs[0].Type);
                Assert.Equal(ResourceType.SequenceColumns, errs[0].Resource);
                Assert.Equal(ErrorType.ItemDuplicated, errs[1].Type);
                Assert.Equal(ResourceType.ColumnExternalId, errs[1].Resource);
                Assert.Equal(ErrorType.ItemDuplicated, errs[2].Type);
                Assert.Equal(ResourceType.ExternalId, errs[2].Resource);

                Assert.Equal(3, result.Errors.Count());
                

                Assert.Equal(2, result.Results.Count());
                Assert.Equal(tester.Prefix + new string('æ', 255 - tester.Prefix.Length), result.Results.First().ExternalId);
                Assert.Equal($"{tester.Prefix} test-duplicate-id", result.Results.Last().ExternalId);
            }
            finally
            {
                var ids = new[]
                {
                    tester.Prefix + new string('æ', 255 - tester.Prefix.Length),
                    $"{tester.Prefix} test-duplicate-id"
                };
                await SafeDelete(ids, tester);
            }
        }
    }
}
