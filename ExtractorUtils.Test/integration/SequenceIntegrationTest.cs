using Cognite.Extensions;
using CogniteSdk;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace ExtractorUtils.Test.Integration
{
    public class SequenceIntegrationTest
    {
        private readonly ITestOutputHelper _output;
        public SequenceIntegrationTest(ITestOutputHelper output)
        {
            _output = output;
        }

        private static async Task SafeDelete(string[] ids, CDFTester tester)
        {
            // There's no "ignore missing" functionality on sequence delete, so we need this.
            var toDelete = ids;

            while (toDelete.Any())
            {
                try
                {
                    await tester.Destination.CogniteClient.Sequences.DeleteAsync(toDelete, tester.Source.Token);
                    break;
                }
                catch (ResponseException ex)
                {
                    if (!ex.Missing?.Any() ?? true) break;
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
            using var tester = new CDFTester(host, _output);
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
            using var tester = new CDFTester(host, _output);

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
                            Metadata = Enumerable.Range(0, 200)
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
                foreach (var err in errs)
                {
                    _output.WriteLine(err.Message + ", " + err.Type + ", " + err.Resource);
                }

                Assert.Equal(3, errs.Count);
                Assert.Contains(errs, err => err.Type == ErrorType.SanitationFailed && err.Resource == ResourceType.SequenceColumns);
                Assert.Contains(errs, err => err.Type == ErrorType.ItemDuplicated && err.Resource == ResourceType.ColumnExternalId);
                Assert.Contains(errs, err => err.Type == ErrorType.ItemDuplicated && err.Resource == ResourceType.ExternalId);

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
        [Theory]
        [InlineData(CogniteHost.GreenField)]
        [InlineData(CogniteHost.BlueField)]
        public async Task TestErrorHandling(CogniteHost host)
        {
            using var tester = new CDFTester(host, _output);

            var columns = new[] { new SequenceColumnWrite
            {
                ExternalId = "col"
            } };

            await tester.Destination.EnsureSequencesExistsAsync(new[]
            {
                new SequenceCreate
                {
                    ExternalId = $"{tester.Prefix} existing-sequence",
                    Columns = columns
                },
            }, RetryMode.None, SanitationMode.None, tester.Source.Token);

            var sequences = new[]
            {
                new SequenceCreate
                {
                    Name = "existing-sequence",
                    ExternalId = $"{tester.Prefix} existing-sequence",
                    Columns = columns
                },
                new SequenceCreate
                {
                    Name = "test-missing-asset-1",
                    ExternalId = $"{tester.Prefix} test-missing-asset-1",
                    AssetId = 123,
                    Columns = columns
                },
                new SequenceCreate
                {
                    Name = "test-missing-asset-2",
                    ExternalId = $"{tester.Prefix} test-missing-asset-2",
                    AssetId = 123,
                    Columns = columns
                },
                new SequenceCreate
                {
                    Name = "test-missing-asset-3",
                    ExternalId = $"{tester.Prefix} test-missing-asset-3",
                    AssetId = 124,
                    Columns = columns
                },
                new SequenceCreate
                {
                    Name = "test-missing-dataset-1",
                    ExternalId = $"{tester.Prefix} test-missing-dataset-1",
                    DataSetId = 123,
                    Columns = columns
                },
                new SequenceCreate
                {
                    Name = "test-missing-dataset-2",
                    ExternalId = $"{tester.Prefix} test-missing-dataset-2",
                    DataSetId = 124,
                    Columns = columns
                },
                new SequenceCreate
                {
                    Name = "test-final-ok",
                    ExternalId = $"{tester.Prefix} test-final-ok",
                    Columns = columns
                }
            };

            try
            {
                var result = await tester.Destination.EnsureSequencesExistsAsync(sequences, RetryMode.OnError, SanitationMode.Clean, tester.Source.Token);

                Assert.Single(result.Results);
                Assert.Equal(3, result.Errors.Count());
                Assert.Equal("test-final-ok", result.Results.First().Name);

                foreach (var error in result.Errors)
                {
                    switch (error.Resource)
                    {
                        case ResourceType.ExternalId:
                            Assert.Equal(ErrorType.ItemExists, error.Type);
                            Assert.Single(error.Values);
                            Assert.Equal($"{tester.Prefix} existing-sequence", error.Values.First().ExternalId);
                            Assert.Single(error.Skipped);
                            break;
                        case ResourceType.AssetId:
                            Assert.Equal(ErrorType.ItemMissing, error.Type);
                            Assert.Equal(2, error.Values.Count());
                            Assert.Contains(error.Values, idt => idt.Id == 123);
                            Assert.Contains(error.Values, idt => idt.Id == 124);
                            Assert.Equal(3, error.Skipped.Count());
                            break;
                        case ResourceType.DataSetId:
                            Assert.Equal(ErrorType.ItemMissing, error.Type);
                            Assert.Equal(2, error.Values.Count());
                            Assert.Contains(error.Values, idt => idt.Id == 123);
                            Assert.Contains(error.Values, idt => idt.Id == 124);
                            Assert.Equal(2, error.Skipped.Count());
                            break;
                        default:
                            throw new Exception($"Bad resource type: {error.Type}", error.Exception);
                    }
                }
            }
            finally
            {
                var ids = sequences.Select(seq => seq.ExternalId).ToArray();
                await SafeDelete(ids, tester);
            }
        }


        private async Task<(string extId, long id)[]> CreateTestSequences(CDFTester tester)
        {
            var columns = new[]
                {
                    new SequenceColumnWrite
                    {
                        ExternalId = "col1",
                        ValueType = MultiValueType.DOUBLE
                    },
                    new SequenceColumnWrite
                    {
                        ExternalId = "col2",
                        ValueType = MultiValueType.LONG
                    },
                    new SequenceColumnWrite
                    {
                        ExternalId = "col3",
                        ValueType = MultiValueType.STRING
                    }
                };

            var sequences = new[] {
                new SequenceCreate
                {
                    ExternalId = $"{tester.Prefix} test-create-rows-1",
                    Columns = columns
                },
                new SequenceCreate
                {
                    ExternalId = $"{tester.Prefix} test-create-rows-2",
                    Columns = columns
                },
                new SequenceCreate
                {
                    ExternalId = $"{tester.Prefix} test-create-rows-3",
                    Columns = columns
                }
            };
            var results = await tester.Destination.EnsureSequencesExistsAsync(sequences, RetryMode.None, SanitationMode.None, tester.Source.Token);
            return results.Results.Select(seq => (seq.ExternalId, seq.Id)).ToArray();
        }

        [Theory]
        [InlineData(CogniteHost.GreenField)]
        [InlineData(CogniteHost.BlueField)]
        public async Task TestRowsCreate(CogniteHost host)
        {
            using var tester = new CDFTester(host, _output);
            var ids = await CreateTestSequences(tester);
            var columns = new[] { "col1", "col3", "col2" };
            var writes = new[]
            {
                new SequenceDataCreate
                {
                    Columns = columns,
                    ExternalId = ids[0].extId,
                    Rows = Enumerable.Range(0, 10).Select(i =>
                        new SequenceRow
                        {
                            RowNumber = i,
                            Values = new MultiValue[] { MultiValue.Create(123.2), MultiValue.Create("test"), MultiValue.Create(i) }
                        }).ToArray()
                },
                new SequenceDataCreate
                {
                    Columns = columns,
                    Id = ids[1].id,
                    Rows = Enumerable.Range(0, 10).Select(i =>
                        new SequenceRow
                        {
                            RowNumber = i,
                            Values = new MultiValue[] { MultiValue.Create(123.2), MultiValue.Create("test"), MultiValue.Create(i) }
                        }).ToArray()
                },
                new SequenceDataCreate
                {
                    Columns = columns,
                    ExternalId = ids[2].extId,
                    Rows = Enumerable.Range(0, 10).Select(i =>
                        new SequenceRow
                        {
                            RowNumber = i,
                            Values = new MultiValue[] { MultiValue.Create(123.2), MultiValue.Create("test"), MultiValue.Create(i) }
                        }).ToArray()
                }
            };
            tester.Config.Cognite.CdfChunking.SequenceRowSequences = 2;
            tester.Config.Cognite.CdfChunking.SequenceRows = 5;

            try
            {
                var result = await tester.Destination.InsertSequenceRowsAsync(writes, RetryMode.None, SanitationMode.None, tester.Source.Token);
                Assert.Empty(result.Errors);

                bool found = true;
                int[] counts = new int[3];
                // Need to potentially wait a while for results to show...
                // For some reason this is much slower on greenfield
                for (int i = 0; i < 10; i++)
                {
                    found = true;
                    for (int j = 0; j < 3; j++)
                    {
                        var retrieved = await tester.Destination.CogniteClient.Sequences.ListRowsAsync(
                            new SequenceRowQuery
                            {
                                Limit = 1000,
                                ExternalId = ids[j].extId
                            }, tester.Source.Token);
                        found &= retrieved.Rows.Count() == 10;
                        counts[j] = retrieved.Rows.Count();
                        Assert.Null(retrieved.NextCursor);
                    }
                    if (found) break;
                    await Task.Delay(1000);
                }
                Assert.True(found, string.Join(",", counts));
            }
            finally
            {
                await SafeDelete(ids.Select(id => id.extId).ToArray(), tester);
            }
        }

        [Theory]
        [InlineData(CogniteHost.GreenField)]
        [InlineData(CogniteHost.BlueField)]
        public async Task TestRowsSanitation(CogniteHost host)
        {
            using var tester = new CDFTester(host, _output);
            var ids = await CreateTestSequences(tester);

            var columns = new[] { "col1", "col3", "col2" };

            SequenceRow GetRow(int num, double dVal, long lVal, string sVal)
            {
                return new SequenceRow
                {
                    RowNumber = num,
                    Values = new MultiValue[] { MultiValue.Create(dVal), MultiValue.Create(sVal), MultiValue.Create(lVal) }
                };
            }


            var creates1 = new[]
            {
                // Duplicate extId
                new SequenceDataCreate
                {
                    ExternalId = ids[0].extId,
                    Columns = columns,
                    Rows = new[]
                    {
                        GetRow(0, 123.4, 1, "test")
                    }
                },
                new SequenceDataCreate
                {
                    ExternalId = ids[0].extId,
                    Columns = columns,
                    Rows = new[]
                    {
                        GetRow(0, 123.4, 1, "test")
                    }
                },
                // Duplicate internalId
                new SequenceDataCreate
                {
                    Id = ids[1].id,
                    Columns = columns,
                    Rows = new[]
                    {
                        GetRow(0, 123.4, 1, "test")
                    }
                },
                new SequenceDataCreate
                {
                    Id = ids[1].id,
                    Columns = columns,
                    Rows = new[]
                    {
                        GetRow(0, 123.4, 1, "test")
                    }
                },
                // Duplicate columns
                new SequenceDataCreate
                {
                    Id = ids[2].id,
                    Columns = new string[] { columns[0], columns[0], columns[0] },
                    Rows = new []
                    {
                        GetRow(0, 123.4, 1, "test")
                    }
                }
            };

            SequenceDataCreate[] GetCreates2()
            {
                return new[]
                {
                    // Misc bad rows
                    new SequenceDataCreate
                    {
                        ExternalId = ids[0].extId,
                        Columns = columns,
                        Rows = new []
                        {
                            // Bad double
                            GetRow(0, double.NaN, 1, "test"),
                            // Too large double
                            GetRow(1, 1E101, 1, "test"),
                            // Too long string
                            GetRow(2, 123.4, 1, new string('æ', 300)),
                            // Duplicate row number
                            GetRow(3, 123.4, 1, "test"),
                            GetRow(3, 123.4, 1, "test"),
                            // Negative row number
                            GetRow(-1, 123.4, 1, "test"),
                            // Null values
                            new SequenceRow { RowNumber = 4, Values = null },
                            // Too few values
                            new SequenceRow { RowNumber = 5, Values = new MultiValue[] { null, null } }
                        }
                    },
                    // Null columns
                    new SequenceDataCreate
                    {
                        ExternalId = ids[1].extId,
                        Columns = null,
                        Rows = new []
                        {
                            GetRow(0, 123.4, 1, "test")
                        }
                    },
                    // Null rows
                    new SequenceDataCreate
                    {
                        ExternalId = ids[2].extId,
                        Columns = columns,
                        Rows = null
                    }
                };
            }


            try
            {
                var result = await tester.Destination.InsertSequenceRowsAsync(creates1, RetryMode.OnError, SanitationMode.Remove, tester.Source.Token);
                var errs = result.Errors.ToArray();
                Assert.Equal(2, errs.Length);
                var err = errs.First(e => e.Resource == ResourceType.ColumnExternalId && e.Type == ErrorType.ItemDuplicated);
                Assert.Single(err.Skipped);
                err = errs.First(e => e.Resource == ResourceType.Id && e.Type == ErrorType.ItemDuplicated);
                Assert.Equal(2, err.Values.Count());

                var creates2 = GetCreates2();
                result = await tester.Destination.InsertSequenceRowsAsync(creates2, RetryMode.OnError, SanitationMode.Remove, tester.Source.Token);
                errs = result.Errors.ToArray();
                Assert.Equal(5, errs.Length);
                err = errs.First(e => e.Resource == ResourceType.SequenceRowNumber && e.Type == ErrorType.ItemDuplicated);
                Assert.Single(err.Skipped);

                err = errs.First(e => e.Resource == ResourceType.SequenceColumns && e.Type == ErrorType.SanitationFailed);
                Assert.Single(err.Skipped);

                err = errs.First(e => e.Resource == ResourceType.SequenceRows && e.Type == ErrorType.SanitationFailed);
                Assert.Single(err.Skipped);

                err = errs.First(e => e.Resource == ResourceType.SequenceRowValues && e.Type == ErrorType.SanitationFailed);
                Assert.Single(err.Skipped);
                Assert.Equal(5, err.Skipped.First().SkippedRows.Count());

                err = errs.First(e => e.Resource == ResourceType.SequenceRowNumber && e.Type == ErrorType.SanitationFailed);
                Assert.Single(err.Skipped);

                // The inserts are modified in-place
                creates2 = GetCreates2();
                result = await tester.Destination.InsertSequenceRowsAsync(creates2, RetryMode.OnError, SanitationMode.Clean, tester.Source.Token);
                errs = result.Errors.ToArray();
                Assert.Equal(5, errs.Length);

                err = errs.First(e => e.Resource == ResourceType.SequenceRowNumber && e.Type == ErrorType.ItemDuplicated);
                Assert.Single(err.Skipped);

                err = errs.First(e => e.Resource == ResourceType.SequenceColumns && e.Type == ErrorType.SanitationFailed);
                Assert.Single(err.Skipped);

                err = errs.First(e => e.Resource == ResourceType.SequenceRows && e.Type == ErrorType.SanitationFailed);
                Assert.Single(err.Skipped);

                err = errs.First(e => e.Resource == ResourceType.SequenceRowNumber && e.Type == ErrorType.SanitationFailed);
                Assert.Single(err.Skipped);

                // Three of the bad rows have now been cleaned and should not be removed
                err = errs.First(e => e.Resource == ResourceType.SequenceRowValues && e.Type == ErrorType.SanitationFailed);
                Assert.Single(err.Skipped);
                Assert.Equal(2, err.Skipped.First().SkippedRows.Count());
            }
            finally
            {
                await SafeDelete(ids.Select(id => id.extId).ToArray(), tester);
            }
        }

        [Theory]
        [InlineData(CogniteHost.GreenField)]
        [InlineData(CogniteHost.BlueField)]
        public async Task TestRowsErrorHandling(CogniteHost host)
        {
            using var tester = new CDFTester(host, _output);
            var ids = await CreateTestSequences(tester);

            var columns = new[] { "col1", "col3", "col2" };

            SequenceRow GetRow(int num, double dVal, long lVal, string sVal)
            {
                return new SequenceRow
                {
                    RowNumber = num,
                    Values = new MultiValue[] { MultiValue.Create(dVal), MultiValue.Create(sVal), MultiValue.Create(lVal) }
                };
            }

            var creates = new[]
            {
                // Mismatched data types
                new SequenceDataCreate
                {
                    ExternalId = ids[0].extId,
                    Columns = columns,
                    Rows = new[]
                    {
                        new SequenceRow
                        {
                            // String for double
                            RowNumber = 0, Values = new MultiValue[] { MultiValue.Create("string"), null, null }
                        },
                        new SequenceRow
                        {
                            // string for long
                            RowNumber = 1, Values = new MultiValue[] { null, MultiValue.Create("string"), null }
                        },
                        new SequenceRow
                        {
                            // number for string
                            RowNumber = 2, Values = new MultiValue[] { null, null, MultiValue.Create(123.4) }
                        },
                        new SequenceRow
                        {
                            // long for double (should be OK)
                            RowNumber = 3, Values = new MultiValue[] { MultiValue.Create(123), MultiValue.Create(123), null }
                        },
                        new SequenceRow
                        {
                            // double for long
                            RowNumber = 4, Values = new MultiValue[] { MultiValue.Create(123.4), MultiValue.Create(123.4), null }
                        }
                    }
                },
                // Missing columns
                new SequenceDataCreate
                {
                    ExternalId = ids[1].extId,
                    Columns = new [] { "col1", "col4", "col5" },
                    Rows = new[]
                    {
                        GetRow(0, 123.4, 123, "test"),
                        GetRow(1, 123.4, 123, "test")
                    }
                },
                // Missing extId
                new SequenceDataCreate
                {
                    ExternalId = "missing-sequence",
                    Columns = columns,
                    Rows = new[] { GetRow(0, 123.4, 123, "test") }
                },
                // Missing id
                new SequenceDataCreate
                {
                    Id = 123,
                    Columns = columns,
                    Rows = new[] { GetRow(0, 123.4, 123, "test") }
                },
                // All rows bad
                new SequenceDataCreate
                {
                    ExternalId = ids[2].extId,
                    Columns = columns,
                    Rows = new[]
                    {
                        new SequenceRow
                        {
                            // String for double
                            RowNumber = 0, Values = new MultiValue[] { MultiValue.Create("string"), null, null }
                        },
                        new SequenceRow
                        {
                            // string for long
                            RowNumber = 1, Values = new MultiValue[] { null, MultiValue.Create("string"), null }
                        }
                    }
                },
            };

            try
            {
                var result = await tester.Destination.InsertSequenceRowsAsync(creates, RetryMode.OnError, SanitationMode.None, tester.Source.Token);
                var errs = result.Errors.ToArray();
                Assert.Equal(3, errs.Length);

                Assert.Equal(ResourceType.Id, errs[0].Resource);
                Assert.Equal(ErrorType.ItemMissing, errs[0].Type);
                Assert.Equal(2, errs[0].Values.Count());
                Assert.Equal(2, errs[0].Skipped.Count());

                Assert.Equal(ResourceType.ColumnExternalId, errs[1].Resource);
                Assert.Equal(ErrorType.ItemMissing, errs[1].Type);
                Assert.Single(errs[1].Skipped);
                Assert.Equal(2, errs[1].Skipped.Sum(err => err.SkippedRows.Count()));

                Assert.Equal(ResourceType.SequenceRowValues, errs[2].Resource);
                Assert.Equal(ErrorType.MismatchedType, errs[2].Type);
                Assert.Equal(2, errs[2].Skipped.Count());
                Assert.Equal(5, errs[2].Skipped.Sum(err => err.SkippedRows.Count()));
                Assert.Single(errs[2].Values);
            }
            finally
            {
                await SafeDelete(ids.Select(id => id.extId).ToArray(), tester);
            }
        }
    }
}
