using Xunit;
using Cognite.Extractor.Common;
using System.Linq;
using System;

namespace ExtractorUtils.Test.Unit
{
    public static class CommonUtilsTest
    {

        [Theory]
        [InlineData("", null)]
        [InlineData("  \n", null)]
        [InlineData(" foo  \n", "foo")]
        public static void TrimToNullTest(string input, string expected)
        {
            Assert.Equal(expected, input.TrimToNull());
        }
        [Theory]
        [InlineData(0, 100, 10000, 1)]
        [InlineData(10, 100, 1001, 10)]
        [InlineData(100, 100, 101, 100)]
        [InlineData(100, 1, 10000, 1)]
        [InlineData(100, 10, 1001, 10)]
        [InlineData(1000, 1000, 11, 1000)]
        [InlineData(1000, 10, 1001, 10)]
        public static void GroupByGranularityTest(int granms, int chunkSize, int expectedFirst, int expectedSecond)
        {
            var now = new DateTime(2020, 01, 01, 01, 00, 05, 505);
            var items = Enumerable.Range(0, 10000)
                .Select(ts => now.AddMilliseconds(ts))
                .ToList();

            var chunks = items.GroupByTimeGranularity(TimeSpan.FromMilliseconds(granms), dt => dt, chunkSize);

            Assert.Equal(expectedFirst, chunks.Count());
            Assert.True(chunks.All(chunk => chunk.Count() <= expectedSecond));
        }
        [Fact]
        public static void DistinctByTest()
        {
            var items = Enumerable.Range(0, 10000);

            var result = items.DistinctBy(item => item % 10);
            Assert.Equal(10, result.Count());
        }
        [Fact]
        public static void MinMaxTest()
        {
            var now = new DateTime(2020, 01, 01);
            var items = Enumerable.Range(0, 10000)
                .Select(ts => now.AddMilliseconds(ts))
                .ToList();

            var (min, max) = items.MinMax(item => item);
            Assert.Equal(now, min);
            Assert.Equal(now.AddMilliseconds(9999), max);
        }
    }
}