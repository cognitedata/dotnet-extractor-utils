using Xunit;
using Cognite.Utils;

namespace ExtractorUtils.Test
{
    public static class CommonUtilsTest {

        [Theory]
        [InlineData("", null)]
        [InlineData("  \n", null)]
        [InlineData(" foo  \n", "foo")]
        public static void TrimToNullTest(string input, string expected)
        {
            Assert.Equal(expected, input.TrimToNull());
        }
    }
}