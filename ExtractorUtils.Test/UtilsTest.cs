using System.Reflection;
using Xunit;

namespace ExtractorUtils.Test {
    public class UtilsTest {

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