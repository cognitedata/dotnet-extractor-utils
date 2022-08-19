using Cognite.Extractor.Testing;
using Cognite.Extractor.Utils.CommandLine;
using System.CommandLine;
using Xunit;
using Xunit.Abstractions;

namespace ExtractorUtils.Test.Unit
{
    internal class CliType
    {
        [CommandLineOption("Some string type", true, "-s", "-t")]
        public string StringType { get; set; }
        [CommandLineOption("Some int type", false, "-i")]
        public int IntType { get; set; }
        [CommandLineOption("Some flag type", false, "-b")]
        public bool Flag { get; set; }
        public bool IgnoredOption { get; set; }
    }


    public class CommandLineTest : ConsoleWrapper
    {
        public CommandLineTest(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void TestSimpleCliType()
        {
            var binder = new AttributeBinder<CliType>();
            var command = new RootCommand()
            {
                Description = "My description"
            };
            binder.AddOptionsToCommand(command);

            command.SetHandler<CliType>(result =>
            {
                Assert.Equal("stringvalue", result.StringType);
                Assert.Equal(123, result.IntType);
                Assert.True(result.Flag);
                Assert.False(result.IgnoredOption);
            }, binder);

            Assert.Equal(0, command.Invoke("--string-type stringvalue -b -i 123"));
        }
    }
}
