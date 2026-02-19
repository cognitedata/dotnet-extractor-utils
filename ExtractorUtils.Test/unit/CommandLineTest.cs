using Cognite.Extractor.Testing;
using Cognite.Extractor.Utils.CommandLine;
using System.CommandLine;
using System.Threading.Tasks;
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

    internal class SubCommandType : CliType
    {
        [CommandLineOption("Some other flag", false, "-o")]
        public bool OtherFlag { get; set; }
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

            command.SetHandler(result =>
            {
                Assert.Equal("stringvalue", result.StringType);
                Assert.Equal(123, result.IntType);
                Assert.True(result.Flag);
                Assert.False(result.IgnoredOption);
            }, binder);

            Assert.Equal(0, command.Invoke(new[] { "--string-type", "stringvalue", "-b", "-i", "123" }));
        }

        [Fact]
        public void TestMultiCommandCli()
        {
            var binder = new AttributeBinder<CliType>();
            var subBinder = new AttributeBinder<SubCommandType>();

            var command = new RootCommand()
            {
                Description = "My description"
            };
            binder.AddOptionsToCommand(command);
            command.SetHandler(result =>
            {
                Assert.Fail("Root command should not be invoked in this test.");
            }, binder);

            var subCommand = new Command("subcommand")
            {
                Description = "Subcommand description"
            };
            subBinder.AddOptionsToCommand(subCommand);
            subCommand.SetHandler(result =>
            {
                Assert.Equal("stringvalue", result.StringType);
                Assert.Equal(123, result.IntType);
                Assert.True(result.Flag);
                Assert.False(result.IgnoredOption);
                Assert.True(result.OtherFlag);
            }, subBinder);
            command.Add(subCommand);

            Assert.Equal(0, command.Invoke(new[] { "subcommand", "-o", "--string-type", "stringvalue", "-b", "-i", "123" }));
        }

        [Fact]
        public async Task TestAsyncCli()
        {
            var binder = new AttributeBinder<CliType>();

            var command = new RootCommand()
            {
                Description = "My description"
            };
            binder.AddOptionsToCommand(command);

            command.SetHandler(async result =>
            {
                await Task.Delay(10);
                Assert.Equal("stringvalue", result.StringType);
                Assert.Equal(123, result.IntType);
                Assert.True(result.Flag);
                Assert.False(result.IgnoredOption);
            }, binder);

            Assert.Equal(0, command.Invoke(new[] { "--string-type", "stringvalue", "-b", "-i", "123" }));
        }
    }
}
