using System.Collections.Generic;
using System.Linq;
using Cognite.Extractor.Configuration;
using Cognite.Extractor.Utils.Unstable.Configuration;
using Xunit;

namespace ExtractorUtils.Test.Unit.Unstable
{
    public class CdfWriterTests
    {
        private static List<CdfMappingItem> Mapping(params string[] tasks)
            => tasks.Select(t => new CdfMappingItem { TaskName = t, Type = "space_routing" }).ToList();

        [Fact]
        public void TestParseConfig()
        {
            var config = ConfigurationUtils.ReadString<BaseCogniteConfig>(@"
cdf-writer:
    enabled: true
cdf-mapping:
    - task-name: datapoints
      type: space_routing
    - task-name: timeseries
      type: space_routing
");
            Assert.NotNull(config.CdfWriter);
            Assert.True(config.CdfWriter!.Enabled);
            Assert.Null(CdfWriterConfig.Validate(config.CdfWriter, config.CdfMapping));
        }

        [Fact]
        public void TestParseConfigDisabledByDefault()
        {
            var config = ConfigurationUtils.ReadString<BaseCogniteConfig>("nan-replacement: 0");
            Assert.Null(config.CdfWriter);
        }

        [Fact]
        public void TestValidate()
        {
            var enabled = new CdfWriterConfig { Enabled = true };

            Assert.Null(CdfWriterConfig.Validate(null, null));
            Assert.Null(CdfWriterConfig.Validate(new CdfWriterConfig { Enabled = false }, null));

            // Enabled without a mapping section, both tasks are missing.
            Assert.Equal("cognite.cdf-writer requires a cognite.cdf-mapping item with task-name \"datapoints\"",
                CdfWriterConfig.Validate(enabled, null));
            Assert.Equal("cognite.cdf-writer requires a cognite.cdf-mapping item with task-name \"datapoints\"",
                CdfWriterConfig.Validate(enabled, Mapping("other")));
            Assert.Equal("cognite.cdf-writer requires a cognite.cdf-mapping item with task-name \"timeseries\"",
                CdfWriterConfig.Validate(enabled, Mapping("datapoints")));
            Assert.Null(CdfWriterConfig.Validate(enabled, Mapping("datapoints", "timeseries")));
        }

        [Fact]
        public void TestTaskNamesAreFixed()
        {
            Assert.Equal("datapoints", CdfWriterConfig.DatapointsTaskName);
            Assert.Equal("timeseries", CdfWriterConfig.TimeseriesTaskName);
        }
    }
}
