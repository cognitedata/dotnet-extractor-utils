using System.Collections.Generic;
using System.Linq;
using Cognite.Extractor.Configuration;
using Cognite.Extractor.Utils.Unstable.Configuration;
using Xunit;

namespace ExtractorUtils.Test.Unit.Unstable
{
    public class CdfWriterTests
    {
        private static CdfMappingConfig Mapping()
            => new CdfMappingConfig
            {
                IntegrationId = "test",
                ExtractorType = "test",
                ExtractorVersion = "1.0",
                Mappings = new List<CdfMapping>
                {
                    new CdfMapping { ExternalId = "m1", Name = "M1", Type = "space_routing" }
                },
                Bindings = new List<CdfMappingBinding>
                {
                    new CdfMappingBinding { MappingId = "m1", Destinations = new List<string> { "/d" } }
                }
            };

        [Fact]
        public void TestParseConfig()
        {
            var config = ConfigurationUtils.ReadString<BaseCogniteConfig>(@"
cdf-writer:
    enabled: true
cdf-mapping:
    integration-id: test
    extractor-type: test
    extractor-version: 1.0
    mappings:
      - external-id: m1
        name: M1
        type: space_routing
    bindings:
      - mapping-id: m1
        destinations: [/d]
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

            // Enabled without a mapping section
            var error = CdfWriterConfig.Validate(enabled, null);
            Assert.NotNull(error);
            Assert.Contains("requires cognite.cdf-mapping", error, System.StringComparison.Ordinal);

            // With mapping
            Assert.Null(CdfWriterConfig.Validate(enabled, Mapping()));
        }

        [Fact]
        public void TestTaskNamesAreFixed()
        {
            Assert.Equal("datapoints", CdfWriterConfig.DatapointsTaskName);
            Assert.Equal("timeseries", CdfWriterConfig.TimeseriesTaskName);
        }
    }
}
