using Cognite.Extensions.Alpha;
using Xunit;

namespace ExtractorUtils.Test.Unit
{
    public class StatusCodeTests
    {
        [Fact]
        public void TestFromCategory()
        {
            // Just check that this doesn't fail.
            Assert.Equal("Good", StatusCode.FromCategory(StatusCodeCategory.Good).ToString());
            Assert.Equal("BadBrowseDirectionInvalid", StatusCode.FromCategory(StatusCodeCategory.BadBrowseDirectionInvalid).ToString());
            Assert.Equal("UncertainDataSubNormal", StatusCode.FromCategory(StatusCodeCategory.UncertainDataSubNormal).ToString());
        }

        [Fact]
        public void TestParse()
        {
            Assert.Equal("Good, StructureChanged, Calculated", StatusCode.Parse("Good, StructureChanged, Calculated").ToString());
            Assert.Equal("UncertainSensorCalibration, Overflow, ExtraData",
                StatusCode.Parse("UncertainSensorCalibration, Overflow, ExtraData").ToString());

            Assert.Equal("Good", StatusCode.Create(0).ToString());

            Assert.Throws<InvalidStatusCodeException>(() => StatusCode.Create(12345));
            Assert.Throws<InvalidStatusCodeException>(() => StatusCode.Parse("Bad, Whoop"));
        }

    }
}