using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extractor.Common;
using Xunit;

namespace ExtractorUtils.Test.Unit
{
    public class ThresholdTest
    {
        FailureThresholdManager<string> _thresholdManager;
        IEnumerable<string> _failed;
        public ThresholdTest()
        {
            _thresholdManager = new FailureThresholdManager<string>(10.1, 10, (x) => { _failed = x; });
        }
        [Fact]
        public void TestThreshold()
        {
            _thresholdManager.Failed("a");
            Assert.Null(_failed);
            Assert.Equal(0, _thresholdManager.RemainingBudget);

            _thresholdManager.Failed("b");
            Assert.NotNull(_failed);
            Assert.Contains("a", _failed);
            Assert.Contains("b", _failed);
        }
        [Fact]
        public void TestChangeThresholdTrigger()
        {
            _thresholdManager.Failed("c");
            Assert.Null(_failed);
            Assert.Equal(0, _thresholdManager.RemainingBudget);

            _thresholdManager.UpdateBudget(20.1, 30);
            Assert.Equal(5, _thresholdManager.RemainingBudget);

            _thresholdManager.UpdateBudget(9, 10);
            Assert.NotNull(_failed);
            Assert.Contains("c", _failed);
        }
    }
}
