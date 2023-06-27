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
        FailureThresholdManager<string, int> _thresholdManager;
        IReadOnlyDictionary<string, int> _failed;
        public ThresholdTest()
        {
            _thresholdManager = new FailureThresholdManager<string, int>(10.1, 10, (x) => { _failed = x; });
        }
        [Fact]
        public void TestThreshold()
        {
            _thresholdManager.Failed("a", 1);
            Assert.Null(_failed);
            Assert.Equal(0, _thresholdManager.RemainingBudget);

            _thresholdManager.Failed("b", 2);
            Assert.NotNull(_failed);
            Assert.Contains("a", _failed);
            Assert.Equal(1, _failed["a"]);
            Assert.Contains("b", _failed);
            Assert.Equal(2, _failed["b"]);
        }
        [Fact]
        public void TestChangeThresholdTrigger()
        {
            _thresholdManager.Failed("c", 3);
            Assert.Null(_failed);
            Assert.Equal(0, _thresholdManager.RemainingBudget);

            _thresholdManager.UpdateBudget(20.1, 30);
            Assert.Equal(5, _thresholdManager.RemainingBudget);

            _thresholdManager.UpdateBudget(9, 10);
            Assert.NotNull(_failed);
            Assert.Contains("c", _failed);
            Assert.Equal(3, _failed["c"]);
        }
    }
}
