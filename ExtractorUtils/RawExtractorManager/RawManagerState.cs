using System;
using Cognite.Extractor.Common;

namespace Cognite.Extractor.Utils
{
    internal class RawExtractorInstance : IExtractorInstance
    {
        public int Index { get; set; }

        public DateTime TimeStamp { get; set; }

        public bool Active { get; set; }

        internal RawExtractorInstance(
            int index,
            DateTime timeStamp,
            bool active)
        {
            Index = index;
            TimeStamp = timeStamp;
            Active = active;
        }
    }

    internal class RawLogData
    {
        public DateTime TimeStamp { get; }

        public bool Active { get; }

        public RawLogData(DateTime timeStamp, bool active)
        {
            TimeStamp = timeStamp;
            Active = active;
        }
    }
}