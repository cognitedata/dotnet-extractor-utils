using System;
using Cognite.Extractor.Common;

namespace Cognite.Extractor.Utils
{
    internal class RawExtractorInstance : IExtractorInstance
    {
        public int Key { get; set; }
        public DateTime TimeStamp { get; set; }
        public bool Active { get; set; }
        internal RawExtractorInstance(
            int key,
            DateTime timeStamp,
            bool active)
        {
            Key = key;
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