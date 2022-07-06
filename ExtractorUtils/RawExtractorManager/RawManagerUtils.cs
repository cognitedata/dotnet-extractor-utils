using System;
using Cognite.Extractor.Common;

namespace Cognite.Extractor.Utils
{
    internal class RawExtractorInstance : IExtractorInstance
    {
        internal RawExtractorInstance(
            int key, 
            DateTime timeStamp, 
            bool active)
        {
            Key = key;
            TimeStamp = timeStamp;
            Active = active;
        }
        public int Key { get; set; }
        public DateTime TimeStamp { get; set; }
        public bool Active { get; set; }  
    }

    internal class RawLogData
    {
        public RawLogData(DateTime timeStamp, bool active)
        {
            TimeStamp = timeStamp;
            Active = active;
        }
        public DateTime TimeStamp { get; }
        public bool Active { get; }
    }
}