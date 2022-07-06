using System;
using System.Threading.Tasks;
using System.Collections.Generic;

namespace Cognite.Extractor.Common
{
    ///
    public interface IExtractorManager 
    {
        ///
        public Task WaitToBecomeActive(); 
    }
    ///
    public interface IExtractorInstance
    {
        ///
        public int Key { get; set; }
        ///
        public DateTime TimeStamp { get; set; }
        ///
        public bool Active { get; set; }  
    }
    ///
    public class ExtractorState 
    {
        ///
        public ExtractorState(bool initialStatus = false)
        {
            CurrentState = new List<IExtractorInstance>();
            UpdatedStatus = initialStatus;
        }
        ///
        public List<IExtractorInstance> CurrentState { get; set; }
        ///
        public bool UpdatedStatus { get; set; }
    }
}