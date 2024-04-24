using System.ServiceProcess;

namespace ServiceManager
{
    internal class ServiceListElement
    {
        public ServiceController Service { get; }

        public ServiceListElement(ServiceController service)
        {
            Service = service;
        }

        public override string ToString()
        {
            return Service.DisplayName;
        }
    }
}
