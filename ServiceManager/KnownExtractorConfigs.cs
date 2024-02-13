using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ServiceManager
{
    internal abstract class KnownExtractor
    {
        public abstract string WellKnownExePath { get; }
        public abstract string Name { get; }
        public abstract string RegistryName { get; }
        public abstract string ServicePrefix { get; }
        public abstract string ServiceCommand { get; }
        public abstract string WorkingDirFlag { get; }

        public string? FullExePath { get; set; }

        public override string ToString()
        {
            return Name;
        }
    }

    internal class OpcUaExtractor : KnownExtractor
    {
        public override string WellKnownExePath => "OpcUaExtractor\\bin\\OpcUaExtractor.exe";
        public override string Name => "Cognite OPC UA Extractor";
        public override string RegistryName => "OpcUaExtractor";
        public override string ServicePrefix => "opcuaext";
        public override string ServiceCommand => "-s";
        public override string WorkingDirFlag => "-w";
    }

    internal class OpcClassicExtractor : KnownExtractor
    {
        public override string WellKnownExePath => "OpcClassicExtractor\\bin\\OpcClassicExtractor.exe";
        public override string Name => "Cognite OPC Classic Extractor";
        public override string RegistryName => "OpcClassicExtractor";
        public override string ServicePrefix => "opcclassicext";
        public override string ServiceCommand => "-s";
        public override string WorkingDirFlag => "-w";
    }

    internal class PiExtractor : KnownExtractor
    {
        public override string WellKnownExePath => "PiExtractor\\bin\\PiExtractor.exe";
        public override string Name => "Cognite PI Extractor";
        public override string RegistryName => "PiExtractor";
        public override string ServicePrefix => "piextractor";
        public override string ServiceCommand => "-s";
        public override string WorkingDirFlag => "-w";
    }




    internal static class KnownExtractorConfigs
    {
        public static KnownExtractor[] KnownExtractors { get; } = new[]
        {
            (KnownExtractor)new PiExtractor(),
            new OpcUaExtractor(),
            new OpcClassicExtractor(),
        };
    }
}
