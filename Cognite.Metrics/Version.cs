using System.IO;
using System.Reflection;

namespace Cognite.Extractor.Metrics
{
    /// <summary>
    /// Utility class for reading version information from attributes.
    /// In order to use this, version information has to be added to the assembly.
    /// See the online documentation for an example.
    /// </summary>
    public static class Version
    {
        /// <summary>
        /// Get the AssemblyDescription set at compile time
        /// </summary>
        /// <param name="assembly">Assembly to get version from</param>
        /// <param name="def">Default value</param>
        /// <returns>Value of the description attribute</returns>
        public static string GetDescription(Assembly assembly, string def = "Unknown")
        {
            return assembly.GetCustomAttribute<AssemblyDescriptionAttribute>()?.Description ?? def;
        }

        /// <summary>
        /// Get the AssemblyInformationalVersion set at compile time
        /// </summary>
        /// <param name="assembly">Assembly to get version from</param>
        /// <param name="def">Default value</param>
        /// <returns>Value of the verison attribute or default</returns>
        public static string GetVersion(Assembly assembly, string def = "1.0.0")
        {
            return assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()
                ?.InformationalVersion ?? def;
        }
    }
}
