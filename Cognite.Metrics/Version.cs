using System.IO;
using System.Reflection;

namespace Cognite.Extractor.Metrics
{
    /// <summary>
    /// Utility class for reading version information from properties.
    /// In order to use this, version information has to be added to the assembly.
    /// See the online documentation for an example.
    /// </summary>
    public static class Version
    {
        /// <summary>
        /// Read a property from the calling assembly.
        /// </summary>
        /// <param name="property">Property to read</param>
        /// <param name="assembly">Assembly to use, default is the assembly that calls this method.</param>
        /// <returns>null or the value of the property in the assembly</returns>
        public static string Read(string property, Assembly assembly = null)
        {
            if (assembly == null)
            {
                assembly = Assembly.GetCallingAssembly();
            }
            using (var stream = assembly.GetManifestResourceStream($"{assembly.GetName().Name}.Properties.{property}"))
            {
                if (stream == null) return null;
                using (var reader = new StreamReader(stream))
                {
                    return reader.ReadToEnd().Trim();
                }
            }
        }

        /// <summary>
        /// Get status on the form [GitCommitHash] [GitCommitTime].
        /// </summary>
        /// <returns></returns>
        public static string Status()
        {
            string hash = Read("GitCommitHash", Assembly.GetCallingAssembly());
            string time = Read("GitCommitTime", Assembly.GetCallingAssembly());
            return $"{hash} {time}";
        }
        /// <summary>
        /// Get the version, just the GitCommitHash. This can be parsed further to produce a readable string.
        /// </summary>
        /// <returns></returns>
        public static string GetVersion()
        {
            return Read("GitCommitHash", Assembly.GetCallingAssembly());
        }
    }
}
