using System;

namespace Cognite.Extractor.Configuration
{
    /// <summary>
    /// Exception produced by the configuration utils 
    /// </summary>
    public class ConfigurationException : Exception
    {
        /// <summary>
        /// Create a new configuration exception with the given <paramref name="message"/>
        /// </summary>
        /// <param name="message">Exception message</param>
        /// <returns></returns>
        public ConfigurationException(string message) : base(message)
        {
        }

        /// <summary>
        /// Create a new configuration exception with the given <paramref name="message"/>
        /// and containing the given <paramref name="innerException"/>
        /// </summary>
        /// <param name="message">Exception message</param>
        /// <param name="innerException">Inner exception</param>
        /// <returns></returns>
        public ConfigurationException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}