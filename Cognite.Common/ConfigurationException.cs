using System;

namespace Cognite.Extractor.Common
{
    /// <summary>
    /// Exception produced by the configuration utils 
    /// </summary>
    public class ConfigurationException : Exception
    {
        /// <summary>
        /// Create a new configuration exception with the default error message
        /// </summary>
        public ConfigurationException() : base("Configuration error")
        {
        }

        /// <summary>
        /// Create a new configuration exception with the given <paramref name="message"/>
        /// </summary>
        /// <param name="message">Exception message</param>
        public ConfigurationException(string message) : base(message)
        {
        }

        /// <summary>
        /// Create a new configuration exception with the given <paramref name="message"/>
        /// and containing the given <paramref name="innerException"/>
        /// </summary>
        /// <param name="message">Exception message</param>
        /// <param name="innerException">Inner exception</param>
        public ConfigurationException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}