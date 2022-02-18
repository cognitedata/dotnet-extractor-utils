using System;
using System.Collections.Generic;
using System.CommandLine;

namespace Cognite.Extractor.Utils.CommandLine
{
    /// <summary>
    /// Utility attribute for building flat command line interfaces using System.CommandLine
    /// </summary>
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false, Inherited = true)]
    public sealed class CommandLineOptionAttribute : Attribute
    {
        /// <summary>
        /// List of aliases.
        /// </summary>
        public IEnumerable<string> Aliases { get; }
        /// <summary>
        /// Description of command.
        /// </summary>
        public string Description { get; }
        /// <summary>
        /// True to include the name of the property itself as an alias, converted to snake-case.
        /// </summary>
        public bool IncludePropertyName { get; }
        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="description">Description of command.</param>
        /// <param name="includePropertyName">True to include the name of the property itself as an alias, converted to snake-case.
        /// </param>
        /// <param name="aliases">List of aliases.</param>
        public CommandLineOptionAttribute(
            string description = "",
            bool includePropertyName = true,
           params string[] aliases)
        {
            Description = description;
            Aliases = aliases;
            IncludePropertyName = includePropertyName;
        }
    }
}
