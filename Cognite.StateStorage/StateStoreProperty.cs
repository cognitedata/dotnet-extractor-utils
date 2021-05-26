using System;
using System.Collections.Generic;
using System.Text;

namespace Cognite.Extractor.StateStorage
{
    /// <summary>
    /// Property to assign custom name in destination store.
    /// Default behavior transforms PascalCase into snake-case.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public sealed class StateStoreProperty : Attribute
    {
        /// <summary>
        /// Override name
        /// </summary>
        public string Name { get; }
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="name">Name to use</param>
        public StateStoreProperty(string name)
        {
            Name = name;
        }
    }
}
