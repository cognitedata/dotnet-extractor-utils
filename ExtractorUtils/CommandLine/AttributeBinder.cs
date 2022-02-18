using Cognite.Extractor.StateStorage;
using System;
using System.Collections.Generic;
using System.CommandLine;
using System.CommandLine.Binding;
using System.Linq;

namespace Cognite.Extractor.Utils.CommandLine
{
    /// <summary>
    /// Binder class for objects built with CommandLineOptionAttribute.
    /// </summary>
    /// <typeparam name="T">Type to create. Must have a parameterless constructor.</typeparam>
    public class AttributeBinder<T> : BinderBase<T>
    {
        /// <summary>
        /// Built options, options here can be modified.
        /// They are organized by property name.
        /// You can also manually add options to the binder here, if you need custom logic.
        /// </summary>
        public Dictionary<string, Option> Options { get; }

        /// <summary>
        /// Constructor.
        /// </summary>
        public AttributeBinder()
        {
            Options = new Dictionary<string, Option>();
            BuildCommandOptions();
        }

        /// <summary>
        /// Add build attributes to the given command, so that this binder can be used with it.
        /// </summary>
        /// <param name="command">Command to add to</param>
        public void AddOptionsToCommand(Command command)
        {
            if (command == null) throw new ArgumentNullException(nameof(Command));

            foreach (var kvp in Options)
            {
                command.AddOption(kvp.Value);
            }
        }

        private void BuildCommandOptions()
        {
            var type = typeof(T);

            var props = type.GetProperties(System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.Public);
            
            foreach (var prop in props)
            {
                if (!(prop.GetCustomAttributes(typeof(CommandLineOptionAttribute), true).FirstOrDefault() is CommandLineOptionAttribute attr)) continue;

                var aliases = new List<string>();
                if (attr.IncludePropertyName)
                {
                    aliases.Add($"--{prop.Name.ToSnakeCase()}");
                }
                if (attr.Aliases != null)
                {
                    aliases.AddRange(attr.Aliases);
                }
                Option option;
                var optType = typeof(Option<>).MakeGenericType(prop.PropertyType);
                option = (Option)Activator.CreateInstance(optType, aliases.ToArray(), attr.Description);

                Options[prop.Name] = option;
            }
        }

        /// <summary>
        /// Create an instance of <typeparamref name="T"/>, and bind to it using options constructed from
        /// CommandLineOptionAttributes.
        /// </summary>
        /// <param name="bindingContext">Context</param>
        /// <returns>An instance of <typeparamref name="T"/> with fields filled from command line.</returns>
        protected override T GetBoundValue(BindingContext bindingContext)
        {
            if (bindingContext == null) throw new ArgumentNullException(nameof(bindingContext));

            var type = typeof(T);
            var result = Activator.CreateInstance(type);
            var props = type.GetProperties(System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.Public);

            foreach (var prop in props)
            {
                if (!prop.CanWrite) continue;
                if (!Options.TryGetValue(prop.Name, out var option)) continue;

                if (option.GetType() == typeof(Option<>).MakeGenericType(typeof(bool)))
                {
                    prop.SetValue(result, bindingContext.ParseResult.GetValueForOption((Option<bool>)option));
                }
                else
                {
                    var res = bindingContext.ParseResult.GetValueForOption(option);
                    prop.SetValue(result, res);
                }
                
            }

            return (T)result;
        }
    }
}
