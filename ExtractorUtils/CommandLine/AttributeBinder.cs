using Cognite.Extractor.StateStorage;
using System;
using System.Collections.Generic;
using System.CommandLine;
using System.CommandLine.Binding;
using System.Linq;
using System.Threading.Tasks;

namespace Cognite.Extractor.Utils.CommandLine
{
    /// <summary>
    /// Binder class for objects built with CommandLineOptionAttribute.
    /// </summary>
    /// <typeparam name="T">Type to create. Must have a parameterless constructor.</typeparam>
    public class AttributeBinder<T>
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
            if (command == null) throw new ArgumentNullException(nameof(command));

            foreach (var kvp in Options)
            {
                command.Add(kvp.Value);
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
                option = (Option)Activator.CreateInstance(optType, aliases.ToArray())!;
                option.Description = attr.Description;

                Options[prop.Name] = option;
            }
        }

        /// <summary>
        /// Create an instance of <typeparamref name="T"/>, and bind to it using options constructed from
        /// CommandLineOptionAttributes.
        /// </summary>
        /// <param name="result">Parse result</param>
        /// <returns>An instance of <typeparamref name="T"/> with fields filled from command line.</returns>
        public T GetBoundValue(ParseResult result)
        {
            if (result == null) throw new ArgumentNullException(nameof(result));

            var type = typeof(T);
            var instance = Activator.CreateInstance(type);
            var props = type.GetProperties(System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.Public);

            foreach (var prop in props)
            {
                if (!prop.CanWrite) continue;
                if (!Options.TryGetValue(prop.Name, out var option)) continue;

                var getter = typeof(ParseResult).GetMethods().First(m =>
                    m.Name == nameof(ParseResult.GetValue)
                    && m.IsGenericMethod
                    && m.GetParameters()[0].ParameterType.IsGenericType
                    && m.GetParameters()[0].ParameterType.GetGenericTypeDefinition() == typeof(Option<>)
                ).MakeGenericMethod(prop.PropertyType);
                prop.SetValue(instance, getter.Invoke(result, new object[] { option }));
            }

            return (T)instance!;
        }
    }

    /// <summary>
    /// Extensions for Command, to make our command line interface backwards compatible.
    /// </summary>
    public static class CommandExtensions
    {
        /// <summary>
        /// Set a typed handler for the command.
        /// </summary>
        /// <typeparam name="T">The type of the bound value.</typeparam>
        /// <param name="command">Command to set the handler for.</param>
        /// <param name="action">The action to execute when the command is invoked.</param>
        /// <param name="binder">The attribute binder to use for binding command line options.</param>
        /// <exception cref="ArgumentNullException"></exception>
        public static void SetHandler<T>(this Command command, Action<T> action, AttributeBinder<T> binder)
        {
            if (command == null) throw new ArgumentNullException(nameof(command));
            if (action == null) throw new ArgumentNullException(nameof(action));
            if (binder == null) throw new ArgumentNullException(nameof(binder));

            command.SetAction(result =>
            {
                var boundValue = binder.GetBoundValue(result);
                action(boundValue);
            });
        }

        /// <summary>
        /// Set a typed async handler for the command.
        /// </summary>
        /// <typeparam name="T">The type of the bound value.</typeparam>
        /// <param name="command">Command to set the handler for.</param>
        /// <param name="action">The action to execute when the command is invoked.</param>
        /// <param name="binder">The attribute binder to use for binding command line options.</param>
        /// <exception cref="ArgumentNullException"></exception>
        public static void SetHandler<T>(this Command command, Func<T, Task> action, AttributeBinder<T> binder)
        {
            if (command == null) throw new ArgumentNullException(nameof(command));
            if (action == null) throw new ArgumentNullException(nameof(action));
            if (binder == null) throw new ArgumentNullException(nameof(binder));

            command.SetAction(async result =>
            {
                var boundValue = binder.GetBoundValue(result);
                await action(boundValue).ConfigureAwait(false);
            });
        }


        /// <summary>
        /// Invoke the command.
        /// </summary>
        /// <param name="command">Command to invoke.</param>
        /// <param name="args">Raw command line arguments</param>
        /// <returns>The command's exit code.</returns>
        /// <exception cref="ArgumentNullException"></exception>
        public static int Invoke(this Command command, string[] args)
        {
            if (command == null) throw new ArgumentNullException(nameof(command));
            if (args == null) throw new ArgumentNullException(nameof(args));

            return command.Parse(args).Invoke();
        }

        /// <summary>
        /// Invoke the command asynchronously.
        /// </summary>
        /// <param name="command">Command to invoke.</param>
        /// <param name="args">Raw command line arguments</param>
        /// <returns>A task representing the asynchronous operation, with the command's exit code as the result.</returns>
        /// <exception cref="ArgumentNullException"></exception>
        public static async Task<int> InvokeAsync(this Command command, string[] args)
        {
            if (command == null) throw new ArgumentNullException(nameof(command));
            if (args == null) throw new ArgumentNullException(nameof(args));

            return await command.Parse(args).InvokeAsync().ConfigureAwait(false);
        }

    }
}
