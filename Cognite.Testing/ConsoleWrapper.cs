using System;
using System.IO;
using Xunit;

namespace Cognite.Extractor.Testing
{
    /// <summary>
    /// Base class for test classes that dump console output to the xunit test output.
    /// Requires disabling test parallelization. A better solution is to use TestUtils.AddTestLogger
    /// but this requires the code to use dependency injected loggers, and that they are
    /// instantiated after 
    /// </summary>
    public class ConsoleWrapper : IDisposable
    {
        private readonly ITestOutputHelper _output;
        private readonly TextWriter _originalOut;
        private readonly TextWriter _textWriter;
        private bool disposed;

        /// <summary>
        /// Constructor, pass the test output helper from the test class.
        /// </summary>
        /// <param name="output"></param>
        public ConsoleWrapper(ITestOutputHelper output)
        {
            _output = output;
            _originalOut = Console.Out;
            _textWriter = new StringWriter();
            Console.SetOut(_textWriter);
        }

        /// <inheritDoc />
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Internal dispose method, should be called if overridden in subclass.
        /// </summary>
        /// <param name="disposing">True if we are disposing disposables now</param>
        protected virtual void Dispose(bool disposing)
        {
            if (disposed) return;
            if (disposing)
            {
                Console.SetOut(_originalOut);
                // This can rarely randomly fail due to some obscure threading issue.
                // It's just cleanup of tests, so we can just retry.
                try
                {
                    _output.WriteLine(_textWriter.ToString());
                }
                catch (ArgumentOutOfRangeException)
                {
                    _output.WriteLine(_textWriter.ToString());
                }
                _textWriter.Dispose();
                disposed = true;
            }
        }
    }
}
