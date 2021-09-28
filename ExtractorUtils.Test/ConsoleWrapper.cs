using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit.Abstractions;

namespace ExtractorUtils.Test
{
    public class ConsoleWrapper : IDisposable
    {
        private readonly ITestOutputHelper _output;
        private readonly TextWriter _originalOut;
        private readonly TextWriter _textWriter;
        private bool disposed;

        public ConsoleWrapper(ITestOutputHelper output)
        {
            _output = output;
            _originalOut = Console.Out;
            _textWriter = new StringWriter();
            Console.SetOut(_textWriter);
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

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
            }
            disposed = true;
        }
    }
}
