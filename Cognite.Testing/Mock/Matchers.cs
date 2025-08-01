using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf.WellKnownTypes;

namespace Cognite.Extractor.Testing.Mock
{
    /// <summary>
    /// Matcher for a request sent to the CDF mocker.
    /// </summary>
    public abstract class RequestMatcher
    {
        /// <summary>
        /// Returns true if the matcher matches the given request.
        /// </summary>
        /// <param name="method">HTTP method to match against.</param>
        /// <param name="path">Path to match against.</param>
        /// <returns>true if the request is a match</returns>
        public abstract bool Matches(HttpMethod method, string path);
        /// <summary>
        /// Name of the matcher, used for error messages and debugging.
        /// </summary>
        public abstract string Name { get; }
        /// <summary>
        /// Handles the request and returns a response.
        /// </summary>
        /// <param name="context">Request context object.</param>
        /// <param name="token">Optional cancellation token.</param>
        /// <returns>A task representing the asynchronous operation, with a response message.</returns>
        public abstract Task<HttpResponseMessage> Handle(RequestContext context, CancellationToken token);

        /// <summary>
        /// Expected request count range for the matcher.
        /// </summary>
        public (int min, int max) ExpectedRequestCount { get; protected set; }
        /// <summary>
        /// Current request count for the matcher.
        /// </summary>
        public int RequestCount { get; set; }

        /// <summary>
        /// Force the matcher to return an error status code.
        /// </summary>
        public int? ForceErrorStatus { get; set; }
    }

    /// <summary>
    /// Simple matcher that matches requests based on a regular expression for the path and a specific HTTP method.
    /// </summary>
    public class SimpleMatcher : RequestMatcher
    {
        private readonly Func<RequestContext, CancellationToken, Task<HttpResponseMessage>> _handler;

        /// <inheritdoc />
        public override string Name { get; }

        private readonly Regex _pathRegex;
        private readonly string _method;

        /// <summary>
        /// Constructor for a simple matcher that handles requests with a specified handler.
        /// </summary>
        /// <param name="handler">Message handler</param>
        /// <param name="method">HTTP method to match, e.g. "GET", "POST"</param>
        /// <param name="pathRegex">Regular expression to match the request path</param>
        /// <param name="minRequests">Minimum number of requests expected</param>
        /// <param name="maxRequests">Maximum number of requests expected</param>
        public SimpleMatcher(
            string method,
            string pathRegex,
            Func<RequestContext, CancellationToken, HttpResponseMessage> handler,
            int minRequests = 0,
            int maxRequests = int.MaxValue)
        {
            Name = $"{method} {pathRegex}";
            _handler = (ctx, token) => Task.FromResult(handler(ctx, token));
            ExpectedRequestCount = (minRequests, maxRequests);
            _pathRegex = new Regex(pathRegex, RegexOptions.Compiled | RegexOptions.IgnoreCase);
            _method = method?.ToLower() ?? throw new ArgumentNullException(nameof(method));
        }

        /// <inheritdoc />
        public override bool Matches(HttpMethod method, string path)
        {
            if (method == null) throw new ArgumentNullException(nameof(method));
            if (path == null) throw new ArgumentNullException(nameof(path));
            return method.Method?.ToLower() == _method && _pathRegex.IsMatch(path);
        }

        /// <inheritdoc />
        public override Task<HttpResponseMessage> Handle(RequestContext context, CancellationToken token)
        {
            if (context == null) throw new ArgumentNullException(nameof(context));
            return _handler(context, token);
        }
    }
}