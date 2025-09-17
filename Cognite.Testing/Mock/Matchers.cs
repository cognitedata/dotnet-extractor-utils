using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf.WellKnownTypes;
using Oryx;
using Xunit;

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
        public (int min, int max) ExpectedRequestCount { get; set; }
        /// <summary>
        /// Current request count for the matcher.
        /// </summary>
        public int RequestCount { get; set; }

        /// <summary>
        /// Force the matcher to return an error status code.
        /// </summary>
        public int? ForceErrorStatus { get; set; }

        /// <summary>
        /// Assert that this has been called the correct number of times.
        /// </summary>
        public void AssertMatches()
        {
            Assert.True(RequestCount >= ExpectedRequestCount.min && RequestCount <= ExpectedRequestCount.max,
                $"Expected request count for path '{Name}' to be between {ExpectedRequestCount.min} and {ExpectedRequestCount.max}, but was {RequestCount}.");
        }

        /// <summary>
        /// Assert that this has been called the correct number of times,
        /// then reset the request count.
        /// </summary>
        public void AssertAndReset()
        {
            AssertMatches();
            RequestCount = 0;
        }
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
            Func<RequestContext, CancellationToken, Task<HttpResponseMessage>> handler,
            int minRequests = 0,
            int maxRequests = int.MaxValue)
        {
            Name = $"{method} {pathRegex}";
            _handler = handler;
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

    /// <summary>
    /// Wrapper around a different matcher that makes it fail if a predicate is met.
    /// </summary>
    public class FailIfMatcher : RequestMatcher
    {
        private readonly RequestMatcher _inner;
        private readonly object? _error;
        private readonly HttpStatusCode _statusCode;

        /// <summary>
        /// Get the inner request matcher.
        /// </summary>
        public RequestMatcher Inner => _inner;

        private readonly Func<RequestMatcher, bool> _failIfTrue;

        /// <summary>
        /// Constructor
        /// </summary>
        public FailIfMatcher(RequestMatcher inner, Func<RequestMatcher, bool> failIfTrue, HttpStatusCode statusCode, object? error = null)
        {
            _inner = inner ?? throw new ArgumentNullException(nameof(inner));
            _failIfTrue = failIfTrue ?? throw new ArgumentNullException(nameof(failIfTrue));
            _error = error;
            _statusCode = statusCode;
            ExpectedRequestCount = inner.ExpectedRequestCount;
        }

        /// <inheritdoc />
        public override string Name => _inner.Name;

        /// <inheritdoc />
        public override Task<HttpResponseMessage> Handle(RequestContext context, CancellationToken token)
        {
            if (context == null) throw new ArgumentNullException(nameof(context));
            if (_failIfTrue(this))
            {
                return Task.FromResult(
                    context.CreateJsonResponse(_error ?? new CogniteErrorWrapper(new CogniteError((int)_statusCode, "Something went wrong")),
                    _error?.GetType() ?? typeof(CogniteErrorWrapper),
                    _statusCode)
                );
            }
            return _inner.Handle(context, token);
        }

        /// <inheritdoc />
        public override bool Matches(HttpMethod method, string path)
        {
            return _inner.Matches(method, path);
        }
    }

    /// <summary>
    /// Matcher for failing N times before succeeding.
    /// </summary>
    public class FailNTimesMatcher : FailIfMatcher
    {
        /// <summary>
        /// Constructor
        /// </summary>
        public FailNTimesMatcher(int count, RequestMatcher inner, HttpStatusCode statusCode, object? error = null)
            : base(inner, (matcher) => matcher.RequestCount <= count, statusCode, error)
        {
            if (count < 1) throw new ArgumentOutOfRangeException(nameof(count), "Count must be at least 1");
            ExpectedRequestCount = (
                inner.ExpectedRequestCount.min + count,
                inner.ExpectedRequestCount.max + count
            );
        }
    }
}