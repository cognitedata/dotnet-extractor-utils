using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using Cognite.Extractor.Utils;
using CogniteSdk;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Http;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Xunit;

namespace Cognite.Extractor.Testing.Mock
{
    /// <summary>
    /// Request context object, containing convenience methods for handling requests and responses.
    /// </summary>
    public class RequestContext
    {
        /// <summary>
        /// Raw HTTP request message.
        /// </summary>
        public HttpRequestMessage RawRequest { get; }

        // Note that the matcher is initialized after the RequestContext is created...

        /// <summary>
        /// Matcher that handled the request.
        /// </summary>
        public RequestMatcher Matcher { get; set; } = null!;

        /// <summary>
        /// Constructor
        /// </summary>
        public RequestContext(HttpRequestMessage rawRequest)
        {
            RawRequest = rawRequest;
        }

        /// <summary>
        /// Reads the JSON body of the request and deserializes it into the specified type.
        /// </summary>
        public async Task<T?> ReadJsonBody<T>() where T : class
        {
            if (RawRequest.Content == null) return null;
            var content = await RawRequest.Content.ReadAsStringAsync().ConfigureAwait(false);
            return System.Text.Json.JsonSerializer.Deserialize<T>(content, Oryx.Cognite.Common.jsonOptions);
        }

        /// <summary>
        /// Creates a JSON response with the specified value and status code.
        /// </summary>
        public HttpResponseMessage CreateJsonResponse<T>(T value, HttpStatusCode statusCode = HttpStatusCode.OK) where T : class
        {
            var response = new HttpResponseMessage(statusCode)
            {
                Content = new StringContent(System.Text.Json.JsonSerializer.Serialize(value, Oryx.Cognite.Common.jsonOptions))
            };
            response.Content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
            return response;
        }

        /// <summary>
        /// Creates a JSON response with the specified value and status code.
        /// </summary>
        public HttpResponseMessage CreateJsonResponse(object value, Type type, HttpStatusCode statusCode = HttpStatusCode.OK)
        {
            var response = new HttpResponseMessage(statusCode)
            {
                Content = new StringContent(System.Text.Json.JsonSerializer.Serialize(value, type, Oryx.Cognite.Common.jsonOptions)!)
            };
            response.Content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
            return response;
        }

        /// <summary>
        /// Creates a JSON error response with the specified error details and status code.
        /// </summary>
        public HttpResponseMessage CreateError(CogniteError error, HttpStatusCode statusCode = HttpStatusCode.BadRequest)
        {
            if (error == null) throw new ArgumentNullException(nameof(error));
            return CreateJsonResponse(new CogniteErrorWrapper(error), statusCode);
        }


        /// <summary>
        /// Creates a JSON error response with the specified status code and optional message.
        /// </summary>
        public HttpResponseMessage CreateError(HttpStatusCode statusCode, string? message = null)
        {
            var error = new CogniteError((int)statusCode, message);
            return CreateJsonResponse(new CogniteErrorWrapper(error), statusCode);
        }

        /// <summary>
        /// Parses the query string of the request URI into a dictionary.
        /// </summary>
        public Dictionary<string, string> ParseQuery()
        {
            var query = HttpUtility.ParseQueryString(RawRequest.RequestUri.Query);
            var result = new Dictionary<string, string>();
            foreach (var key in query.AllKeys)
            {
                result[key] = query[key];
            }
            return result;
        }
    }

    /// <summary>
    /// General HTTP mock server specialized for CDF.
    /// </summary>
    public class CdfMock : HttpMessageHandler
    {
        private SemaphoreSlim _semaphore = new SemaphoreSlim(1, 1);
        private List<RequestMatcher> _pathMatchers = new List<RequestMatcher>();

        /// <summary>
        /// Reject all messages sent to the mock server with a 500 error.
        /// This is useful for testing error handling in the client code.
        /// </summary>
        public bool RejectAllMessages { get; set; }
        /// <summary>
        /// Total number of requests received by the mock server.
        /// This is incremented for each request, regardless of whether it matches a path matcher or not.
        /// </summary>
        public int TotalRequestCount { get; private set; }

        private ILogger _logger;
        private bool _assertCount;

        /// <summary>
        /// Creates a new instance of the CdfMock class.
        /// </summary>
        public CdfMock(ILogger logger, bool assertCount = true)
        {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _assertCount = assertCount;
        }

        /// <summary>
        /// Add CdfMock as a singleton service to the service collection, and add it
        /// as the default IHttpClientFactory.
        /// </summary>
        /// <param name="services"></param>
        /// <exception cref="ArgumentNullException"></exception>
        public static void RegisterHttpClient(IServiceCollection services)
        {
            if (services == null) throw new ArgumentNullException(nameof(services));
            services.AddSingleton(provider => new CdfMock(provider.GetRequiredService<ILogger<CdfMock>>()));
            ConfigureHttpHandler(DestinationUtils.AUTH_CLIENT_NAME, services);
            // The service collection uses a crazy type name helper to get the name of the client type
            // but it isn't public. Fortunately for simple types it's just the type name.
            ConfigureHttpHandler(typeof(Client.Builder).Name, services);
        }

        private static void ConfigureHttpHandler(string name, IServiceCollection services)
        {
            // We can't just use `services.Configure`, since in that case we won't have access
            // to the service provider to get the CdfMock instance.
            // This is just the content of `services.Configure`, with the generic options type
            // replaced with `HttpClientFactoryOptions`.
            services.AddOptions();
            services.AddSingleton<IConfigureOptions<HttpClientFactoryOptions>>(provider =>
            {
                var mock = provider.GetRequiredService<CdfMock>();
                return new ConfigureNamedOptions<HttpClientFactoryOptions>(name, options =>
                {
                    options.HttpMessageHandlerBuilderActions.Add(b => b.PrimaryHandler = new HttpMessageHandlerStub(mock.MessageHandler));
                });
            });
        }

        private async Task<HttpResponseMessage> MessageHandler(HttpRequestMessage req, CancellationToken token)
        {
            await _semaphore.WaitAsync(token).ConfigureAwait(false);
            TotalRequestCount++;

            _logger.LogDebug("Received request: {Method} {Uri}", req.Method, req.RequestUri);

            try
            {
                var ctx = new RequestContext(req);

                if (RejectAllMessages)
                {
                    return ctx.CreateError(HttpStatusCode.ServiceUnavailable, "Mock server is rejecting all messages.");
                }

                foreach (var matcher in _pathMatchers)
                {
                    if (matcher.Matches(req.Method, req.RequestUri!.AbsolutePath))
                    {
                        ctx.Matcher = matcher;
                        matcher.RequestCount++;
                        if (matcher.ForceErrorStatus.HasValue)
                        {
                            return ctx.CreateError((HttpStatusCode)matcher.ForceErrorStatus.Value, "Forced error response.");
                        }
                        return await matcher.Handle(ctx, token).ConfigureAwait(false);
                    }
                }

                return ctx.CreateError(HttpStatusCode.NotFound, "No matching path found for request.");
            }
            finally
            {
                _semaphore.Release();
            }
        }

        /// <summary>
        /// Assert that all expected requests were made, and clear all matchers.
        /// </summary>
        public void AssertAndClear()
        {
            foreach (var matcher in _pathMatchers)
            {
                matcher.AssertAndReset();
            }
            TotalRequestCount = 0;
            RejectAllMessages = false;
            _pathMatchers.Clear();
        }

        private bool IsInExceptionUnwind()
        {
            // Hack to avoid throwing if we're currently unwinding due to another exception.
            // There's no great way to do this in .NET.
#if NET8_0_OR_GREATER
            return Marshal.GetExceptionPointers() != IntPtr.Zero;
#else
#pragma warning disable CS0618
            return Marshal.GetExceptionCode() != 0;
#pragma warning restore CS0618
#endif
        }

        /// <summary>
        /// Dispose the CdfMock instance and verify that all expected requests were made.
        /// </summary>
        protected override void Dispose(bool disposing)
        {
            if (!disposing) return;
            _semaphore.Dispose();
            if (_assertCount && !IsInExceptionUnwind())
            {
                foreach (var matcher in _pathMatchers)
                {
                    matcher.AssertMatches();
                }
            }

        }

        /// <inheritdoc />
        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            if (request == null) throw new ArgumentNullException(nameof(request));
            return await MessageHandler(request, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Add a new request matcher to the mock server.
        /// </summary>
        /// <param name="matcher">Matcher to add</param>
        public T AddMatcher<T>(T matcher) where T : RequestMatcher
        {
            if (matcher == null) throw new ArgumentNullException(nameof(matcher));
            _pathMatchers.Add(matcher);
            _logger.LogDebug("Added matcher: {Name}", matcher.Name);
            return matcher;
        }

        /// <summary>
        /// Get the first matcher for the specified HTTP method and path.
        /// If no matcher is found, returns null.
        /// </summary>
        /// <param name="method">Http method to test against</param>
        /// <param name="path">Path to test against</param>
        /// <returns>Request matcher if found</returns>
        public RequestMatcher? GetMatcher(HttpMethod method, string path)
        {
            return _pathMatchers.FirstOrDefault(m => m.Matches(method, path));
        }

        /// <summary>
        /// Get the matcher at the specified index. Matcher are added in insertion order.
        /// </summary>
        /// <param name="index">Index of the matcher</param>
        /// <returns>Request matcher</returns>
        public RequestMatcher GetMatcher(int index)
        {
            if (index < 0 || index >= _pathMatchers.Count)
            {
                throw new ArgumentOutOfRangeException(nameof(index), "Index is out of range.");
            }
            return _pathMatchers[index];
        }

        class HttpMessageHandlerStub : HttpMessageHandler
        {

            private readonly Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> _sendAsync;

            public HttpMessageHandlerStub(Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> sendAsync)
            {
                _sendAsync = sendAsync;
            }

            protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
            {
                return await _sendAsync(request, cancellationToken).ConfigureAwait(false);
            }
        }
    }
}