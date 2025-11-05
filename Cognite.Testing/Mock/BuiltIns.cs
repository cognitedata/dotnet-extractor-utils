using System;
using System.Dynamic;
using System.Net.Http;
using CogniteSdk.Token;
using Moq;

namespace Cognite.Extractor.Testing.Mock
{
    /// <summary>
    /// Extension methods for built-in matchers in the CdfMock.
    /// </summary>
    public static class BuiltIns
    {
        /// <summary>
        /// Add a basic token endpoint that matches /test/token and returns a static token response.
        /// </summary>
        /// <param name="mock">CdfMock object</param>
        /// <param name="expectedRequestCount">Expected request count for the matcher.</param>
        public static void AddTokenEndpoint(this CdfMock mock, Times expectedRequestCount)
        {
            mock.AddMatcher(new SimpleMatcher("post", "/test/token", (ctx, token) =>
            {
                dynamic tokenResponse = new ExpandoObject();
                tokenResponse.token_type = "Bearer";
                tokenResponse.expires_in = 3600;
                tokenResponse.access_token = "test-access-token";
                return (HttpResponseMessage)ctx.CreateJsonResponse(tokenResponse);
            }, expectedRequestCount));
        }

        /// <summary>
        /// Add the /token/inspect endpoint that returns a static token inspection response,
        /// with the given project in the list of projects.
        /// </summary>
        /// <param name="mock">CdfMock object</param>
        /// <param name="expectedRequestCount">Expected request count for the matcher.</param>
        /// <param name="project">Project name to include in the token inspection response.</param>
        public static void AddTokenInspectEndpoint(this CdfMock mock, Times expectedRequestCount, string project)
        {
            mock.AddMatcher(new SimpleMatcher("get", "/api/v1/token/inspect", (ctx, token) =>
            {
                return ctx.CreateJsonResponse(new TokenInspect
                {
                    Subject = "subject",
                    Projects = new[] { new TokenProject {
                        ProjectUrlName = project,
                    }}
                });
            }, expectedRequestCount));
        }
    }
}