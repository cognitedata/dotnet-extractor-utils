using System.Dynamic;

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
        /// <param name="minRequests">Expected requests minimum</param>
        /// <param name="maxRequests">Expected requests maximum</param>
        public static void AddTokenEndpoint(this CdfMock mock, int minRequests = 0, int maxRequests = int.MaxValue)
        {
            mock.AddMatcher(new SimpleMatcher("post", "/test/token", (ctx, token) =>
            {
                dynamic tokenResponse = new ExpandoObject();
                tokenResponse.token_type = "Bearer";
                tokenResponse.expires_in = 3600;
                tokenResponse.access_token = "test-access-token";
                return ctx.CreateJsonResponse(tokenResponse);
            }, minRequests, maxRequests));
        }
    }
}