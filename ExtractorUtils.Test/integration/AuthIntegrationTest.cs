using System.Linq;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;
using Cognite.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Identity.Client;
using Xunit;
using Xunit.Abstractions;

namespace ExtractorUtils.Test.Integration
{
    public class AuthIntegrationTest
    {
        private readonly ITestOutputHelper _output;
        public AuthIntegrationTest(ITestOutputHelper output)
        {
            _output = output;
        }


        [Theory]
        [InlineData(CogniteHost.GreenField)]
        [InlineData(CogniteHost.BlueField)]
        public async Task TestClientConfig(CogniteHost host)
        {
            if (host == CogniteHost.BlueField)
            {
                var configMsal = CDFTester.GetConfig(host);
                using (var tester = new CDFTester(configMsal, _output))
                {
                    await tester.Destination.TestCogniteConfig(tester.Source.Token); // should not throw
                }

                // Fail to validate the project
                configMsal = CDFTester.GetConfig(host);
                configMsal[5] = "  project: not-a-valid-project";
                using (var tester = new CDFTester(configMsal, _output))
                {
                    await Assert.ThrowsAsync<CogniteUtilsException>(() => tester.Destination.TestCogniteConfig(tester.Source.Token));
                }

                // Fail to obtain the token
                configMsal = CDFTester.GetConfig(host);
                configMsal[10] = "    secret: invalid-secret";
                using (var tester = new CDFTester(configMsal, _output))
                {
                    await Assert.ThrowsAsync<CogniteUtilsException>(() => tester.Destination.TestCogniteConfig(tester.Source.Token));
                }

                var configList = new List<string>(CDFTester.GetConfig(host));
                configList.Insert(13, "    implementation: basic");
                configList.Insert(14, "    token-url: https://login.microsoftonline.com/${BF_TEST_TENANT}/oauth2/v2.0/token");
                var configBasic = configList.ToArray();
                using (var tester = new CDFTester(configBasic, _output))
                {
                    await tester.Destination.TestCogniteConfig(tester.Source.Token); // should not throw
                }

                configBasic = configList.ToArray();
                configBasic[10] = "    secret: invalid-secret";
                using (var tester = new CDFTester(configBasic, _output))
                {
                    await Assert.ThrowsAsync<CogniteUtilsException>(() => tester.Destination.TestCogniteConfig(tester.Source.Token));
                }
            }
            else if (host == CogniteHost.GreenField)
            {
                var configKey = CDFTester.GetConfig(host);
                using (var tester = new CDFTester(configKey, _output))
                {
                    await tester.Destination.TestCogniteConfig(tester.Source.Token); // should not throw
                }

                // Fail to validate the project
                configKey = CDFTester.GetConfig(host);
                configKey[5] = "  project: not-a-valid-project";
                using (var tester = new CDFTester(configKey, _output))
                {
                    await Assert.ThrowsAsync<CogniteUtilsException>(() => tester.Destination.TestCogniteConfig(tester.Source.Token));
                }

                // Not logged in
                configKey = CDFTester.GetConfig(host);
                configKey[6] = "  api-key: invalid-api-key";
                using (var tester = new CDFTester(configKey, _output))
                {
                    await Assert.ThrowsAsync<CogniteUtilsException>(() => tester.Destination.TestCogniteConfig(tester.Source.Token));
                }
            }
        }

        [Theory]
        [InlineData(CogniteHost.BlueField)]
        public async Task TestClientHeaders(CogniteHost host)
        {
            var configMsal = CDFTester.GetConfig(host);
            using var tester = new CDFTester(configMsal, _output);

            var factory = tester.Provider.GetRequiredService<IHttpClientFactory>();
            var client = factory.CreateClient("AuthenticatorClient");
            Assert.NotEmpty(client.DefaultRequestHeaders);
            Assert.NotEmpty(client.DefaultRequestHeaders.UserAgent);
            Assert.Equal(2, client.DefaultRequestHeaders.UserAgent.Count);
            var product = client.DefaultRequestHeaders.UserAgent.ToArray()[0].Product;
            Assert.Equal("Utils-Tests", product.Name);
            Assert.Equal("v1.0.0", product.Version);
            var comment = client.DefaultRequestHeaders.UserAgent.ToArray()[1].Comment;
            Assert.Equal("(Test)", comment);
            await tester.Destination.TestCogniteConfig(tester.Source.Token); // should not throw
        }
    }
}
