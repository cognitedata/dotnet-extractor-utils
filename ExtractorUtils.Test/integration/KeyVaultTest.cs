using System.IO;
using Cognite.Extractor.Utils;
using ExtractorUtils.Test.Unit;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace ExtractorUtils.Test.Integration
{
    public static class KeyVaultTest
    {
        [Fact]
        public static void TestKeyVault()
        {
            var lines = new[] {
                "version: 1",
                "foo: !keyvault test-id",
                "baz: !keyvault test-secret",
                "key-vault:",
                "    authentication-method: client-secret",
                "    keyvault-name: extractor-keyvault",
                "    tenant-id: ${KEYVAULT_TENANT_ID}",
                "    client-id: ${KEYVAULT_CLIENT_ID}",
                "    secret: ${KEYVAULT_CLIENT_SECRET}"
            };
            string path = "test-keyvault-config.yml";

            File.WriteAllLines(path, lines);
            var services = new ServiceCollection();
            var res = ConfigurationExtensions.AddConfig<TestConfig>(services, path);

            Assert.Equal("12345", res.Foo);
            Assert.Equal("abcde", res.Baz);
        }
    }
}