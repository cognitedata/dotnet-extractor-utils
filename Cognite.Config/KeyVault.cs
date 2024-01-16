using System;
using System.Runtime.Serialization;
using Azure.Core;
using Azure.Identity;
using Azure.Security.KeyVault.Secrets;
using Cognite.Extractor.Common;
using Cognite.Extractor.Configuration;
using YamlDotNet.Core;
using YamlDotNet.Core.Events;
using YamlDotNet.Serialization;

namespace Cognite.Extractor.KeyVault
{
    /// <summary>
    /// Method used to authenticate against azure.
    /// </summary>
    public enum KeyVaultAuthenticationMethod
    {
        /// <summary>
        /// Authenticate using client secret credentials.
        /// </summary>
        [EnumMember(Value = "client-secret")]
        ClientSecret,
        /// <summary>
        /// Authenticate using machine default credentials.
        /// </summary>
        [EnumMember(Value = "default")]
        Default,
    }


    /// <summary>
    /// Configuration for azure key vault.
    /// </summary>
    public class KeyVaultConfig
    {
        /// <summary>
        /// How to authenticate against key vault.
        /// </summary>
        public KeyVaultAuthenticationMethod? AuthenticationMethod { get; set; }
        /// <summary>
        /// Azure key vault name.
        /// </summary>
        public string? VaultName { get; set; }
        /// <summary>
        /// Azure tenant ID.
        /// </summary>
        public string? TenantId { get; set; }
        /// <summary>
        /// Azure client id.
        /// </summary>
        public string? ClientId { get; set; }
        /// <summary>
        /// Azure client secret.
        /// </summary>
        public string? Secret { get; set; }

        private SecretClient? _client;

        /// <summary>
        /// Get a client singleton owned by this config.
        /// </summary>
        /// <returns></returns>
        public SecretClient GetClient()
        {
            if (_client != null)
            {
                return _client;
            }

            TokenCredential? credentials = null;

            if (AuthenticationMethod == KeyVaultAuthenticationMethod.Default)
            {
                credentials = new DefaultAzureCredential();
            }
            else if (AuthenticationMethod == KeyVaultAuthenticationMethod.ClientSecret)
            {
                if (string.IsNullOrWhiteSpace(VaultName)) throw new ConfigurationException("Missing KeyVault vault name");
                if (string.IsNullOrWhiteSpace(TenantId)) throw new ConfigurationException("Missing KeyVault tenant ID");
                if (string.IsNullOrWhiteSpace(ClientId)) throw new ConfigurationException("Missing KeyVault client ID");
                if (string.IsNullOrWhiteSpace(Secret)) throw new ConfigurationException("Missing KeyVault client secret");
                credentials = new ClientSecretCredential(
                    TenantId,
                    ClientId,
                    Secret
                );
            }


            if (credentials == null) throw new ConfigurationException("Missing authentication-method in KeyVault config");

            _client = new SecretClient(
                new Uri($"https://{VaultName}.vault.azure.net"),
                credentials
            );
            return _client;
        }

        /// <summary>
        /// Add a key vault node deserializer and tag mapping to a yaml deserializer builder.
        /// </summary>
        /// <param name="builder">Builder to add key vault support to</param>
        internal void AddKeyVault(DeserializerBuilder builder)
        {
            if (builder == null) throw new ArgumentNullException(nameof(builder));
            try
            {
                builder.WithoutNodeDeserializer(typeof(KeyVaultResolver));
            }
            catch { }

            builder.WithNodeDeserializer(new KeyVaultResolver(this));
        }
    }

    internal class KeyVaultResolver : INodeDeserializer
    {
        private SecretClient _client;

        public KeyVaultResolver(KeyVaultConfig config)
        {
            _client = config.GetClient();
        }
        public bool Deserialize(IParser reader, Type expectedType, Func<IParser, Type, object?> nestedObjectDeserializer, out object? value)
        {
            if (reader.Accept<Scalar>(out var scalar)
                && scalar != null
                && !scalar.Tag.IsEmpty
                && scalar.Tag.Value == "!keyvault")
            {
                reader.MoveNext();

                var secretValue = _client.GetSecret(scalar.Value);
                value = secretValue.Value.Value;
                return true;
            }
            value = null;
            return false;
        }
    }
}