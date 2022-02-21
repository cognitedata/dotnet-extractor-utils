# Basic utils usage

Some applications do not want the full utils runtime, but would still like the utilities for authentication, and the dependency injection based flow to make working with the SDK easier. This article describes the easiest way to use the utils to connect to the Cognite API.

In general, it is highly recommended to use an IDE with code completion like visual studio, visual studio code with a C# extension, or similar.

## Using the full ExtractorUtils package

The most powerful is to use the full utils package. To start with, add a dependency to a recent version of `Cognite.ExtractorUtils`, found [here](https://www.nuget.org/packages/Cognite.ExtractorUtils/). This will include the rest of the Extractor Utils packages and the CogniteSdk package.

Next, we want to add the necessary services:

```c#
var services = new ServiceCollection();
services.AddExtractorDependencies<BaseConfig>(
    configPath: "config.yml", // Path to your configuration file
    acceptedConfigVersions: new [] { 1 }, // Fail if "version" in the config file does not match this, this can be null to not have any version requirement.
    appId: "my-app", // Application id passed with HTTP requests to CDF
    userAgent: "myapp/1.0.0", // User agent for HTTP requests, including application version
    addStateStore: false, // True if you want to register code for using a litedb state store
    addLogger: true, // False if you do not want to register a serilog logger
    addMetrics: false // True if you want to push or serve metrics to prometheus
);
var provider = services.BuildServiceProvider();

var destination = provider.GetRequiredService<CogniteDestination>();
```

`destination` contains a number of useful methods, and also a property `CogniteClient`, which is an instance of the SDK client, containing methods for most CDF endpoints.

The configuration type is here `BaseConfig`, but you can use any class that extends `VersionedConfig`. The full range of available options in `BaseConfig` is shown in [config.example.yml](https://github.com/cognitedata/dotnet-extractor-utils/blob/master/ExtractorUtils/config/config.example.yml).

A reasonable minimal configuration would look something like this:

```yaml
version: 1

logger:
  console:
    level: "debug"

cognite:
    project: ${COGNITE_PROJECT}
    host: ${COGNITE_BASE_URL}
  
    # This is for microsoft as IdP, to use a different provider,
    # set implementation: Basic, and use token-url instead of tenant.
    # See the example config for the full list of options.
    idp-authentication:
        # Directory tenant
        tenant: ${COGNITE_TENANT_ID}
        # Application Id
        client-id: ${COGNITE_CLIENT_ID}
        # Client secret
        secret: ${COGNITE_CLIENT_SECRET}
        # List of resource scopes, ex:
        # scopes:
        #   - scopeA
        #   - scopeB
        scopes:
          - ${COGNITE_BASE_URL}/.default
```

Alternatively you can create the configuration object manually. For the equivalent to the minimal configuration above:

```c#
var config = new BaseConfig {
    Version = 1,
    Logger = new LoggerConfig {
        Console = new ConsoleConfig {
            Level = "debug"
        }
    },
    Cognite = new CogniteConfig {
        Project = Environment.GetEnvironmentVariable("COGNITE_PROJECT"),
        Host = Environment.GetEnvironmentVariable("COGNITE_BASE_URL"),
        IdpAuthentication = new AuthenticatorConfig
        {
            Tenant = Environment.GetEnvironmentVariable("COGNITE_TENANT"),
            ClientId = Environment.GetEnvironmentVariable("COGNITE_CLIENT_ID"),
            Secret = Environment.GetEnvironmentVariable("COGNITE_CLIENT_SECRET"),
            Scopes = new[]
            {
                $"{Environment.GetEnvironmentVariable("COGNITE_BASE_URL")}/.default"
            }
        }
    }
};

config.GenerateDefaults();

services.AddExtractorDependencies(
    configPath: null,
    acceptedConfigVersions: new [] { 1 }, // Fail if "version" in the config file does not match this, this can be null to not have any version requirement.
    appId: "my-app", // Application id passed with HTTP requests to CDF
    userAgent: "myapp/1.0.0", // User agent for HTTP requests, including application version
    addStateStore: false, // True if you want to register code for using a litedb state store
    addLogger: true, // False if you do not want to register a serilog logger
    addMetrics: false, // True if you want to push or serve metrics to prometheus
    config: config // Pass a pre-existing config object
);
```

## Using just Cognite.Extensions

Since the full ExtractorUtils package has quite a few dependencies, it may sometimes be desirable to just use packages related to authentication. In this case you will have to create the SDK client yourself.

Include a reference to the latest version of `Cognite.Extensions`, found [here](https://www.nuget.org/packages/Cognite.Extensions/), this also includes a reference to the SDK.

To create a client:

```c#
var config = new AuthenticatorConfig
{
    Tenant = Environment.GetEnvironmentVariable("COGNITE_TENANT"),
    ClientId = Environment.GetEnvironmentVariable("COGNITE_CLIENT_ID"),
    Secret = Environment.GetEnvironmentVariable("COGNITE_CLIENT_SECRET"),
    Scopes = new[]
    {
        $"{Environment.GetEnvironmentVariable("COGNITE_BASE_URL")}/.default"
    }
};

var authenticator = new Authenticator(config);
using var client = new HttpClient();

var cogniteClient = new Client.Builder()
    .SetHttpClient(client)
    .SetBaseUrl(new Uri(Environment.GetEnvironmentVariable("COGNITE_BASE_URL")))
    .SetProject(Environment.GetEnvironmentVariable("COGNITE_PROJECT"))
	.SetAppId("my-app")
    .SetUserAgent("myapp/1.0.0")
	.SetTokenProvider(auth.GetToken)
	.Build();
```
