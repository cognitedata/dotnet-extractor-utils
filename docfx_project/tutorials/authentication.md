# Using Cognite.Extensions for authentication

Since the SDK has no built-in way to do OIDC authentication, it is sometimes convenient to have a library to do so. The utils handles this automatically when using the standard dependency injection method described elsewhere, but this means loading libraries for metrics, logging, configuration and local state. The Authenticator class is contained in the Cognite.Extensions library, and can be used with the SDK as follows:

```c#
using var client = new HttpClient();
var auth = new Authenticator(new AuthenticatorConfig
{
	// Credentials added here
}, client, null /* or logger, if you have one configured */);
var cogniteClient = new Client.Builder()
	.SetAppId("my-app")
	.SetProject("my-project")            
	.SetTokenProvider(auth.GetToken)
	.Build();
```