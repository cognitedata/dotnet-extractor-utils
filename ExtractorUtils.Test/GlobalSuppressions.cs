// This file is used by Code Analysis to maintain SuppressMessage
// attributes that are applied to this project.
// Project-level suppressions either have no target or are given
// a specific target and scoped to a namespace, type, member, etc.

using System.Diagnostics.CodeAnalysis;

[assembly: SuppressMessage("Reliability", "CA2007:Consider calling ConfigureAwait on the awaited task", Justification = "xUnit1030", Scope = "namespaceanddescendants", Target = "~N:ExtractorUtils.Test")]

// TODO: Look into clearing these out
[assembly: SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope", Justification = "<Pending>", Scope = "namespaceanddescendants", Target = "~N:ExtractorUtils.Test.Unit")]
[assembly: SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope", Justification = "<Pending>", Scope = "namespaceanddescendants", Target = "~N:ExtractorUtils.Test.Integration")]
[assembly: SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope", Justification = "<Pending>", Scope = "member", Target = "~M:ExtractorUtils.Test.TestUtilities.GetMockedHttpClientFactory(System.Func{System.Net.Http.HttpRequestMessage,System.Threading.CancellationToken,System.Threading.Tasks.Task{System.Net.Http.HttpResponseMessage}})~System.ValueTuple{Moq.Mock{System.Net.Http.IHttpClientFactory},Moq.Mock{System.Net.Http.HttpMessageHandler}}")]
