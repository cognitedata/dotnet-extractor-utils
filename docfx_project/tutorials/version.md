# Embedding version information
When deploying an extractor it is very useful to be able to show the version number in both the metrics and the log. The extractor has some utilities to help with this, using git tags.

In order to use embedded version information, add the following code to your `.csproj` file:

```xml
  <ItemGroup>
    <EmbeddedResource Include="Properties\GitCommitHash">
      <CopyToOutputDirectory>Always</CopyToOutputDirectory>
    </EmbeddedResource>
    <EmbeddedResource Include="Properties\GitCommitTime">
      <CopyToOutputDirectory>Always</CopyToOutputDirectory>
    </EmbeddedResource>
  </ItemGroup>
  <PropertyGroup>
    <PreBuildEvent>
      mkdir "$(MSBuildProjectDirectory)/Properties"
      git describe --tags --dirty &gt; "$(MSBuildProjectDirectory)/Properties/GitCommitHash"
      git log -1  --format="%25%25ai" &gt; "$(MSBuildProjectDirectory)/Properties/GitCommitTime"
    </PreBuildEvent>
  </PropertyGroup>
```

The calls to `git log` and `git describe` can be replaced or expanded if desired.

Now version can be read during execution by calling

```c#
string version = Cognite.Extractor.Metrics.Version.GetVersion();
string status = Cognite.Extractor.Metrics.Version.Status();
log.Information("Revision information: {status}", status);

// One way to parse version for a cleaner output, for pure tags on the form x.x.x...
// If the last version was 1.0.0 three commits ago, this outputs 1.0.0-pre.03
// It might need to be replaced if your tags are on a different form.
string parsedVersion = new Regex(@"-(\d+)-*.*").Replace(version, "-pre.$1", 1);
log.Information("My Extractor. Version: {version}", parsedVersion);

Cognite.Extractor.Metrics.CommonMetrics.SetInfo("My Extractor", parsedVersion);
```

