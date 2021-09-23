# Embedding version information
When deploying an extractor it is very useful to be able to show the version number in both the metrics and the log. The extractor has some utilities to help with this, using git tags.

In order to use embedded version information, compile with the following options:

```
	/p:InformationalVersion="1.0.0"
	/p:Description="Description"
```

Typically, these can be obtained from git tags, using something along these lines:

```
	version = git describe --tags HEAD
	version = version.replace(/-(\d+)-.*/, '-pre.$1')
	description = sh('git describe --tags --dirty')
	time = git log -1 --format=%ai
	
	dotnet publish /p:InformationalVersion="$version" /p:Description="$description $time"
```

That way we get last tag as version, modified so that if the current commit is not tagged, you get `1.0.0-pre.05` where 05 indicates that there has been five commits since the last tag.

Description is here set to the last commit and last commit time.

When starting the extractor we would typically log

```
log.Information("Starting My Extractor. Version: {version}", Version.GetVersion(Assembly.GetExecutingAssembly()));
log.Information("Revision information: {status}", Version.GetDescription(Assembly.GetExecutingAssembly()));

Cognite.Extractor.Metrics.CommonMetrics.SetInfo("My Extractor", version);
```

If using the SetupTemplate, these parameters are passed to the build from the command line. 

`build.ps1 -b Path\To\MSBuild.exe -v $version -d $description -c Path\To\setup-config.json`
