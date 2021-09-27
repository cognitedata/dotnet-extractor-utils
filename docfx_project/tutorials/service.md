# Deploying extractor as windows servie

The installer template has built in support for creating a windows service using the same executable as the standalone version. This requires some setup, as the extractor must report its status to windows.

This setup requires the following packages:

 - `Microsoft.Extensions.Hosting`
 - `Microsoft.Extensions.Hosting.WindowsServices`
 - `Microsoft.Extensions.Logging.EventLog`
 
It might also be useful to use `System.CommandLine` for parsing command line arguments, though that will not be covered here.

The idea is to launch the extractor as a service if passed a specific command line argument, like `--service`, and launch it as standalone otherwise. The code could be as follows:

```c#
public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _eventLog;
    
    public Worker(ILogger<Worker> eventLog)
    {
        _eventLog = eventLog;
    }
    
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await ExtractorRunner.Run(
            "config.yml",
            null,
            "myExtractor",
            "myExtractor/1.0.0",
            false,
            true,
            true,
            true, // We don't really want the service to die too quickly
            stoppingToken,
            null,
            null,
            null,
            _eventLog // Pass the eventlog as startup logger
        );
    }
}

public static class Program
{
    static void Main(string[] args)
    {
        if (args[0] == '--service')
        {
            RunService();
        }
        else
        {
            RunStandalone();
        }
    }
    
    private static void RunService()
    {
        var builder = Host.CreateDefaultBuilder()
            .ConfigureServices((hostContext, services) =>
            {
                services.AddHostedService<Worker>();
            });
        if (OperatingSystem.IsWindows())
        {
            builder = builder
                .ConfigureLogging(loggerFactory => loggerFactory.AddEventLog())
                // It is very important that this is equal to "product_short_name" in the installer template
                .UseWindowsService(options => options.ServiceName == "MyExtractor");
        }
        builder.Build().Run();
    }
    
    private static void RunStandalone()
    {
        // Run extractor as usual.
    }
}
```

In the installer template config file, set

 - `product_short_name` equal to "MyExtractor" (the same as you used for the service name)
 - `service` to `true`
 - `service_args` to `--service` (the argument to run as service)
 
 
## Running multiple service instances

The easiest way to run multiple service instances is to add an argument `--working-dir` which indicates where the extractor should look for the config file, then pass that to `Run`.

Services can then be created using something like

`sc create myextractor1 binPath="path/to/exe --service --working-dir path/to/config" DisplayName="My Extractor 1"`

## Running services on Linux.

Technically any application can run as a service with no extra setup, but it is good practice to run the extractor as a daemon. Fortunately this is simple using `Microsoft.Extensions.Hosting.Systemd`. Just add 

```c#
else if (OperatingSystem.IsLinux())
{
    builder = builder.UseSystemd();
}
```

to `RunService()`.

Next, add a file `my-extractor.service` to `/etc/systemd/system/` which should look something like this:

```
[Unit]
Description=MyExtractor Service
After=network.target # Network is a fine dependency for most extractors
StartLimitIntervalSec=0

[Service]
Type=simple
Restart=always
RestartSec=1
ExecStart=/path/to/extractor --service --working-dir path/to/config

[Install]
WantedBy=multi-user.target
```

To start you should now be able to do `sudo systemctl start my-extractor`.

### Running multiple services on linux

On linux running multiple extractors can be done trivially using service templates.

Add another file to `/etc/systemd/system/` named `my-extractor@.service`.

```
[Unit]
Description=MyExtractor Service (%I)
After=network.target # Network is a fine dependency for most extractors
StartLimitIntervalSec=0

[Service]
Type=simple
Restart=always
RestartSec=1
ExecStartPre= mkdir -p /path/to/config/%i/
ExecStart=/path/to/extractor --service --working-dir path/to/config/%i/

[Install]
WantedBy=multi-user.target
```

Very similar to the normal version, except now we take an extra argument as a parameter and create a named folder for each instance of the extractor containing the working directory.

### Installing on linux

It is worth mentioning the proper way to install system applications on linux. Unlike windows having a single "working directory" is not considered normal or good practice. Instead, you should use the following directories:

 - `/etc/company/myextractor` for configuration files.
 - `/var/log/company/myextractor` for logs.
 - `/var/lib/company/myextractor` for the working directory, if you need one. This can contain temporary files and symlinks to logs and config files.
 - `/usr/bin` for the executable.
 
Setup of the extractor might be the following:

```
mkdir -p /var/lib/company/myextractor/
mkdir -p /var/log/company/myextractor/
mkdir -p /etc/company/myextractor/
ln -sfn /etc/company/myextractor/ /var/lib/company/myextractor/config
ln -sfn /var/log/company/myextractor/ /var/lib/company/myextractor/logs
/usr/bin/my-extractor --service --working-dir /var/lib/company/myextractor/
```

To actually create installers, you will typically create both RPM and DEB installers, which are the two most common types.
