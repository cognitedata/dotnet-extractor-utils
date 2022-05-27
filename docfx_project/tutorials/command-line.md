# Adding command line arguments

The utils contains a thin wrapper around [System.CommandLine](https://www.nuget.org/packages/System.CommandLine), adapted for fast development of command line interfaces.

To use it, create a class containing your command line arguments, and tag them with a `CommandLineOption` attribute:

```c#
class MyOptions
{
    [CommandLineOption("Specify path to config file", true, "-c")]
    public string ConfigPath { get; set; }
}
```

The first argument is the description shown when using `--help`, the second is whether to include an automatic name, in this case it would be `--config-file`, and any further arguments are aliases.

Now, instead of invoking `ExtractorRunner.Run` directly, invoke it inside a command line handler:

```c#
static async Task Main(string[] args)
{
    var command = new RootCommand
    {
        Description = "My Extractor"
    };
    var binder = new AttributeBinder<MyOptions>();
    binder.AddOptionsToCommand(command);

    command.SetHandler<MyOptions>(async opt =>
    {
        await ExtractorRunner.Run(...);
    }, binder);

    await command.InvokeAsync(args).ConfigureAwait(false);
}
```

This is flexible, and can be used with sub-commands, just call `AddOptionsToCommand` for each sub-command.

