# Using the installer template

The installer template is mostly configured from the command live and the config .json file, but more advanced changes may require modifying the project files. Feel free to copy the installer project and modifying it to your needs. The following instructions produce a single file executable for a .NET core project. It assumes that you have a config file named `config.example.yml` somewhere.

 - Update the `setup-config.json` file to suit your needs. The default fields are described below. All config fields except for `setup_project` is injected as a build property with msbuild into the `.wixproj` file.
 - Modify the `InstBanner.bmp` file in `Resources`, adding your extractor name to the blue field.
 - Modify or replace the `License.rtf` with a license adapted to your project.
 - (Optional) Add any new additional config files at the bottom of the `Product.wxs` file, by adding new `Component` blocks after the one for `config.example.yml`
 - (Optional) Add a cognite icon to your extractor by adding `Resources/black32x32.ico` to the folder for your actual extractor, and adding `<ApplicationIcon>black32x32.ico</ApplicationIcon>` to its .csproj file.
 - Compile the installer in a windows environment with Wix utils and msbuild installed, using something like the following command:
 
`build.ps1 -b Path\To\MSBuild.exe -v 1.0.0 -d "some description" -c Path\To\setup-config.json`

 - `-v` or `-version` is embedded as InformationalVersion when compiling, and can be retrieved at runtime. It is also used for the installer version, so it is required. A good way to retrieve this in a build environment is using a git tag, this way a github release can also be created based on tags.
 - `-b` or `-build` is the path to your MSBuild.exe.
 - `-d` or `-description` is embedded as Description when compiling. It must be set, either to something static, or something like the current git commit + git commit time.
 - `-c` or `-config` is the path to the json configuration file.

The following configuration parameters are used by default:

 - `target_project` is the csproj file for your extractor. Note that all paths are relative to the .wixproj file.
 - `target_configuration` is generally Release.
 - `target_runtime` is most likely either win-x64 or win-x86
 - `product_name` full display name of your product. This is used both in the installer and in the installed programs registry on the target computer.
 - `product_short_name` a short version of the product name without spaces. Should not contain "Cognite". It is used for registry keys and folder names.
 - `exe_name` the name of the final executable.
 - `config_dir` the path to the config.example.yml folder. This is also relative to the .wixproj folder.
 - `service` can be left out. If `true`, the installer will add and configure a windows service for the extractor.
 - `service-args` can be left out. Arguments to specify to the extractor when running it as a service. This is useful if the standalone and service versions use the same executable, but with different command line parameters.
 - `upgrade-guid` is a unique upper case GUID which you must generate yourself. It identifies the project when upgrading.
 - `setup-project` is the path to the .wixproj file used to build.
 - `output-name` is the name of the output msi, like `MyExtractorSetup`.
 
See the ExampleExtractorSetup project in this repository for a full example.

In general the setup template assumes that this is a cognite product, but changing this is no more difficult than replacing instances of `Cognite` in `Product.wxs` with whatever suits your purposes.
 
## Modifying the installer template

Adding to the installer template is relatively easy. New builds can be added in the `<Target Name="BeforeBuild">` block in `SetupTemplate.wixproj`, these should output to new folders. New files going in the `bin/` folder can be added to `<ComponentGroup Id="ExecutableComponentGroup">`. Note that the executable is duplicated here due to conditionals on `service`. New components can be added after `<?endif?>`.

New folders can be added by adding new `Directory` tags in the first `Fragment`, a new `ComponentGroupRef` at the bottom of `Product`, and a new `ComponentGroup` somewhere in the last `Fragment`.