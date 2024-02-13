using Microsoft.Win32;
using System;
using System.Collections.Generic;
using System.Drawing;
using System.IO;
using System.Linq;
using System.ServiceProcess;
using System.Windows.Forms;

namespace ServiceManager
{
    public partial class Form1 : Form
    {
        private List<KnownExtractor> _knownExtractors;

        public Form1()
        {
            InitializeComponent();
            _knownExtractors = FindWellKnownExtractors().ToList();
            PopulateExtractorsDropdown();
            Icon = Icon.ExtractAssociatedIcon(AppDomain.CurrentDomain.FriendlyName);
        }

        private void PopulateExtractorsDropdown()
        {
            extractorsDropDown.Items.Clear();
            foreach (var ext in _knownExtractors)
            {
                extractorsDropDown.Items.Add(ext);
            }

            if (extractorsDropDown.Items.Count > 0)
            {
                extractorsDropDown.SelectedIndex = 0;
            }
        }

        private IEnumerable<KnownExtractor> FindWellKnownExtractors()
        {
            var configs = KnownExtractorConfigs.KnownExtractors;
            foreach (var config in configs)
            {
                var reg = Registry.LocalMachine.OpenSubKey($"Software\\WOW6432Node\\Cognite\\{config.RegistryName}");
                if (reg == null)
                {
                    continue;
                }
                var installFolder = reg.GetValue("InstallFolder");
                if (installFolder == null) continue;
                var path = Path.Combine(installFolder.ToString(), config.WellKnownExePath);
                if (!File.Exists(path)) continue;

                config.FullExePath = path;

                yield return config;
            }
        }

        private void ReloadServiceList()
        {
            serviceList.Items.Clear();

            var selected = extractorsDropDown.SelectedItem as KnownExtractor;

            if (selected == null)
            {
                serviceList.Items.Clear();
                return;
            }

            var services = ServiceController
                .GetServices()
                .Where(s => s.ServiceName.StartsWith(selected.ServicePrefix))
                .ToList();

            foreach (var service in services)
            {
                serviceList.Items.Add(new ServiceListElement(service));
            }
        }

        private void extractorsDropDown_SelectedIndexChanged(object sender, EventArgs e)
        {
            ReloadServiceList();
        }

        private void Error(string message)
        {
            serviceStatus.ForeColor = Color.Red;
            serviceStatus.Text = message;
        }

        private void Error(IEnumerable<string> errors)
        {
            serviceStatus.ForeColor = Color.Red;
            serviceStatus.Lines = errors.ToArray();
        }

        private void Error(params string[] errors)
        {
            Error(errors as IEnumerable<string>);
        }

        private void Ok(string message)
        {
            serviceStatus.ForeColor = Color.Green;
            serviceStatus.Text = message;
        }

        private void addServiceBtn_Click(object sender, EventArgs e)
        {
            var name = nameBox.Text;
            var description = descriptionBox.Text;
            var workingDir = workingDirBox.Text;

            var errors = new List<string>();

            var selected = extractorsDropDown.SelectedItem as KnownExtractor;
            if (selected == null)
            {
                Error("No extractor selected");
                return;
            }

            if (string.IsNullOrEmpty(name)) errors.Add("Missing service name");
            if (string.IsNullOrEmpty(workingDir)) errors.Add("Missing service working directory");
            else if (!Directory.Exists(workingDir)) errors.Add("Working directory does not exist");
            

            if (errors.Any())
            {
                Error(errors);
                return;
            }

            var services = serviceList.Items
                .OfType<ServiceListElement>()
                .ToList();

            int index = 0;
            foreach (var svc in services)
            {
                try
                {
                    var idx = svc.Service.ServiceName.Replace(selected.ServicePrefix, "");
                    Error(idx);
                    var cIndex = Convert.ToInt32(idx);

                    if (cIndex > index)
                    {
                        index = cIndex;
                    }
                }
                catch { }
            }

            var serviceName = $"{selected.ServicePrefix}{index + 1}";

            string result = RunCommand.Run(
                $"/C sc create {serviceName} binPath=\"\\\"{selected.FullExePath}\\\" {selected.ServiceCommand} {selected.WorkingDirFlag} \\\"{workingDir}\\\"\" DisplayName=\"{name}\"");

            if (result.Contains("SUCCESS"))
            {
                RunCommand.Run($"/C sc description {serviceName} \"{description}\"");
                result = result.Replace("[SC] ", "");
                Ok(result);
            }
            else
            {
                Error("Failed to create service:", result);
            }

            ReloadServiceList();
        }

        private void selectWorkingDirBtn_Click(object sender, EventArgs e)
        {
            if (folderBrowserDialog1.ShowDialog() == DialogResult.OK)
            {
                workingDirBox.Text = folderBrowserDialog1.SelectedPath;
            }
        }

        private void deleteServiceBtn_Click(object sender, EventArgs e)
        {
            var selected = serviceList.SelectedItem as ServiceListElement;

            if (selected == null)
            {
                Error("No service selected");
                return;
            }

            DialogResult userCheck = MessageBox.Show("Are you sure you want to delete this service",
                "Confirm Delete", MessageBoxButtons.YesNo, MessageBoxIcon.Warning);

            if (userCheck == DialogResult.Yes)
            {
                string result = RunCommand.Run($"/C sc delete {selected.Service.ServiceName}");

                var userResult = result.Replace("[SC] ", "");
                if (result.Contains("SUCCESS"))
                {
                    Ok(userResult);
                }
                else
                {
                    Error("Failed to delete service:", userResult);
                }
            }

            ReloadServiceList();
        }
    }
}