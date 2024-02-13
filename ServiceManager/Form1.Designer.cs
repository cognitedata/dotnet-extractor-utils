using System.Windows.Forms;

namespace ServiceManager
{
    partial class Form1
    {
        /// <summary>
        ///  Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        ///  Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Windows Form Designer generated code

        /// <summary>
        ///  Required method for Designer support - do not modify
        ///  the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            this.panel1 = new System.Windows.Forms.Panel();
            this.serviceList = new System.Windows.Forms.ListBox();
            this.panel5 = new System.Windows.Forms.Panel();
            this.deleteServiceBtn = new System.Windows.Forms.Button();
            this.label1 = new System.Windows.Forms.Label();
            this.panel2 = new System.Windows.Forms.Panel();
            this.label2 = new System.Windows.Forms.Label();
            this.extractorsDropDown = new System.Windows.Forms.ComboBox();
            this.panel3 = new System.Windows.Forms.Panel();
            this.panel4 = new System.Windows.Forms.Panel();
            this.panel7 = new System.Windows.Forms.Panel();
            this.panel8 = new System.Windows.Forms.Panel();
            this.panel10 = new System.Windows.Forms.Panel();
            this.descriptionBox = new System.Windows.Forms.TextBox();
            this.label5 = new System.Windows.Forms.Label();
            this.panel11 = new System.Windows.Forms.Panel();
            this.nameBox = new System.Windows.Forms.TextBox();
            this.label6 = new System.Windows.Forms.Label();
            this.panel9 = new System.Windows.Forms.Panel();
            this.workingDirBox = new System.Windows.Forms.TextBox();
            this.label7 = new System.Windows.Forms.Label();
            this.panel12 = new System.Windows.Forms.Panel();
            this.selectWorkingDirBtn = new System.Windows.Forms.Button();
            this.label4 = new System.Windows.Forms.Label();
            this.panel13 = new System.Windows.Forms.Panel();
            this.addServiceBtn = new System.Windows.Forms.Button();
            this.panel6 = new System.Windows.Forms.Panel();
            this.serviceStatus = new System.Windows.Forms.TextBox();
            this.folderBrowserDialog1 = new System.Windows.Forms.FolderBrowserDialog();
            this.panel1.SuspendLayout();
            this.panel5.SuspendLayout();
            this.panel2.SuspendLayout();
            this.panel3.SuspendLayout();
            this.panel4.SuspendLayout();
            this.panel7.SuspendLayout();
            this.panel8.SuspendLayout();
            this.panel10.SuspendLayout();
            this.panel11.SuspendLayout();
            this.panel9.SuspendLayout();
            this.panel12.SuspendLayout();
            this.panel13.SuspendLayout();
            this.panel6.SuspendLayout();
            this.SuspendLayout();
            // 
            // panel1
            // 
            this.panel1.AutoSize = true;
            this.panel1.Controls.Add(this.serviceList);
            this.panel1.Controls.Add(this.panel5);
            this.panel1.Controls.Add(this.label1);
            this.panel1.Dock = System.Windows.Forms.DockStyle.Fill;
            this.panel1.Location = new System.Drawing.Point(15, 102);
            this.panel1.Name = "panel1";
            this.panel1.Padding = new System.Windows.Forms.Padding(6);
            this.panel1.Size = new System.Drawing.Size(278, 505);
            this.panel1.TabIndex = 0;
            // 
            // serviceList
            // 
            this.serviceList.Dock = System.Windows.Forms.DockStyle.Fill;
            this.serviceList.FormattingEnabled = true;
            this.serviceList.ItemHeight = 25;
            this.serviceList.Location = new System.Drawing.Point(6, 47);
            this.serviceList.Name = "serviceList";
            this.serviceList.Size = new System.Drawing.Size(266, 397);
            this.serviceList.TabIndex = 0;
            // 
            // panel5
            // 
            this.panel5.Controls.Add(this.deleteServiceBtn);
            this.panel5.Dock = System.Windows.Forms.DockStyle.Bottom;
            this.panel5.Location = new System.Drawing.Point(6, 444);
            this.panel5.Name = "panel5";
            this.panel5.Padding = new System.Windows.Forms.Padding(12);
            this.panel5.Size = new System.Drawing.Size(266, 55);
            this.panel5.TabIndex = 2;
            // 
            // deleteServiceBtn
            // 
            this.deleteServiceBtn.AutoSize = true;
            this.deleteServiceBtn.Location = new System.Drawing.Point(12, 12);
            this.deleteServiceBtn.Name = "deleteServiceBtn";
            this.deleteServiceBtn.Size = new System.Drawing.Size(132, 35);
            this.deleteServiceBtn.TabIndex = 1;
            this.deleteServiceBtn.Text = "Delete Service";
            this.deleteServiceBtn.UseVisualStyleBackColor = true;
            this.deleteServiceBtn.Click += new System.EventHandler(this.deleteServiceBtn_Click);
            // 
            // label1
            // 
            this.label1.Dock = System.Windows.Forms.DockStyle.Top;
            this.label1.Location = new System.Drawing.Point(6, 6);
            this.label1.Margin = new System.Windows.Forms.Padding(3);
            this.label1.Name = "label1";
            this.label1.Padding = new System.Windows.Forms.Padding(0, 0, 0, 3);
            this.label1.Size = new System.Drawing.Size(266, 41);
            this.label1.TabIndex = 1;
            this.label1.Text = "Existing services";
            // 
            // panel2
            // 
            this.panel2.Controls.Add(this.label2);
            this.panel2.Controls.Add(this.extractorsDropDown);
            this.panel2.Dock = System.Windows.Forms.DockStyle.Top;
            this.panel2.Location = new System.Drawing.Point(15, 15);
            this.panel2.Name = "panel2";
            this.panel2.Padding = new System.Windows.Forms.Padding(6);
            this.panel2.Size = new System.Drawing.Size(278, 87);
            this.panel2.TabIndex = 2;
            // 
            // label2
            // 
            this.label2.Dock = System.Windows.Forms.DockStyle.Fill;
            this.label2.Location = new System.Drawing.Point(6, 6);
            this.label2.Margin = new System.Windows.Forms.Padding(3);
            this.label2.Name = "label2";
            this.label2.Size = new System.Drawing.Size(266, 42);
            this.label2.TabIndex = 2;
            this.label2.Text = "Extractors";
            // 
            // extractorsDropDown
            // 
            this.extractorsDropDown.Dock = System.Windows.Forms.DockStyle.Bottom;
            this.extractorsDropDown.DropDownStyle = System.Windows.Forms.ComboBoxStyle.DropDownList;
            this.extractorsDropDown.FormattingEnabled = true;
            this.extractorsDropDown.Location = new System.Drawing.Point(6, 48);
            this.extractorsDropDown.Name = "extractorsDropDown";
            this.extractorsDropDown.Size = new System.Drawing.Size(266, 33);
            this.extractorsDropDown.TabIndex = 0;
            this.extractorsDropDown.SelectedIndexChanged += new System.EventHandler(this.extractorsDropDown_SelectedIndexChanged);
            // 
            // panel3
            // 
            this.panel3.Controls.Add(this.panel1);
            this.panel3.Controls.Add(this.panel2);
            this.panel3.Dock = System.Windows.Forms.DockStyle.Left;
            this.panel3.Location = new System.Drawing.Point(0, 0);
            this.panel3.Name = "panel3";
            this.panel3.Padding = new System.Windows.Forms.Padding(15, 15, 0, 15);
            this.panel3.Size = new System.Drawing.Size(293, 622);
            this.panel3.TabIndex = 3;
            // 
            // panel4
            // 
            this.panel4.Controls.Add(this.panel7);
            this.panel4.Controls.Add(this.panel13);
            this.panel4.Controls.Add(this.panel6);
            this.panel4.Dock = System.Windows.Forms.DockStyle.Fill;
            this.panel4.Location = new System.Drawing.Point(293, 0);
            this.panel4.Name = "panel4";
            this.panel4.Padding = new System.Windows.Forms.Padding(15);
            this.panel4.Size = new System.Drawing.Size(631, 622);
            this.panel4.TabIndex = 4;
            // 
            // panel7
            // 
            this.panel7.Controls.Add(this.panel8);
            this.panel7.Controls.Add(this.label4);
            this.panel7.Dock = System.Windows.Forms.DockStyle.Fill;
            this.panel7.Location = new System.Drawing.Point(15, 102);
            this.panel7.Name = "panel7";
            this.panel7.Padding = new System.Windows.Forms.Padding(6);
            this.panel7.Size = new System.Drawing.Size(601, 444);
            this.panel7.TabIndex = 2;
            // 
            // panel8
            // 
            this.panel8.Controls.Add(this.panel10);
            this.panel8.Controls.Add(this.panel11);
            this.panel8.Controls.Add(this.panel9);
            this.panel8.Dock = System.Windows.Forms.DockStyle.Fill;
            this.panel8.Location = new System.Drawing.Point(6, 47);
            this.panel8.Name = "panel8";
            this.panel8.Size = new System.Drawing.Size(589, 391);
            this.panel8.TabIndex = 2;
            // 
            // panel10
            // 
            this.panel10.Controls.Add(this.descriptionBox);
            this.panel10.Controls.Add(this.label5);
            this.panel10.Dock = System.Windows.Forms.DockStyle.Fill;
            this.panel10.Location = new System.Drawing.Point(0, 36);
            this.panel10.Name = "panel10";
            this.panel10.Padding = new System.Windows.Forms.Padding(0, 6, 0, 6);
            this.panel10.Size = new System.Drawing.Size(589, 312);
            this.panel10.TabIndex = 1;
            // 
            // descriptionBox
            // 
            this.descriptionBox.Dock = System.Windows.Forms.DockStyle.Fill;
            this.descriptionBox.Location = new System.Drawing.Point(177, 6);
            this.descriptionBox.Multiline = true;
            this.descriptionBox.Name = "descriptionBox";
            this.descriptionBox.Size = new System.Drawing.Size(412, 300);
            this.descriptionBox.TabIndex = 1;
            // 
            // label5
            // 
            this.label5.Dock = System.Windows.Forms.DockStyle.Left;
            this.label5.Location = new System.Drawing.Point(0, 6);
            this.label5.Name = "label5";
            this.label5.Size = new System.Drawing.Size(177, 300);
            this.label5.TabIndex = 0;
            this.label5.Text = "Description:";
            // 
            // panel11
            // 
            this.panel11.Controls.Add(this.nameBox);
            this.panel11.Controls.Add(this.label6);
            this.panel11.Dock = System.Windows.Forms.DockStyle.Top;
            this.panel11.Location = new System.Drawing.Point(0, 0);
            this.panel11.Name = "panel11";
            this.panel11.Padding = new System.Windows.Forms.Padding(0, 0, 0, 6);
            this.panel11.Size = new System.Drawing.Size(589, 36);
            this.panel11.TabIndex = 2;
            // 
            // nameBox
            // 
            this.nameBox.Dock = System.Windows.Forms.DockStyle.Fill;
            this.nameBox.Location = new System.Drawing.Point(177, 0);
            this.nameBox.Name = "nameBox";
            this.nameBox.Size = new System.Drawing.Size(412, 31);
            this.nameBox.TabIndex = 2;
            // 
            // label6
            // 
            this.label6.Dock = System.Windows.Forms.DockStyle.Left;
            this.label6.Location = new System.Drawing.Point(0, 0);
            this.label6.Name = "label6";
            this.label6.Size = new System.Drawing.Size(177, 30);
            this.label6.TabIndex = 1;
            this.label6.Text = "Name:";
            // 
            // panel9
            // 
            this.panel9.Controls.Add(this.workingDirBox);
            this.panel9.Controls.Add(this.label7);
            this.panel9.Controls.Add(this.panel12);
            this.panel9.Dock = System.Windows.Forms.DockStyle.Bottom;
            this.panel9.Location = new System.Drawing.Point(0, 348);
            this.panel9.Name = "panel9";
            this.panel9.Padding = new System.Windows.Forms.Padding(0, 6, 0, 0);
            this.panel9.Size = new System.Drawing.Size(589, 43);
            this.panel9.TabIndex = 0;
            // 
            // workingDirBox
            // 
            this.workingDirBox.Dock = System.Windows.Forms.DockStyle.Top;
            this.workingDirBox.Location = new System.Drawing.Point(177, 6);
            this.workingDirBox.Name = "workingDirBox";
            this.workingDirBox.Size = new System.Drawing.Size(330, 31);
            this.workingDirBox.TabIndex = 2;
            // 
            // label7
            // 
            this.label7.Dock = System.Windows.Forms.DockStyle.Left;
            this.label7.Location = new System.Drawing.Point(0, 6);
            this.label7.Name = "label7";
            this.label7.Size = new System.Drawing.Size(177, 37);
            this.label7.TabIndex = 1;
            this.label7.Text = "Working Dir:";
            // 
            // panel12
            // 
            this.panel12.Controls.Add(this.selectWorkingDirBtn);
            this.panel12.Dock = System.Windows.Forms.DockStyle.Right;
            this.panel12.Location = new System.Drawing.Point(507, 6);
            this.panel12.Name = "panel12";
            this.panel12.Size = new System.Drawing.Size(82, 37);
            this.panel12.TabIndex = 3;
            // 
            // selectWorkingDirBtn
            // 
            this.selectWorkingDirBtn.AutoSize = true;
            this.selectWorkingDirBtn.Location = new System.Drawing.Point(6, -2);
            this.selectWorkingDirBtn.Name = "selectWorkingDirBtn";
            this.selectWorkingDirBtn.Size = new System.Drawing.Size(68, 35);
            this.selectWorkingDirBtn.TabIndex = 0;
            this.selectWorkingDirBtn.Text = "Select";
            this.selectWorkingDirBtn.UseVisualStyleBackColor = true;
            this.selectWorkingDirBtn.Click += new System.EventHandler(this.selectWorkingDirBtn_Click);
            // 
            // label4
            // 
            this.label4.Dock = System.Windows.Forms.DockStyle.Top;
            this.label4.Location = new System.Drawing.Point(6, 6);
            this.label4.Margin = new System.Windows.Forms.Padding(3);
            this.label4.Name = "label4";
            this.label4.Padding = new System.Windows.Forms.Padding(0, 0, 0, 3);
            this.label4.Size = new System.Drawing.Size(589, 41);
            this.label4.TabIndex = 1;
            this.label4.Text = "Create new service:";
            // 
            // panel13
            // 
            this.panel13.Controls.Add(this.addServiceBtn);
            this.panel13.Dock = System.Windows.Forms.DockStyle.Bottom;
            this.panel13.Location = new System.Drawing.Point(15, 546);
            this.panel13.Name = "panel13";
            this.panel13.Size = new System.Drawing.Size(601, 61);
            this.panel13.TabIndex = 2;
            // 
            // addServiceBtn
            // 
            this.addServiceBtn.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Right)));
            this.addServiceBtn.AutoSize = true;
            this.addServiceBtn.Location = new System.Drawing.Point(455, 12);
            this.addServiceBtn.Name = "addServiceBtn";
            this.addServiceBtn.Size = new System.Drawing.Size(132, 35);
            this.addServiceBtn.TabIndex = 2;
            this.addServiceBtn.Text = "Create Service";
            this.addServiceBtn.UseVisualStyleBackColor = true;
            this.addServiceBtn.Click += new System.EventHandler(this.addServiceBtn_Click);
            // 
            // panel6
            // 
            this.panel6.Controls.Add(this.serviceStatus);
            this.panel6.Dock = System.Windows.Forms.DockStyle.Top;
            this.panel6.Location = new System.Drawing.Point(15, 15);
            this.panel6.Name = "panel6";
            this.panel6.Padding = new System.Windows.Forms.Padding(6);
            this.panel6.Size = new System.Drawing.Size(601, 87);
            this.panel6.TabIndex = 1;
            // 
            // serviceStatus
            // 
            this.serviceStatus.Dock = System.Windows.Forms.DockStyle.Fill;
            this.serviceStatus.Location = new System.Drawing.Point(6, 6);
            this.serviceStatus.Multiline = true;
            this.serviceStatus.Name = "serviceStatus";
            this.serviceStatus.ReadOnly = true;
            this.serviceStatus.ScrollBars = System.Windows.Forms.ScrollBars.Vertical;
            this.serviceStatus.Size = new System.Drawing.Size(589, 75);
            this.serviceStatus.TabIndex = 1;
            // 
            // Form1
            // 
            this.ClientSize = new System.Drawing.Size(924, 622);
            this.Controls.Add(this.panel4);
            this.Controls.Add(this.panel3);
            this.Name = "Form1";
            this.panel1.ResumeLayout(false);
            this.panel5.ResumeLayout(false);
            this.panel5.PerformLayout();
            this.panel2.ResumeLayout(false);
            this.panel3.ResumeLayout(false);
            this.panel3.PerformLayout();
            this.panel4.ResumeLayout(false);
            this.panel7.ResumeLayout(false);
            this.panel8.ResumeLayout(false);
            this.panel10.ResumeLayout(false);
            this.panel10.PerformLayout();
            this.panel11.ResumeLayout(false);
            this.panel11.PerformLayout();
            this.panel9.ResumeLayout(false);
            this.panel9.PerformLayout();
            this.panel12.ResumeLayout(false);
            this.panel12.PerformLayout();
            this.panel13.ResumeLayout(false);
            this.panel13.PerformLayout();
            this.panel6.ResumeLayout(false);
            this.panel6.PerformLayout();
            this.ResumeLayout(false);

        }

        #endregion

        private Panel panel1;
        private ListBox serviceList;
        private Label label1;
        private Button deleteServiceBtn;
        private Panel panel2;
        private Label label2;
        private Panel panel3;
        private Panel panel5;
        private Panel panel4;
        private Panel panel6;
        private ComboBox extractorsDropDown;
        private Panel panel7;
        private Label label4;
        private Panel panel8;
        private Panel panel10;
        private Label label5;
        private Panel panel11;
        private Panel panel9;
        private TextBox descriptionBox;
        private TextBox nameBox;
        private Label label6;
        private TextBox workingDirBox;
        private Label label7;
        private Panel panel12;
        private Button selectWorkingDirBtn;
        private TextBox serviceStatus;
        private Panel panel13;
        private Button addServiceBtn;
        private FolderBrowserDialog folderBrowserDialog1;
    }
}