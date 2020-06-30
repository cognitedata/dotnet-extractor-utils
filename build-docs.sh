#!/bin/sh
wget https://github.com/dotnet/docfx/releases/download/v2.51/docfx.zip
unzip docfx.zip -d docfx && rm docfx.zip
mono docfx/docfx.exe docfx_project/docfx.json
rm -r docfx
