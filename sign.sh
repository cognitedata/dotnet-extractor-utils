set -e

root=$1
assembly=$2
find . -path "$root/bin/Release/*/$assembly.dll" -exec sh -c "echo -n $CODE_SIGN_PASSWORD | sn -R {} ./cognite_code_signing.pfx" \;
