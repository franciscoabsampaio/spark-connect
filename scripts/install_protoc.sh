#!/bin/bash
# Fetches a pinned protoc release into $2, for builds on machines without one.
# `apache-spark-connect-proto` compiles the Connect protos at build time.
set -euo pipefail

version=$1
target=$2

case "$(uname -s)" in
    Linux) os=linux ;;
    Darwin) os=osx ;;
    *) echo "No protoc build for $(uname -s); install protobuf-compiler and re-run." >&2; exit 1 ;;
esac

case "$(uname -m)" in
    x86_64 | amd64) arch=x86_64 ;;
    arm64 | aarch64) arch=aarch_64 ;;
    *) echo "No protoc build for $(uname -m); install protobuf-compiler and re-run." >&2; exit 1 ;;
esac

archive="protoc-${version}-${os}-${arch}.zip"
url="https://github.com/protocolbuffers/protobuf/releases/download/v${version}/${archive}"

echo "Fetching ${archive}"
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT

curl -sSfL -o "$scratch/protoc.zip" "$url"
unzip -q "$scratch/protoc.zip" -d "$scratch/protoc"

mkdir -p "$(dirname "$target")"
rm -rf "$target"
mv "$scratch/protoc" "$target"
"$target/bin/protoc" --version
