#!/usr/bin/env bash

set -ex

echo "Releasing Function Mesh Worker Service"
HOMEDIR=$PWD

version=${1#v}
if [[ "x$version" == "x" ]]; then
  echo "You need give a version number of the Function Mesh Worker Service"
  exit 1
fi

# Create a direcotry to save assets
ASSETS_DIR=release
mkdir $ASSETS_DIR

sudo wget https://github.com/mikefarah/yq/releases/download/v4.6.0/yq_linux_amd64 -O /usr/bin/yq
sudo chmod +x /usr/bin/yq
yq --help

docker login -u="${GITHUB_USERNAME}" -p="${GITHUB_TOKEN}" ghcr.io

source ./scripts/generate-crd.sh
pushd $HOMEDIR
mvn license:format
mvn clean install -DskipTests
mv target/mesh-worker-service-*.nar  ./$ASSETS_DIR
cp README.md ./$ASSETS_DIR/mesh-worker-service-readme.md
