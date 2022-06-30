#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

set -x
DEST_DIR=$PWD

FUNCTION_MESH_VERSION=`${DEST_DIR}/scripts/get-function-mesh-version.sh`
BASE_DOWNLOAD_URL="https://raw.githubusercontent.com/streamnative/function-mesh/${FUNCTION_MESH_VERSION}/config/crd/bases"

CRD_FUNCTIONS_FILE=compute.functionmesh.io_functions.yaml # Target functions CRD file
CRD_SOURCES_FILE=compute.functionmesh.io_sources.yaml # Target sources CRD file
CRD_SINKS_FILE=compute.functionmesh.io_sinks.yaml # Target sinks CRD file

GEN_DIR=/tmp/functions-mesh/crd
mkdir -p $GEN_DIR
pushd $GEN_DIR

LOCAL_MANIFEST_FUNCTIONS_FILE=$GEN_DIR/$CRD_FUNCTIONS_FILE
LOCAL_MANIFEST_SOURCES_FILE=$GEN_DIR/$CRD_SOURCES_FILE
LOCAL_MANIFEST_SINKS_FILE=$GEN_DIR/$CRD_SINKS_FILE

wget $BASE_DOWNLOAD_URL/$CRD_FUNCTIONS_FILE -O $LOCAL_MANIFEST_FUNCTIONS_FILE
wget $BASE_DOWNLOAD_URL/$CRD_SOURCES_FILE -O $LOCAL_MANIFEST_SOURCES_FILE
wget $BASE_DOWNLOAD_URL/$CRD_SINKS_FILE -O $LOCAL_MANIFEST_SINKS_FILE
# yq site: https://mikefarah.gitbook.io/yq/
yq eval "del(.spec.versions.[].schema.openAPIV3Schema.x-kubernetes-preserve-unknown-fields)" -i $CRD_FUNCTIONS_FILE
yq eval "del(.spec.versions.[].schema.openAPIV3Schema.x-kubernetes-preserve-unknown-fields)" -i $CRD_SOURCES_FILE
yq eval "del(.spec.versions.[].schema.openAPIV3Schema.x-kubernetes-preserve-unknown-fields)" -i $CRD_SINKS_FILE

DEFAULT_IMAGE_NAME=ghcr.io/kubernetes-client/java/crd-model-gen
DEFAULT_IMAGE_TAG=v1.0.4
IMAGE_NAME=${IMAGE_NAME:=$DEFAULT_IMAGE_NAME}
IMAGE_TAG=${IMAGE_TAG:=$DEFAULT_IMAGE_TAG}

# Generate functions crd
docker run \
  --rm \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v "$(pwd)":"$(pwd)" \
  --network host \
  ${IMAGE_NAME}:${IMAGE_TAG} \
  /generate.sh \
  -u $LOCAL_MANIFEST_FUNCTIONS_FILE \
  -n io.functionmesh \
  -p io.functionmesh.compute.functions \
  -o "$(pwd)"

# Generate sources crd
docker run \
  --rm \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v "$(pwd)":"$(pwd)" \
  --network host \
  ${IMAGE_NAME}:${IMAGE_TAG} \
  /generate.sh \
  -u $LOCAL_MANIFEST_SOURCES_FILE \
  -n io.functionmesh \
  -p io.functionmesh.compute.sources \
  -o "$(pwd)"

# Generate sinks crd
docker run \
  --rm \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v "$(pwd)":"$(pwd)" \
  --network host \
  ${IMAGE_NAME}:${IMAGE_TAG} \
  /generate.sh \
  -u $LOCAL_MANIFEST_SINKS_FILE \
  -n io.functionmesh \
  -p io.functionmesh.compute.sinks \
  -o "$(pwd)"
#open $GEN_DIR

cp -r $GEN_DIR/src/main/java/io/functionmesh/compute/* $DEST_DIR/src/main/java/io/functionmesh/compute/
popd
