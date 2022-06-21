#!/usr/bin/env bash
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

set -ex

BINDIR=`dirname "$0"`
PULSAR_HOME=`cd ${BINDIR}/..;pwd`
FUNCTION_MESH_HOME=${PULSAR_HOME}
OUTPUT_BIN=${FUNCTION_MESH_HOME}/output/bin
KIND_BIN=$OUTPUT_BIN/kind
HELM=${OUTPUT_BIN}/helm
KUBECTL=${OUTPUT_BIN}/kubectl
NAMESPACE=default
CLUSTER=sn-platform
CLUSTER_ID=$(uuidgen | tr "[:upper:]" "[:lower:]")

FUNCTION_NAME=$1

function ci::create_cluster() {
    echo "Creating a kind cluster ..."
    ${FUNCTION_MESH_HOME}/hack/kind-cluster-build.sh --name sn-platform-${CLUSTER_ID} -c 3 -v 10
    echo "Successfully created a kind cluster."
}

function ci::delete_cluster() {
    echo "Deleting a kind cluster ..."
    kind delete cluster --name=sn-platform-${CLUSTER_ID}
    echo "Successfully delete a kind cluster."
}

function ci::cleanup() {
    echo "Print events logs ..."
    ${KUBECTL} -n default get events --sort-by='{.lastTimestamp}'

    echo "Clean up kind clusters ..."
    clusters=( $(kind get clusters | grep sn-platform) )
    for cluster in "${clusters[@]}"
    do
       echo "Deleting a kind cluster ${cluster}"
       kind delete cluster --name=${cluster}
    done
    echo "Successfully clean up a kind cluster."
}

function ci::install_storage_provisioner() {
    echo "Installing the local storage provisioner ..."
    ${HELM} repo add streamnative https://charts.streamnative.io
    ${HELM} repo update
    ${HELM} install local-storage-provisioner streamnative/local-storage-provisioner --debug --wait --set namespace=default
    echo "Successfully installed the local storage provisioner."
}

function ci::install_metrics_server() {
    echo "install metrics-server"
    ${KUBECTL} apply -f https://github.com/kubernetes-sigs/metrics-server/releases/download/v0.3.7/components.yaml
    ${KUBECTL} patch deployment metrics-server -n kube-system -p '{"spec":{"template":{"spec":{"containers":[{"name":"metrics-server","args":["--cert-dir=/tmp", "--secure-port=4443", "--kubelet-insecure-tls","--kubelet-preferred-address-types=InternalIP"]}]}}}}'
    echo "Successfully installed the metrics-server."
    WC=$(${KUBECTL} get pods -n kube-system --field-selector=status.phase=Running | grep metrics-server | wc -l)
    while [[ ${WC} -lt 1 ]]; do
      echo ${WC};
      sleep 20
      ${KUBECTL} get pods -n kube-system
      WC=$(${KUBECTL} get pods -n kube-system --field-selector=status.phase=Running | grep metrics-server | wc -l)
    done
}

function ci::install_cert_manager_charts() {
    echo "Installing the cert manager charts ..."
    ${HELM} repo add jetstack https://charts.jetstack.io
    ${HELM} repo update
    ${HELM} install cert-manager jetstack/cert-manager --set installCRDs=true
    echo "wait until cert-manager is alive"
    ${KUBECTL} wait --for condition=available --timeout=360s deployment/cert-manager
}

function ci::install_pulsar_charts() {
    echo "Installing the pulsar charts ..."
    values=${1:-".ci/clusters/values.yaml"}
    echo $values
    if [ "$values" = ".ci/clusters/values_mesh_worker_service.yaml" ]; then
      echo "load mesh-worker-service-integration-pulsar:latest ..."
      kind load docker-image mesh-worker-service-integration-pulsar:latest --name sn-platform-${CLUSTER_ID}
    fi
    if [ -d "pulsar-charts" ]; then
      rm -rf pulsar-charts
    fi
    git clone https://github.com/streamnative/charts.git pulsar-charts
    cp ${values} pulsar-charts/charts/pulsar/mini_values.yaml
    cd pulsar-charts
    cd charts
    helm repo add loki https://grafana.github.io/loki/charts
    helm dependency update pulsar
    ${HELM} install sn-platform --values ./pulsar/mini_values.yaml ./pulsar --debug

    echo "wait until broker is alive"
    WC=$(${KUBECTL} get pods -n ${NAMESPACE} --field-selector=status.phase=Running | grep ${CLUSTER}-pulsar-broker | wc -l)
    while [[ ${WC} -lt 1 ]]; do
      echo ${WC};
      sleep 20
      ${KUBECTL} get pods -n ${NAMESPACE}
      WC=$(${KUBECTL} get pods -n ${NAMESPACE} | grep ${CLUSTER}-pulsar-broker | wc -l)
      if [[ ${WC} -gt 1 ]]; then
        ${KUBECTL} describe pod -n ${NAMESPACE} ${CLUSTER}-pulsar-broker-0
        ${KUBECTL} describe pod -n ${NAMESPACE} ${CLUSTER}-pulsar-bookie-0
      fi
      WC=$(${KUBECTL} get pods -n ${NAMESPACE} --field-selector=status.phase=Running | grep ${CLUSTER}-pulsar-broker | wc -l)
    done

    ${KUBECTL} get service -n ${NAMESPACE}
    cd ../../
}

function ci::install_function_mesh_charts() {
  echo "Installing the function mesh charts ..."
  FMV=`${FUNCTION_MESH_HOME}/scripts/get-function-mesh-version.sh`
  echo "function mesh version $FMV"
  if [ -d "function-mesh" ]; then
    rm -rf function-mesh
  fi
  git clone --branch ${FMV} https://github.com/streamnative/function-mesh function-mesh
  cd function-mesh/charts/
  helm dependency update ./function-mesh-operator
  ${HELM} install function-mesh --values ./function-mesh-operator/values.yaml ./function-mesh-operator

  echo "wait until controller-manager is alive"
  ${KUBECTL} get deployment -n ${NAMESPACE}
  sleep 120
  ${KUBECTL} -n ${NAMESPACE} logs deployment/function-mesh-controller-manager
  ${KUBECTL} wait --for condition=available --timeout=360s deployment/function-mesh-controller-manager -n ${NAMESPACE}

  cd ../../
}

function ci::test_pulsar_producer() {
    sleep 120
    ${KUBECTL} exec -n ${NAMESPACE} ${CLUSTER}-pulsar-toolset-0 -- bash -c 'until nslookup sn-platform-pulsar-broker; do sleep 3; done'
    ${KUBECTL} exec -n ${NAMESPACE} ${CLUSTER}-pulsar-bookie-0 -- df -h
    ${KUBECTL} exec -n ${NAMESPACE} ${CLUSTER}-pulsar-bookie-0 -- cat conf/bookkeeper.conf
    ${KUBECTL} exec -n ${NAMESPACE} ${CLUSTER}-pulsar-broker-0 -- bin/pulsar-admin tenants create sn-platform
    ${KUBECTL} exec -n ${NAMESPACE} ${CLUSTER}-pulsar-broker-0 -- bin/pulsar-admin namespaces create sn-platform/test
    ${KUBECTL} exec -n ${NAMESPACE} ${CLUSTER}-pulsar-broker-0 -- bin/pulsar-client produce -m "test-message" sn-platform/test/test-topic
}

function ci::verify_function_mesh() {
    FUNCTION_NAME=$1
    WC=$(${KUBECTL} get pods -lname=${FUNCTION_NAME} --field-selector=status.phase=Running | wc -l)
    while [[ ${WC} -lt 1 ]]; do
      echo ${WC};
      sleep 15
      ${KUBECTL} get pods -A
      WC=$(${KUBECTL} get pods -lname=${FUNCTION_NAME} | wc -l)
      if [[ ${WC} -gt 1 ]]; then
        ${KUBECTL} describe pod -lname=${FUNCTION_NAME}
      fi
      WC=$(${KUBECTL} get pods -lname=${FUNCTION_NAME} --field-selector=status.phase=Running | wc -l)
    done
    ${KUBECTL} describe pod -lname=${FUNCTION_NAME}
    ${KUBECTL} logs -lname=${FUNCTION_NAME}  --all-containers=true
}

function ci::verify_hpa() {
    FUNCTION_NAME=$1
    ${KUBECTL} get function
    ${KUBECTL} get function ${FUNCTION_NAME} -o yaml
    ${KUBECTL} get hpa.v2beta2.autoscaling
    ${KUBECTL} get hpa.v2beta2.autoscaling ${FUNCTION_NAME}-function -o yaml
    ${KUBECTL} describe hpa.v2beta2.autoscaling ${FUNCTION_NAME}-function
    WC=$(${KUBECTL} get hpa.v2beta2.autoscaling ${FUNCTION_NAME}-function -o jsonpath='{.status.conditions[?(@.type=="AbleToScale")].status}' | grep False | wc -l)
    while [[ ${WC} -lt 0 ]]; do
      echo ${WC};
      sleep 20
      ${KUBECTL} get hpa.v2beta2.autoscaling ${FUNCTION_NAME}-function -o yaml
      ${KUBECTL} describe hpa.v2beta2.autoscaling ${FUNCTION_NAME}-function
      ${KUBECTL} get hpa.v2beta2.autoscaling ${FUNCTION_NAME}-function -o jsonpath='{.status.conditions[?(@.type=="AbleToScale")].status}'
      WC=$(${KUBECTL} get hpa.v2beta2.autoscaling ${FUNCTION_NAME}-function -o jsonpath='{.status.conditions[?(@.type=="AbleToScale")].status}' | grep False | wc -l)
    done
}

function ci::test_function_runners() {
    ${KUBECTL} exec -n ${NAMESPACE} ${CLUSTER}-pulsar-broker-0 -- bin/pulsar-admin functions create --tenant public --namespace default --name test-java --className org.apache.pulsar.functions.api.examples.ExclamationFunction --inputs persistent://public/default/test-java-input --jar /pulsar/examples/api-examples.jar --cpu 0.1
    sleep 15
    ${KUBECTL} get pods -A
    sleep 5
    WC=$(${KUBECTL} get pods -n ${NAMESPACE} --field-selector=status.phase=Running | grep "test-java" | wc -l)
    while [[ ${WC} -lt 1 ]]; do
      echo ${WC};
      sleep 20
      ${KUBECTL} get pods -n ${NAMESPACE}
      ${KUBECTL} describe pod pf-public-default-test-java-0 
      WC=$(${KUBECTL} get pods -n ${NAMESPACE} --field-selector=status.phase=Running | grep "test-java" | wc -l)
    done
    echo "java runner test done"
    ${KUBECTL} exec -n ${NAMESPACE} ${CLUSTER}-pulsar-broker-0 -- bin/pulsar-admin functions delete --tenant public --namespace default --name test-java

    ${KUBECTL} exec -n ${NAMESPACE} ${CLUSTER}-pulsar-broker-0 -- bin/pulsar-admin functions create --tenant public --namespace default --name test-python --classname exclamation_function.ExclamationFunction --inputs persistent://public/default/test-python-input --py /pulsar/examples/python-examples/exclamation_function.py --cpu 0.1
    sleep 15
    ${KUBECTL} get pods -A
    sleep 5
    WC=$(${KUBECTL} get pods -n ${NAMESPACE} --field-selector=status.phase=Running | grep "test-python" | wc -l)
    while [[ ${WC} -lt 1 ]]; do
      echo ${WC};
      sleep 20
      ${KUBECTL} get pods -n ${NAMESPACE}
      WC=$(${KUBECTL} get pods -n ${NAMESPACE} --field-selector=status.phase=Running | grep "test-python" | wc -l)
    done
    echo "python runner test done"
    ${KUBECTL} exec -n ${NAMESPACE} ${CLUSTER}-pulsar-broker-0 -- bin/pulsar-admin functions delete --tenant public --namespace default --name test-python

    ${KUBECTL} cp "${FUNCTION_MESH_HOME}/.ci/examples/go-examples" "${NAMESPACE}/${CLUSTER}-pulsar-broker-0:/pulsar/examples"
    sleep 1
    ${KUBECTL} exec -n ${NAMESPACE} ${CLUSTER}-pulsar-broker-0 -- bin/pulsar-admin functions create --tenant public --namespace default --name test-go --inputs persistent://public/default/test-go-input --go /pulsar/examples/go-examples/exclamationFunc --cpu 0.1
    sleep 15
    ${KUBECTL} get pods -A
    sleep 5
    WC=$(${KUBECTL} get pods -n ${NAMESPACE} --field-selector=status.phase=Running | grep "test-go" | wc -l)
    while [[ ${WC} -lt 1 ]]; do
      echo ${WC};
      sleep 20
      ${KUBECTL} get pods -n ${NAMESPACE}
      WC=$(${KUBECTL} get pods -n ${NAMESPACE} --field-selector=status.phase=Running | grep "test-go" | wc -l)
    done
    echo "golang runner test done"
    ${KUBECTL} exec -n ${NAMESPACE} ${CLUSTER}-pulsar-broker-0 -- bin/pulsar-admin functions delete --tenant public --namespace default --name test-go
}

function ci::verify_go_function() {
    FUNCTION_NAME=$1
    ${KUBECTL} describe pod -lname=${FUNCTION_NAME}
    ${KUBECTL} logs -lname=${FUNCTION_NAME}  --all-containers=true
    ci:verify_exclamation_function "persistent://public/default/input-go-topic" "persistent://public/default/output-go-topic" "test-message" "test-message!" 30
}

function ci::verify_java_function() {
    FUNCTION_NAME=$1
    ${KUBECTL} describe pod -lname=${FUNCTION_NAME}
    sleep 120
    ${KUBECTL} logs -lname=${FUNCTION_NAME}  --all-containers=true
    ci:verify_exclamation_function "persistent://public/default/input-java-topic" "persistent://public/default/output-java-topic" "test-message" "test-message!" 30
}

function ci::verify_python_function() {
    FUNCTION_NAME=$1
    ${KUBECTL} describe pod -lname=${FUNCTION_NAME}
    ${KUBECTL} logs -lname=${FUNCTION_NAME}  --all-containers=true
    ci:verify_exclamation_function "persistent://public/default/input-python-topic" "persistent://public/default/output-python-topic" "test-message" "test-message!" 30
}

function ci::verify_mesh_function() {
    ci:verify_exclamation_function "persistent://public/default/functionmesh-input-topic" "persistent://public/default/functionmesh-python-topic" "test-message" "test-message!!!" 120
}

function ci::print_function_log() {
    FUNCTION_NAME=$1
    ${KUBECTL} describe pod -lname=${FUNCTION_NAME}
    sleep 120
    ${KUBECTL} logs -lname=${FUNCTION_NAME}  --all-containers=true
}

function ci:verify_exclamation_function() {
  inputtopic=$1
  outputtopic=$2
  inputmessage=$3
  outputmessage=$4
  timesleep=$5
  ${KUBECTL} exec -n ${NAMESPACE} ${CLUSTER}-pulsar-broker-0 -- bin/pulsar-client produce -m ${inputmessage} -n 10 ${inputtopic}
  sleep $timesleep
  MESSAGE=$(timeout 120 ${KUBECTL} exec -n ${NAMESPACE} ${CLUSTER}-pulsar-broker-0 -- bin/pulsar-client consume -n 1 -s "sub" --subscription-position Earliest ${outputtopic})
  echo $MESSAGE
  if [[ "$MESSAGE" == *"$outputmessage"* ]]; then
    return 0
  fi
  return 1
}

function ci::ensure_mesh_worker_service_role() {
  ${KUBECTL} create clusterrolebinding broker-acct-manager-role-binding --clusterrole=function-mesh-function-mesh-controller-manager --serviceaccount=default:sn-platform-pulsar-broker-acct
}

function ci::ensure_function_mesh_config() {
  ${KUBECTL} apply -f ${FUNCTION_MESH_HOME}/.ci/clusters/mesh_worker_service_integration_test_pulsar_config.yaml
}

function ci::verify_mesh_worker_service_pulsar_admin() {
  ${KUBECTL} exec -n ${NAMESPACE} ${CLUSTER}-pulsar-broker-0 -- bin/pulsar-admin sinks available-sinks
  RET=$(${KUBECTL} exec -n ${NAMESPACE} ${CLUSTER}-pulsar-broker-0 -- bin/pulsar-admin sinks available-sinks)
  if [[ $RET != *"data-generator"* ]]; then
   return 1
  fi
  ${KUBECTL} exec -n ${NAMESPACE} ${CLUSTER}-pulsar-broker-0 -- bin/pulsar-admin sources available-sources
  RET=$(${KUBECTL} exec -n ${NAMESPACE} ${CLUSTER}-pulsar-broker-0 -- bin/pulsar-admin sources available-sources)
  if [[ $RET != *"data-generator"* ]]; then
   return 1
  fi
  echo "test create data-generator sink and source"
  RET=$(${KUBECTL} exec -n ${NAMESPACE} ${CLUSTER}-pulsar-broker-0 -- bin/pulsar-admin sources create --name data-generator-source --source-type data-generator --destination-topic-name persistent://public/default/random-data-topic --custom-runtime-options '{"outputTypeClassName": "org.apache.pulsar.io.datagenerator.Person"}' --source-config '{"sleepBetweenMessages": "1000"}')
  echo $RET
  if [[ $RET != *"successfully"* ]]; then
   return 1
  fi
  RET=$(${KUBECTL} exec -n ${NAMESPACE} ${CLUSTER}-pulsar-broker-0 -- bin/pulsar-admin sinks create --name data-generator-sink --sink-type data-generator --inputs persistent://public/default/random-data-topic --custom-runtime-options '{"inputTypeClassName": "org.apache.pulsar.io.datagenerator.Person"}')
  echo "${RET}"
  if [[ "${RET}" != *"successfully"* ]]; then
   return 1
  fi
  ${KUBECTL} get pods -n ${NAMESPACE}
  sleep 120
  ${KUBECTL} get pods -n ${NAMESPACE} --field-selector=status.phase=Running | grep "data-generator-source"
  WC=$(${KUBECTL} get pods -n ${NAMESPACE} --field-selector=status.phase=Running | grep "data-generator-sink" | wc -l)
  while [[ ${WC} -lt 1 ]]; do
    echo ${WC};
    sleep 20
    ${KUBECTL} get pods -n ${NAMESPACE}
    WC=$(${KUBECTL} get pods -n ${NAMESPACE} --field-selector=status.phase=Running | grep "data-generator-sink" | wc -l)
  done
  ${KUBECTL} get pods -n ${NAMESPACE} --field-selector=status.phase=Running | grep "data-generator-source"
  WC=$(${KUBECTL} get pods -n ${NAMESPACE} --field-selector=status.phase=Running | grep "data-generator-source" | wc -l)
  while [[ ${WC} -lt 1 ]]; do
    echo ${WC};
    sleep 20
    ${KUBECTL} get pods -n ${NAMESPACE}
    WC=$(${KUBECTL} get pods -n ${NAMESPACE} --field-selector=status.phase=Running | grep "data-generator-source" | wc -l)
  done
  sleep 120
  ${KUBECTL} exec -n ${NAMESPACE} ${CLUSTER}-pulsar-broker-0 -- bin/pulsar-admin sinks status --name data-generator-sink
  RET=$(${KUBECTL} exec -n ${NAMESPACE} ${CLUSTER}-pulsar-broker-0 -- bin/pulsar-admin sinks status --name data-generator-sink)
  if [[ $RET != *"true"* ]]; then
   return 1
  fi
  ${KUBECTL} exec -n ${NAMESPACE} ${CLUSTER}-pulsar-broker-0 -- bin/pulsar-admin sources status --name data-generator-source
  RET=$(${KUBECTL} exec -n ${NAMESPACE} ${CLUSTER}-pulsar-broker-0 -- bin/pulsar-admin sources status --name data-generator-source)
  if [[ $RET != *"true"* ]]; then
   return 1
  fi

  ${KUBECTL} get pods -n ${NAMESPACE}
  RET=$(${KUBECTL} exec -n ${NAMESPACE} ${CLUSTER}-pulsar-broker-0 -- bin/pulsar-admin sources delete --name data-generator-source)
  echo "${RET}"
  RET=$(${KUBECTL} exec -n ${NAMESPACE} ${CLUSTER}-pulsar-broker-0 -- bin/pulsar-admin sinks delete --name data-generator-sink)
  echo "${RET}"
  ${KUBECTL} get pods -n ${NAMESPACE}
  echo " === verify mesh worker service with empty connector config"
  RET=$(${KUBECTL} exec -n ${NAMESPACE} ${CLUSTER}-pulsar-broker-0 -- bin/pulsar-admin sources create --name data-generator-source --source-type data-generator --destination-topic-name persistent://public/default/random-data-topic --custom-runtime-options '{"outputTypeClassName": "org.apache.pulsar.io.datagenerator.Person"}')
  echo "${RET}"
  if [[ $RET != *"successfully"* ]]; then
   return 1
  fi
  WC=$(${KUBECTL} get pods -n ${NAMESPACE} --field-selector=status.phase=Running | grep "data-generator-source" | wc -l)
  while [[ ${WC} -lt 1 ]]; do
    echo ${WC};
    sleep 20
    ${KUBECTL} get pods -n ${NAMESPACE}
    WC=$(${KUBECTL} get pods -n ${NAMESPACE} --field-selector=status.phase=Running | grep "data-generator-source" | wc -l)
  done
  ${KUBECTL} exec -n ${NAMESPACE} ${CLUSTER}-pulsar-broker-0 -- bin/pulsar-admin sources status --name data-generator-source
  RET=$(${KUBECTL} exec -n ${NAMESPACE} ${CLUSTER}-pulsar-broker-0 -- bin/pulsar-admin sources status --name data-generator-source)
  if [[ $RET != *"true"* ]]; then
    ${KUBECTL} logs -n ${NAMESPACE} data-generator-source-69865103-source-0
    ${KUBECTL} get pods data-generator-source-69865103-source-0 -o yaml
   return 1
  fi
  ${KUBECTL} get pods -n ${NAMESPACE}
  RET=$(${KUBECTL} exec -n ${NAMESPACE} ${CLUSTER}-pulsar-broker-0 -- bin/pulsar-admin sources delete --name data-generator-source)
  echo "${RET}"
  ${KUBECTL} get pods -n ${NAMESPACE}

}

function ci::upload_java_package() {

  RET=$(${KUBECTL} exec -n ${NAMESPACE} ${CLUSTER}-pulsar-broker-0 -- bin/pulsar-admin packages upload function://public/default/java-function@1.0 --path /pulsar/examples/api-examples.jar --description java-function@1.0)
  if [[ $RET != *"successfully"* ]]; then
    echo "${RET}"
    ${KUBECTL} logs -n ${NAMESPACE} ${CLUSTER}-pulsar-broker-0
    sleep 60
    return 1
  fi
  RET=$(${KUBECTL} exec -n ${NAMESPACE} ${CLUSTER}-pulsar-broker-0 -- bin/pulsar-admin packages download function://public/default/java-function@1.0 --path /pulsar/api-examples.jar)
  if [[ $RET != *"successfully"* ]]; then
    echo "${RET}"
    ${KUBECTL} logs -n ${NAMESPACE} ${CLUSTER}-pulsar-broker-0
    sleep 60
    return 1
  fi
}

function ci::verify_java_package() {
  RET=$(${KUBECTL} exec -n ${NAMESPACE} ${CLUSTER}-pulsar-broker-0 -- bin/pulsar-admin functions create --jar function://public/default/java-function@1.0 --name package-java-fn --className org.apache.pulsar.functions.api.examples.ExclamationFunction --inputs persistent://public/default/package-java-fn-input --cpu 0.1)
  ${KUBECTL} logs -n ${NAMESPACE} ${CLUSTER}-pulsar-broker-0
  sleep 15
  echo "${RET}"
  ${KUBECTL} logs -n ${NAMESPACE} ${CLUSTER}-pulsar-broker-0
  sleep 15
  ${KUBECTL} get pods -A
  sleep 5
  WC=$(${KUBECTL} get pods -n ${NAMESPACE} --field-selector=status.phase=Running | grep "package-java-fn" | wc -l)
  while [[ ${WC} -lt 1 ]]; do
    echo ${WC};
    sleep 20
    ${KUBECTL} get pods -n ${NAMESPACE}
    RET=$(${KUBECTL} get pods -n ${NAMESPACE} -o name | grep package-java-fn)
    ${KUBECTL} describe ${RET}
    WC=$(${KUBECTL} get pods -n ${NAMESPACE} --field-selector=status.phase=Running | grep "package-java-fn" | wc -l)
  done
  echo "java function test done"
  RET=$(${KUBECTL} exec -n ${NAMESPACE} ${CLUSTER}-pulsar-broker-0 -- bin/pulsar-admin functions delete --name package-java-fn)
  echo "${RET}"
}

function ci::upload_python_package() {
  RET=$(${KUBECTL} exec -n ${NAMESPACE} ${CLUSTER}-pulsar-broker-0 -- bin/pulsar-admin packages upload function://public/default/python-function@1.0 --path /pulsar/examples/python-examples/exclamation_function.py --description python-function@1.0)
  if [[ $RET != *"successfully"* ]]; then
    echo "${RET}"
    return 1
  fi
}

function ci::verify_python_package() {
  RET=$(${KUBECTL} exec -n ${NAMESPACE} ${CLUSTER}-pulsar-broker-0 -- bin/pulsar-admin functions create --py function://public/default/python-function@1.0 --name package-python-fn --classname exclamation_function.ExclamationFunction --inputs persistent://public/default/package-python-fn-input --cpu 0.1)
  if [[ $RET != *"successfully"* ]]; then
    echo "${RET}"
    return 1
  fi
  sleep 15
  ${KUBECTL} get pods -A
  sleep 5
  WC=$(${KUBECTL} get pods -n ${NAMESPACE} --field-selector=status.phase=Running | grep "package-python-fn" | wc -l)
  while [[ ${WC} -lt 1 ]]; do
    echo ${WC};
    sleep 20
    ${KUBECTL} get pods -n ${NAMESPACE}
    RET=$(${KUBECTL} get pods -n ${NAMESPACE} -o name | grep package-python-fn)
    ${KUBECTL} describe ${RET}
    WC=$(${KUBECTL} get pods -n ${NAMESPACE} --field-selector=status.phase=Running | grep "package-python-fn" | wc -l)
  done
  echo "python function test done"
  RET=$(${KUBECTL} exec -n ${NAMESPACE} ${CLUSTER}-pulsar-broker-0 -- bin/pulsar-admin functions delete --name package-python-fn)
  echo "${RET}"
}

function ci::upload_go_package() {
  ${KUBECTL} cp "${FUNCTION_MESH_HOME}/.ci/examples/go-examples" "${NAMESPACE}/${CLUSTER}-pulsar-broker-0:/pulsar/examples"
  sleep 1
  ${KUBECTL} exec -n ${NAMESPACE} ${CLUSTER}-pulsar-broker-0 -- ls -l /pulsar/examples/go-examples
  sleep 1
  RET=$(${KUBECTL} exec -n ${NAMESPACE} ${CLUSTER}-pulsar-broker-0 -- bin/pulsar-admin packages upload function://public/default/go-function@1.0 --path /pulsar/examples/go-examples/exclamationFunc --description go-function@1.0)
  if [[ $RET != *"successfully"* ]]; then
    echo "${RET}"
    return 1
  fi
}

function ci::verify_go_package() {
  RET=$(${KUBECTL} exec -n ${NAMESPACE} ${CLUSTER}-pulsar-broker-0 -- bin/pulsar-admin functions create --go function://public/default/go-function@1.0 --name package-go-fn --inputs persistent://public/default/package-go-fn-input --cpu 0.1)
  if [[ $RET != *"successfully"* ]]; then
    echo "${RET}"
    return 1
  fi
  sleep 15
  ${KUBECTL} get pods -A
  sleep 5
  WC=$(${KUBECTL} get pods -n ${NAMESPACE} --field-selector=status.phase=Running | grep "package-go-fn" | wc -l)
  while [[ ${WC} -lt 1 ]]; do
    echo ${WC};
    sleep 20
    ${KUBECTL} get pods -n ${NAMESPACE}
    RET=$(${KUBECTL} get pods -n ${NAMESPACE} -o name | grep package-go-fn)
    ${KUBECTL} describe ${RET}
    WC=$(${KUBECTL} get pods -n ${NAMESPACE} --field-selector=status.phase=Running | grep "package-go-fn" | wc -l)
  done
  echo "go function test done"
  RET=$(${KUBECTL} exec -n ${NAMESPACE} ${CLUSTER}-pulsar-broker-0 -- bin/pulsar-admin functions delete --name package-go-fn)
  echo "${RET}"
}

function ci::create_java_function_by_upload() {
  ${KUBECTL} exec -n ${NAMESPACE} ${CLUSTER}-pulsar-broker-0 -- cat conf/functions_worker.yml
  RET=$(${KUBECTL} exec -n ${NAMESPACE} ${CLUSTER}-pulsar-broker-0 -- bin/pulsar-admin functions create --jar /pulsar/examples/api-examples.jar --name package-upload-java-fn --className org.apache.pulsar.functions.api.examples.ExclamationFunction --inputs persistent://public/default/package-upload-java-fn-input --cpu 0.1)
  ${KUBECTL} logs -n ${NAMESPACE} ${CLUSTER}-pulsar-broker-0
  sleep 15
  echo "${RET}"
  ${KUBECTL} logs -n ${NAMESPACE} ${CLUSTER}-pulsar-broker-0
  sleep 15
  ${KUBECTL} get pods -A
  sleep 5
  WC=$(${KUBECTL} get pods -n ${NAMESPACE} --field-selector=status.phase=Running | grep "package-upload-java-fn" | wc -l)
  while [[ ${WC} -lt 1 ]]; do
    echo ${WC};
    sleep 20
    ${KUBECTL} get pods -n ${NAMESPACE}
    ${KUBECTL} describe pod package-upload-java-fn-function-0
    WC=$(${KUBECTL} get pods -n ${NAMESPACE} --field-selector=status.phase=Running | grep "package-upload-java-fn" | wc -l)
  done
  echo "java function test done"
  RET=$(${KUBECTL} exec -n ${NAMESPACE} ${CLUSTER}-pulsar-broker-0 -- bin/pulsar-admin functions delete --name package-upload-java-fn)
  echo "${RET}"
  if ${KUBECTL} exec -n ${NAMESPACE} ${CLUSTER}-pulsar-broker-0 -- bin/pulsar-admin packages get-metadata function://public/default/package-upload-java-fn@latest; then
    RET=$(${KUBECTL} exec -n ${NAMESPACE} ${CLUSTER}-pulsar-broker-0 -- bin/pulsar-admin packages get-metadata function://public/default/package-upload-java-fn@latest)
    echo "${RET}"
  fi
}

function ci::verify_secrets_python_package() {
  ${KUBECTL} apply -f ${FUNCTION_MESH_HOME}/.ci/examples/secret-py-example/secrets_python_secret.yaml
  sleep 10

  ${KUBECTL} cp "${FUNCTION_MESH_HOME}/.ci/examples/secret-py-example" "${NAMESPACE}/${CLUSTER}-pulsar-broker-0:/pulsar/examples/secret-py-example"
  sleep 10

  RET=$(${KUBECTL} exec -n ${NAMESPACE} ${CLUSTER}-pulsar-broker-0 -- bin/pulsar-admin functions create --py /pulsar/examples/secret-py-example/secretsfunction.py --name package-python-secret-fn --classname secretsfunction.SecretsFunction --inputs persistent://public/default/package-python-secret-fn-input --output persistent://public/default/package-python-secret-fn-output --subs-position "Earliest" --cpu 0.1 --secrets '{"APPEND_VALUE":{"path":"test-python-secret","key":"append_value"}}')
  if [[ $RET != *"successfully"* ]]; then
    echo "${RET}"
    return 1
  fi
  sleep 15
  ${KUBECTL} get pods -A
  sleep 5
  WC=$(${KUBECTL} get pods -n ${NAMESPACE} --field-selector=status.phase=Running | grep "package-python-secret-fn" | wc -l)
  while [[ ${WC} -lt 1 ]]; do
    echo ${WC};
    sleep 20
    ${KUBECTL} get pods -n ${NAMESPACE}
    RET=$(${KUBECTL} get pods -n ${NAMESPACE} -o name | grep package-python-secret-fn)
    ${KUBECTL} describe ${RET}
    WC=$(${KUBECTL} get pods -n ${NAMESPACE} --field-selector=status.phase=Running | grep "package-python-secret-fn" | wc -l)
  done

  sleep 120
  ${KUBECTL} get pods -A
  RET=$(${KUBECTL} get pods -n ${NAMESPACE} -o name | grep package-python-secret-fn)
  ${KUBECTL} logs ${RET}

  ci:verify_exclamation_function "persistent://public/default/package-python-secret-fn-input" "persistent://public/default/package-python-secret-fn-output" "test-message" "test-message!" 120

  echo "python function test done"
  RET=$(${KUBECTL} exec -n ${NAMESPACE} ${CLUSTER}-pulsar-broker-0 -- bin/pulsar-admin functions delete --name package-python-secret-fn)
  echo "${RET}"
}

function ci::create_source_by_upload() {
  ${KUBECTL} exec -n ${NAMESPACE} ${CLUSTER}-pulsar-broker-0 -- cat conf/functions_worker.yml
  PULSAR_IO_DATA_GENERATOR=$(${KUBECTL} exec -n ${NAMESPACE} ${CLUSTER}-pulsar-broker-0 -- ls connectors | grep pulsar-io-data-generator)
  RET=$(${KUBECTL} exec -n ${NAMESPACE} ${CLUSTER}-pulsar-broker-0 -- bin/pulsar-admin sources create -a /pulsar/connectors/${PULSAR_IO_DATA_GENERATOR} --name package-upload-source --destination-topic-name persistent://public/default/package-upload-connector-topic --custom-runtime-options '{"outputTypeClassName": "java.nio.ByteBuffer"}')
  ${KUBECTL} logs -n ${NAMESPACE} ${CLUSTER}-pulsar-broker-0
  sleep 15
  echo "${RET}"
  ${KUBECTL} logs -n ${NAMESPACE} ${CLUSTER}-pulsar-broker-0
  sleep 15
  ${KUBECTL} get pods -A
  sleep 5
  WC=$(${KUBECTL} get pods -n ${NAMESPACE} --field-selector=status.phase=Running | grep "package-upload-source" | wc -l)
  while [[ ${WC} -lt 1 ]]; do
    echo ${WC};
    sleep 20
    ${KUBECTL} get pods -n ${NAMESPACE}
    ${KUBECTL} describe pod package-upload-source-2c1626bf-source-0
    ${KUBECTL} get pod package-upload-source-2c1626bf-source-0 -o yaml
    WC=$(${KUBECTL} get pods -n ${NAMESPACE} --field-selector=status.phase=Running | grep "package-upload-source" | wc -l)
  done
  RESOURCENAME=$(${KUBECTL} get sources --no-headers -o custom-columns=":metadata.name" | grep "package-upload-source")
  echo "${RESOURCENAME}"
  RET=$(${KUBECTL} get sources ${RESOURCENAME} -o json | jq .spec.className)
  echo "${RET}"
  [[ -z "${RET}" ]] && { echo "className is empty" ; exit 1; }
  echo "source test done"
  RET=$(${KUBECTL} exec -n ${NAMESPACE} ${CLUSTER}-pulsar-broker-0 -- bin/pulsar-admin sources delete --name package-upload-source)
  echo "${RET}"
  if ${KUBECTL} exec -n ${NAMESPACE} ${CLUSTER}-pulsar-broker-0 -- bin/pulsar-admin packages get-metadata source://public/default/package-upload-source@latest; then
    RET=$(${KUBECTL} exec -n ${NAMESPACE} ${CLUSTER}-pulsar-broker-0 -- bin/pulsar-admin packages get-metadata source://public/default/package-upload-source@latest)
    echo "${RET}"
  fi
}

function ci::create_sink_by_upload() {
  PULSAR_IO_DATA_GENERATOR=$(${KUBECTL} exec -n ${NAMESPACE} ${CLUSTER}-pulsar-broker-0 -- ls connectors | grep pulsar-io-data-generator)
  RET=$(${KUBECTL} exec -n ${NAMESPACE} ${CLUSTER}-pulsar-broker-0 -- bin/pulsar-admin sinks create -a /pulsar/connectors/${PULSAR_IO_DATA_GENERATOR} --name package-upload-sink --inputs persistent://public/default/package-upload-connector-topic --custom-runtime-options '{"inputTypeClassName": "org.apache.pulsar.io.datagenerator.Person"}')
  ${KUBECTL} logs -n ${NAMESPACE} ${CLUSTER}-pulsar-broker-0
  sleep 15
  echo "${RET}"
  ${KUBECTL} logs -n ${NAMESPACE} ${CLUSTER}-pulsar-broker-0
  sleep 15
  ${KUBECTL} get pods -A
  sleep 5
  WC=$(${KUBECTL} get pods -n ${NAMESPACE} --field-selector=status.phase=Running | grep "package-upload-sink" | wc -l)
  while [[ ${WC} -lt 1 ]]; do
    echo ${WC};
    sleep 20
    ${KUBECTL} get pods -n ${NAMESPACE}
    ${KUBECTL} describe pod package-upload-sink-21a402bf-sink-0
    WC=$(${KUBECTL} get pods -n ${NAMESPACE} --field-selector=status.phase=Running | grep "package-upload-sink" | wc -l)
  done
  RESOURCENAME=$(${KUBECTL} get sinks --no-headers -o custom-columns=":metadata.name" | grep "package-upload-sink")
  echo "${RESOURCENAME}"
  RET=$(${KUBECTL} get sinks ${RESOURCENAME} -o json | jq .spec.className)
  echo "${RET}"
  [[ -z "${RET}" ]] && { echo "className is empty" ; exit 1; }
  echo "sink test done"
  RET=$(${KUBECTL} exec -n ${NAMESPACE} ${CLUSTER}-pulsar-broker-0 -- bin/pulsar-admin sinks delete --name package-upload-sink)
  echo "${RET}"
  if ${KUBECTL} exec -n ${NAMESPACE} ${CLUSTER}-pulsar-broker-0 -- bin/pulsar-admin packages get-metadata sink://public/default/package-upload-sink@latest; then
    RET=$(${KUBECTL} exec -n ${NAMESPACE} ${CLUSTER}-pulsar-broker-0 -- bin/pulsar-admin packages get-metadata sink://public/default/package-upload-sink@latest)
    echo "${RET}"
  fi
}

function ci::verify_function_stats_api() {
  echo "create Java function"
  RET=$(${KUBECTL} exec -n ${NAMESPACE} ${CLUSTER}-pulsar-broker-0 -- bin/pulsar-admin functions create --jar /pulsar/examples/api-examples.jar --name api-java-fn --className org.apache.pulsar.functions.api.examples.ExclamationFunction --inputs persistent://public/default/api-java-fn-input -o persistent://public/default/api-java-fn-output --cpu 0.1 --subs-position Earliest)
    ${KUBECTL} logs -n ${NAMESPACE} ${CLUSTER}-pulsar-broker-0
    sleep 15
    echo "${RET}"
    ${KUBECTL} logs -n ${NAMESPACE} ${CLUSTER}-pulsar-broker-0
    sleep 15
    ${KUBECTL} get pods -A
    sleep 5
    WC=$(${KUBECTL} get pods -n ${NAMESPACE} --field-selector=status.phase=Running | grep "api-java-fn" | wc -l)
    while [[ ${WC} -lt 1 ]]; do
      echo ${WC};
      sleep 20
      ${KUBECTL} get pods -n ${NAMESPACE}
      RET=$(${KUBECTL} get pods -n ${NAMESPACE} -o name | grep api-java-fn)
      ${KUBECTL} describe ${RET}
      WC=$(${KUBECTL} get pods -n ${NAMESPACE} --field-selector=status.phase=Running | grep "api-java-fn" | wc -l)
    done
    sleep 120
    echo "produce messages"
    ${KUBECTL} exec -n ${NAMESPACE} ${CLUSTER}-pulsar-broker-0 -- bin/pulsar-client produce -m "test-message" -n 100 "persistent://public/default/api-java-fn-input"
    sleep 120
    ${KUBECTL} exec -n ${NAMESPACE} ${CLUSTER}-pulsar-broker-0 -- bin/pulsar-admin topics stats "persistent://public/default/api-java-fn-input"
    echo "java function test done"
    echo "get function stats"
    ${KUBECTL} exec -n ${NAMESPACE} ${CLUSTER}-pulsar-broker-0 -- bin/pulsar-admin functions stats --name api-java-fn
    RET=$(${KUBECTL} exec -n ${NAMESPACE} ${CLUSTER}-pulsar-broker-0 -- bin/pulsar-admin functions stats --name api-java-fn | jq .receivedTotal)
    echo "${RET}"
    while [[ ${RET} -lt 10 ]]; do
      sleep 10
      RET=$(${KUBECTL} exec -n ${NAMESPACE} ${CLUSTER}-pulsar-broker-0 -- bin/pulsar-admin functions stats --name api-java-fn | jq .receivedTotal)
      echo "${RET}"
      ${KUBECTL} exec -n ${NAMESPACE} ${CLUSTER}-pulsar-broker-0 -- bin/pulsar-admin functions stats --name api-java-fn
      ${KUBECTL} exec -n ${NAMESPACE} ${CLUSTER}-pulsar-broker-0 -- bin/pulsar-admin functions status --name api-java-fn
    done
    RET=$(${KUBECTL} exec -n ${NAMESPACE} ${CLUSTER}-pulsar-broker-0 -- bin/pulsar-admin functions delete --name api-java-fn)
    echo "${RET}"
}