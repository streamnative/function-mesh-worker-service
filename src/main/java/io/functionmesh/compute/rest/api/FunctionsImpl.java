/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.functionmesh.compute.rest.api;

import static io.functionmesh.compute.util.KubernetesUtils.validateResourceOwner;
import static io.functionmesh.compute.util.KubernetesUtils.validateStatefulSet;
import com.google.common.annotations.VisibleForTesting;
import io.functionmesh.compute.MeshWorkerService;
import io.functionmesh.compute.functions.models.V1alpha1Function;
import io.functionmesh.compute.functions.models.V1alpha1FunctionList;
import io.functionmesh.compute.functions.models.V1alpha1FunctionSpecJava;
import io.functionmesh.compute.functions.models.V1alpha1FunctionSpecPod;
import io.functionmesh.compute.functions.models.V1alpha1FunctionSpecPodInitContainers;
import io.functionmesh.compute.functions.models.V1alpha1FunctionSpecPodVolumeMounts;
import io.functionmesh.compute.functions.models.V1alpha1FunctionSpecPodVolumes;
import io.functionmesh.compute.functions.models.V1alpha1FunctionStatus;
import io.functionmesh.compute.models.MeshWorkerServiceCustomConfig;
import io.functionmesh.compute.util.CommonUtil;
import io.functionmesh.compute.util.FunctionsUtil;
import io.functionmesh.compute.util.KubernetesUtils;
import io.functionmesh.compute.util.PackageManagementServiceUtil;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.kubernetes.client.openapi.models.V1ContainerState;
import io.kubernetes.client.openapi.models.V1ContainerStatus;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodList;
import io.kubernetes.client.openapi.models.V1PodStatus;
import io.kubernetes.client.openapi.models.V1StatefulSet;
import io.kubernetes.client.util.generic.GenericKubernetesApi;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.ws.rs.core.Response;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.authentication.AuthenticationDataHttps;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.functions.UpdateOptionsImpl;
import org.apache.pulsar.common.policies.data.FunctionInstanceStatsImpl;
import org.apache.pulsar.common.policies.data.FunctionStatus;
import org.apache.pulsar.common.util.RestException;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.InstanceCommunication;
import org.apache.pulsar.functions.proto.InstanceControlGrpc;
import org.apache.pulsar.functions.utils.ComponentTypeUtils;
import org.apache.pulsar.functions.worker.service.api.Functions;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;

@Slf4j
public class FunctionsImpl extends MeshComponentImpl<V1alpha1Function, V1alpha1FunctionList>
        implements Functions<MeshWorkerService> {

    public FunctionsImpl(Supplier<MeshWorkerService> meshWorkerServiceSupplier) {
        super(meshWorkerServiceSupplier, Function.FunctionDetails.ComponentType.FUNCTION);
        this.resourceApi = new GenericKubernetesApi<>(
                V1alpha1Function.class, V1alpha1FunctionList.class, API_GROUP, apiVersion, apiPlural,
                meshWorkerServiceSupplier.get().getApiClient());
    }


    private void validateRegisterFunctionRequestParams(String tenant, String namespace, String functionName,
                                                       FunctionConfig functionConfig, boolean jarUploaded) {
        if (tenant == null) {
            throw new RestException(Response.Status.BAD_REQUEST, "Tenant is not provided");
        }
        if (namespace == null) {
            throw new RestException(Response.Status.BAD_REQUEST, "Namespace is not provided");
        }
        if (functionName == null) {
            throw new RestException(Response.Status.BAD_REQUEST, "Function name is not provided");
        }
        if (functionConfig == null) {
            throw new RestException(Response.Status.BAD_REQUEST, "Function config is not provided");
        }
        MeshWorkerServiceCustomConfig customConfig = worker().getMeshWorkerServiceCustomConfig();
        if (jarUploaded && customConfig != null && !customConfig.isUploadEnabled()) {
            throw new RestException(Response.Status.BAD_REQUEST, "Uploading Jar File is not enabled");
        }
        this.validateResources(functionConfig.getResources(),
                worker().getWorkerConfig().getFunctionInstanceMinResources(),
                worker().getWorkerConfig().getFunctionInstanceMaxResources());
    }

    private void validateUpdateFunctionRequestParams(String tenant, String namespace, String functionName,
                                                     FunctionConfig functionConfig, boolean uploadedJar) {
        validateRegisterFunctionRequestParams(tenant, namespace, functionName, functionConfig, uploadedJar);
    }

    private void validateGetFunctionInfoRequestParams(String tenant, String namespace, String functionName) {
        this.validateGetInfoRequestParams(tenant, namespace, functionName, apiKind);
    }

    private void validateFunctionEnabled() {
        MeshWorkerServiceCustomConfig customConfig = worker().getMeshWorkerServiceCustomConfig();
        if (customConfig != null && !customConfig.isFunctionEnabled()) {
            throw new RestException(Response.Status.BAD_REQUEST, "Function API is disabled");
        }
    }

    @Override
    public void registerFunction(final String tenant,
                                 final String namespace,
                                 final String functionName,
                                 final InputStream uploadedInputStream,
                                 final FormDataContentDisposition fileDetail,
                                 final String functionPkgUrl,
                                 final FunctionConfig functionConfig,
                                 final String clientRole,
                                 AuthenticationDataHttps clientAuthenticationDataHttps) {
        validateFunctionEnabled();

        validateRegisterFunctionRequestParams(tenant, namespace, functionName, functionConfig,
                uploadedInputStream != null);
        this.validatePermission(tenant,
                namespace,
                clientRole,
                clientAuthenticationDataHttps,
                ComponentTypeUtils.toString(componentType));
        this.validateTenantIsExist(tenant, namespace, functionName, clientRole);
        String packageURL = functionPkgUrl;
        if (uploadedInputStream != null && worker().getMeshWorkerServiceCustomConfig().isUploadEnabled()) {
            try {
                String tempDirectory = System.getProperty("java.io.tmpdir");
                packageURL = PackageManagementServiceUtil.uploadPackageToPackageService(
                        worker().getBrokerAdmin(), PackageManagementServiceUtil.PACKAGE_TYPE_FUNCTION, tenant,
                        namespace, functionName, uploadedInputStream, fileDetail, tempDirectory);
            } catch (Exception e) {
                log.error("register {}/{}/{} function failed, error message: {}", tenant, namespace, functionName, e);
                throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
            }
        }

        String cluster = worker().getWorkerConfig().getPulsarFunctionsCluster();
        V1alpha1Function v1alpha1Function = FunctionsUtil.createV1alpha1FunctionFromFunctionConfig(
                apiKind,
                API_GROUP,
                apiVersion,
                functionName,
                packageURL,
                functionConfig,
                cluster,
                worker()
        );
        // override namespace by configuration file
        v1alpha1Function.getMetadata().setNamespace(worker().getJobNamespace());
        try {
            this.upsertFunction(tenant, namespace, functionName, functionConfig, v1alpha1Function,
                    clientAuthenticationDataHttps);

            extractResponse(getResourceApi().create(v1alpha1Function));
        } catch (RestException restException) {
            log.error(
                    "register {}/{}/{} sink failed, error message: {}",
                    tenant,
                    namespace,
                    functionConfig,
                    restException.getMessage());
            throw restException;
        } catch (Exception e) {
            log.error("register {}/{}/{} function failed, error message: {}", tenant, namespace, functionName, e);
            throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    @Override
    public void updateFunction(final String tenant,
                               final String namespace,
                               final String functionName,
                               final InputStream uploadedInputStream,
                               final FormDataContentDisposition fileDetail,
                               final String functionPkgUrl,
                               final FunctionConfig functionConfig,
                               final String clientRole,
                               AuthenticationDataHttps clientAuthenticationDataHttps,
                               UpdateOptionsImpl updateOptions) {
        validateFunctionEnabled();

        validateUpdateFunctionRequestParams(tenant, namespace, functionName, functionConfig,
                uploadedInputStream != null);
        this.validatePermission(tenant,
                namespace,
                clientRole,
                clientAuthenticationDataHttps,
                ComponentTypeUtils.toString(componentType));
        this.validateTenantIsExist(tenant, namespace, functionName, clientRole);
        String packageURL = functionPkgUrl;
        if (uploadedInputStream != null && worker().getMeshWorkerServiceCustomConfig().isUploadEnabled()) {
            try {
                String tempDirectory = System.getProperty("java.io.tmpdir");
                packageURL = PackageManagementServiceUtil.uploadPackageToPackageService(
                        worker().getBrokerAdmin(), PackageManagementServiceUtil.PACKAGE_TYPE_FUNCTION, tenant,
                        namespace, functionName, uploadedInputStream, fileDetail, tempDirectory);
            } catch (Exception e) {
                log.error("update {}/{}/{} function failed, error message: {}", tenant, namespace, functionName, e);
                throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
            }
        }
        try {
            String cluster = worker().getWorkerConfig().getPulsarFunctionsCluster();
            V1alpha1Function v1alpha1Function = FunctionsUtil.createV1alpha1FunctionFromFunctionConfig(
                    apiKind,
                    API_GROUP,
                    apiVersion,
                    functionName,
                    packageURL,
                    functionConfig,
                    cluster,
                    worker()
            );

            String nameSpaceName = worker().getJobNamespace();
            String hashName = CommonUtil.generateObjectName(worker(), tenant, namespace, functionName);
            V1alpha1Function v1alpha1FunctionPre = extractResponse(getResourceApi().get(nameSpaceName, hashName));
            if (v1alpha1FunctionPre.getMetadata() == null || v1alpha1FunctionPre.getMetadata().getLabels() == null) {
                log.error("update {}/{}/{} function failed, the function resource cannot be found", tenant, namespace,
                        functionName);
                throw new RestException(Response.Status.NOT_FOUND, "This function resource was not found");
            }

            v1alpha1Function.getMetadata().setNamespace(worker().getJobNamespace());
            v1alpha1Function.getMetadata().setResourceVersion(v1alpha1FunctionPre.getMetadata().getResourceVersion());

            this.upsertFunction(tenant, namespace, functionName, functionConfig, v1alpha1Function,
                    clientAuthenticationDataHttps);
            extractResponse(getResourceApi().update(v1alpha1Function));
        } catch (Exception e) {
            log.error("update {}/{}/{} function failed", tenant, namespace, functionName, e);
            throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    @SuppressWarnings("checkstyle:RegexpSingleline")
    @Override
    public FunctionConfig getFunctionInfo(final String tenant,
                                          final String namespace,
                                          final String componentName,
                                          final String clientRole,
                                          final AuthenticationDataSource clientAuthenticationDataHttps) {
        validateFunctionEnabled();
        validateGetFunctionInfoRequestParams(tenant, namespace, componentName);

        this.validatePermission(tenant,
                namespace,
                clientRole,
                clientAuthenticationDataHttps,
                ComponentTypeUtils.toString(componentType));

        try {
            String nameSpaceName = worker().getJobNamespace();
            String hashName = CommonUtil.generateObjectName(worker(), tenant, namespace, componentName);

            V1alpha1Function v1alpha1Function = extractResponse(getResourceApi().get(nameSpaceName, hashName));

            return FunctionsUtil.createFunctionConfigFromV1alpha1Function(tenant, namespace, componentName,
                    v1alpha1Function);
        } catch (Exception e) {
            log.error("get {}/{}/{} function failed", tenant, namespace, componentName, e);
            throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    @Override
    List<FunctionInstanceStatsImpl> getComponentInstancesStats(String tenant, String namespace, String componentName) {
        validateFunctionEnabled();
        List<FunctionInstanceStatsImpl> functionInstanceStatsList = new ArrayList<>();
        try {
            String nameSpaceName = worker().getJobNamespace();
            String hashName = CommonUtil.generateObjectName(worker(), tenant, namespace, componentName);

            V1alpha1Function v1alpha1Function = extractResponse(getResourceApi().get(nameSpaceName, hashName));
            try {
                validateResourceObject(v1alpha1Function);
            } catch (IllegalArgumentException e) {
                log.warn("get stats {}/{}/{} function failed", tenant, namespace, componentName, e);
                return functionInstanceStatsList;
            }
            V1alpha1FunctionStatus v1alpha1FunctionStatus = v1alpha1Function.getStatus();
            final V1StatefulSet v1StatefulSet = getFunctionStatefulSet(v1alpha1Function);
            try {
                validateStatefulSet(v1StatefulSet);
            } catch (IllegalArgumentException e) {
                log.warn("get stats {}/{}/{} function failed", tenant, namespace, componentName, e);
                return functionInstanceStatsList;
            }
            final String statefulSetName = v1StatefulSet.getMetadata().getName();
            final String subdomain = v1StatefulSet.getSpec().getServiceName();
            if (v1StatefulSet.getStatus() != null) {
                Integer replicas = v1StatefulSet.getStatus().getReplicas();
                if (replicas != null) {
                    for (int i = 0; i < replicas; i++) {
                        FunctionInstanceStatsImpl functionInstanceStats = new FunctionInstanceStatsImpl();
                        functionInstanceStats.setInstanceId(i);
                        functionInstanceStatsList.add(functionInstanceStats);
                    }
                }
            }
            V1PodList podList = getFunctionPods(tenant, namespace, componentName, v1alpha1FunctionStatus);
            if (podList != null) {
                List<V1Pod> runningPods = podList.getItems().stream().
                        filter(KubernetesUtils::isPodRunning).collect(Collectors.toList());
                if (!runningPods.isEmpty()) {
                    int podsCount = runningPods.size();
                    ManagedChannel[] channel = new ManagedChannel[podsCount];
                    InstanceControlGrpc.InstanceControlFutureStub[] stub =
                            new InstanceControlGrpc.InstanceControlFutureStub[podsCount];
                    Set<CompletableFuture<InstanceCommunication.MetricsData>> completableFutureSet =
                            fetchStatsFromGRPC(runningPods, subdomain, statefulSetName,
                                    nameSpaceName, functionInstanceStatsList, channel, stub);
                    completableFutureSet.forEach(CompletableFuture::join);
                }
            }
        } catch (Exception e) {
            log.warn("Get function {} stats failed from namespace {}",
                    componentName, namespace, e);
        }
        return functionInstanceStatsList;
    }

    @Override
    void validateResourceObject(V1alpha1Function obj) {
        if (obj == null) {
            throw new IllegalArgumentException("Function Resource is null");
        }
        if (obj.getMetadata() == null) {
            throw new IllegalArgumentException("Function Resource metadata is null");
        }
        if (obj.getSpec() == null) {
            throw new IllegalArgumentException("Function Resource spec is null");
        }
        if (obj.getStatus() == null) {
            throw new IllegalArgumentException("Function Resource status is null");
        }
    }

    @Override
    public FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData getFunctionInstanceStatus(
            final String tenant,
            final String namespace,
            final String componentName,
            final String instanceId,
            final URI uri,
            final String clientRole,
            final AuthenticationDataSource clientAuthenticationDataHttps) {

        throw new RestException(Response.Status.BAD_REQUEST, "Unsupported Operation");
    }

    @Override
    public FunctionStatus getFunctionStatus(final String tenant,
                                            final String namespace,
                                            final String componentName,
                                            final URI uri,
                                            final String clientRole,
                                            final AuthenticationDataSource clientAuthenticationDataHttps) {
        validateFunctionEnabled();
        FunctionStatus functionStatus = new FunctionStatus();
        this.validatePermission(tenant,
                namespace,
                clientRole,
                clientAuthenticationDataHttps,
                ComponentTypeUtils.toString(componentType));
        try {
            String hashName = CommonUtil.generateObjectName(worker(), tenant, namespace, componentName);
            String nameSpaceName = worker().getJobNamespace();
            V1alpha1Function v1alpha1Function = extractResponse(getResourceApi().get(nameSpaceName, hashName));
            try {
                validateResourceObject(v1alpha1Function);
            } catch (IllegalArgumentException e) {
                log.warn("get stats {}/{}/{} function failed", tenant, namespace, componentName);
                throw new RestException(Response.Status.NOT_FOUND,
                        String.format("get stats %s/%s/%s function failed,details=%s", tenant, namespace, componentName,
                                e.getMessage()));
            }
            V1alpha1FunctionStatus v1alpha1FunctionStatus = v1alpha1Function.getStatus();
            String functionLabelSelector = v1alpha1FunctionStatus.getSelector();
            V1StatefulSet v1StatefulSet = getFunctionStatefulSet(v1alpha1Function);
            String statefulSetName = "";
            String subdomain = "";
            if (v1StatefulSet == null) {
                log.error(
                        "get status {}/{}/{} function failed, no StatefulSet exists",
                        tenant,
                        namespace,
                        componentName);
                throw new RestException(Response.Status.NOT_FOUND, "no StatefulSet exists");
            }
            if (v1StatefulSet.getMetadata() != null
                    && StringUtils.isNotEmpty(v1StatefulSet.getMetadata().getName())) {
                statefulSetName = v1StatefulSet.getMetadata().getName();
            } else {
                log.error(
                        "get status {}/{}/{} function failed, no statefulSetName exists",
                        tenant,
                        namespace,
                        componentName);
                throw new RestException(Response.Status.NOT_FOUND, "no statefulSetName exists");
            }
            if (v1StatefulSet.getSpec() != null
                    && StringUtils.isNotEmpty(v1StatefulSet.getSpec().getServiceName())) {
                subdomain = v1StatefulSet.getSpec().getServiceName();
            } else {
                log.error(
                        "get status {}/{}/{} function failed, no ServiceName exists",
                        tenant,
                        namespace,
                        componentName);
                throw new RestException(Response.Status.NOT_FOUND, "no ServiceName exists");
            }
            if (v1StatefulSet.getStatus() != null) {
                Integer replicas = v1StatefulSet.getStatus().getReplicas();
                if (replicas != null) {
                    functionStatus.setNumInstances(replicas);
                    for (int i = 0; i < replicas; i++) {
                        FunctionStatus.FunctionInstanceStatus functionInstanceStatus =
                                new FunctionStatus.FunctionInstanceStatus();
                        FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData functionInstanceStatusData =
                                new FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData();
                        functionInstanceStatus.setInstanceId(i);
                        functionInstanceStatus.setStatus(functionInstanceStatusData);
                        functionStatus.addInstance(functionInstanceStatus);
                    }
                    if (v1StatefulSet.getStatus().getReadyReplicas() != null) {
                        functionStatus.setNumRunning(v1StatefulSet.getStatus().getReadyReplicas());
                    }
                }
            } else {
                log.error(
                        "no StatefulSet status exists when get status of function {}/{}/{}",
                        tenant,
                        namespace,
                        componentName);
                throw new RestException(Response.Status.NOT_FOUND, "no StatefulSet status exists");
            }
            V1PodList podList = getFunctionPods(tenant, namespace, componentName, v1alpha1FunctionStatus);
            if (podList != null) {
                List<V1Pod> runningPods = podList.getItems().stream().
                        filter(KubernetesUtils::isPodRunning).collect(Collectors.toList());
                List<V1Pod> pendingPods = podList.getItems().stream().
                        filter(pod -> !KubernetesUtils.isPodRunning(pod)).collect(Collectors.toList());
                if (!runningPods.isEmpty()) {
                    int podsCount = runningPods.size();
                    ManagedChannel[] channel = new ManagedChannel[podsCount];
                    InstanceControlGrpc.InstanceControlFutureStub[] stub =
                            new InstanceControlGrpc.InstanceControlFutureStub[podsCount];
                    Set<CompletableFuture<InstanceCommunication.FunctionStatus>> completableFutureSet =
                            fetchFunctionStatusFromGRPC(runningPods, subdomain, statefulSetName, nameSpaceName,
                                    functionStatus, v1alpha1Function, channel, stub);
                    completableFutureSet.forEach(CompletableFuture::join);
                }
                if (!pendingPods.isEmpty()) {
                    fillFunctionStatusByPendingPod(pendingPods, statefulSetName, nameSpaceName, functionStatus,
                            v1alpha1Function);
                }
            }
        } catch (Exception e) {
            log.error("Get function {} status failed from namespace {}",
                    componentName, namespace, e);
        }

        return functionStatus;
    }

    @Override
    public void updateFunctionOnWorkerLeader(final String tenant,
                                             final String namespace,
                                             final String functionName,
                                             final InputStream uploadedInputStream,
                                             final boolean delete,
                                             URI uri,
                                             final String clientRole,
                                             final AuthenticationDataSource clientAuthenticationDataHttps) {
        throw new RestException(Response.Status.BAD_REQUEST, "Unsupported Operation");
    }

    private void upsertFunction(final String tenant,
                                final String namespace,
                                final String functionName,
                                final FunctionConfig functionConfig,
                                V1alpha1Function v1alpha1Function,
                                AuthenticationDataHttps clientAuthenticationDataHttps) {
        if (worker().getWorkerConfig().isAuthenticationEnabled()) {
            if (clientAuthenticationDataHttps != null) {
                try {
                    V1alpha1FunctionSpecPod podPolicy = v1alpha1Function.getSpec().getPod();
                    if (podPolicy == null) {
                        podPolicy = new V1alpha1FunctionSpecPod();
                        v1alpha1Function.getSpec().setPod(podPolicy);
                    }
                    MeshWorkerServiceCustomConfig customConfig = worker().getMeshWorkerServiceCustomConfig();
                    List<V1alpha1FunctionSpecPodVolumes> volumesList =
                            customConfig.asV1alpha1FunctionSpecPodVolumesList();
                    if (volumesList != null && !volumesList.isEmpty()) {
                        podPolicy.setVolumes(volumesList);
                    }
                    List<V1alpha1FunctionSpecPodVolumeMounts> volumeMountsList =
                            customConfig.asV1alpha1FunctionSpecPodVolumeMounts();
                    if (volumeMountsList != null && !volumeMountsList.isEmpty()) {
                        v1alpha1Function.getSpec().setVolumeMounts(volumeMountsList);
                    }
                    if (StringUtils.isNotEmpty(customConfig.getExtraDependenciesDir())) {
                        V1alpha1FunctionSpecJava v1alpha1FunctionSpecJava = null;
                        if (v1alpha1Function.getSpec() != null && v1alpha1Function.getSpec().getJava() != null) {
                            v1alpha1FunctionSpecJava = v1alpha1Function.getSpec().getJava();
                        } else if (v1alpha1Function.getSpec() != null && v1alpha1Function.getSpec().getJava() == null
                                && v1alpha1Function.getSpec().getPython() == null
                                && v1alpha1Function.getSpec().getGolang() == null) {
                            v1alpha1FunctionSpecJava = new V1alpha1FunctionSpecJava();
                        }
                        if (v1alpha1FunctionSpecJava != null && StringUtils.isEmpty(
                                v1alpha1FunctionSpecJava.getExtraDependenciesDir())) {
                            v1alpha1FunctionSpecJava.setExtraDependenciesDir(customConfig.getExtraDependenciesDir());
                            v1alpha1Function.getSpec().setJava(v1alpha1FunctionSpecJava);
                        }
                    }
                    if (!StringUtils.isEmpty(worker().getWorkerConfig().getBrokerClientAuthenticationPlugin())
                            && !StringUtils.isEmpty(
                            worker().getWorkerConfig().getBrokerClientAuthenticationParameters())) {
                        String authSecretName = KubernetesUtils.upsertSecret(apiKind.toLowerCase(), "auth",
                                v1alpha1Function.getSpec().getClusterName(), tenant, namespace, functionName, worker());
                        v1alpha1Function.getSpec().getPulsar().setAuthSecret(authSecretName);
                    }
                    if (worker().getWorkerConfig().getTlsEnabled()) {
                        String tlsSecretName = KubernetesUtils.upsertSecret(apiKind.toLowerCase(), "tls",
                                v1alpha1Function.getSpec().getClusterName(), tenant, namespace, functionName, worker());
                        v1alpha1Function.getSpec().getPulsar().setTlsSecret(tlsSecretName);
                    }
                    if (!StringUtils.isEmpty(customConfig.getDefaultServiceAccountName())
                            && StringUtils.isEmpty(podPolicy.getServiceAccountName())) {
                        podPolicy.setServiceAccountName(customConfig.getDefaultServiceAccountName());
                    }
                    if (customConfig.getImagePullSecrets() != null && !customConfig.getImagePullSecrets().isEmpty()) {
                        podPolicy.setImagePullSecrets(customConfig.asV1alpha1FunctionSpecPodImagePullSecrets());
                    }
                    List<V1alpha1FunctionSpecPodInitContainers> initContainersList =
                            customConfig.asV1alpha1FunctionSpecPodInitContainers();
                    if (initContainersList != null && !initContainersList.isEmpty()) {
                        podPolicy.setInitContainers(initContainersList);
                    }
                    v1alpha1Function.getSpec().setPod(podPolicy);
                } catch (Exception e) {
                    log.error("Error create or update auth or tls secret for {} {}/{}/{}",
                            ComponentTypeUtils.toString(componentType), tenant, namespace, functionName, e);


                    throw new RestException(Response.Status.INTERNAL_SERVER_ERROR,
                            String.format("Error create or update auth or tls secret for %s %s:- %s",
                                    ComponentTypeUtils.toString(componentType), functionName, e.getMessage()));
                }
            }
        }
        if (worker().getMeshWorkerServiceCustomConfig().isEnableTrustedMode()) {
            FunctionsUtil.mergeTrustedConfigs(functionConfig, v1alpha1Function);
        }
    }

    public V1StatefulSet getFunctionStatefulSet(V1alpha1Function v1alpha1Function) {
        try {
            String nameSpaceName = worker().getJobNamespace();
            String jobName =
                    CommonUtil.makeJobName(v1alpha1Function.getMetadata().getName(), CommonUtil.COMPONENT_FUNCTION);
            V1StatefulSet v1StatefulSet =
                    worker().getAppsV1Api().readNamespacedStatefulSet(jobName, nameSpaceName, null, null, null);
            if (validateResourceOwner(v1StatefulSet, v1alpha1Function)) {
                return v1StatefulSet;
            } else {
                log.warn("get function statefulset failed, not owned by the resource");
                return null;
            }
        } catch (Exception e) {
            log.error("get function statefulset failed, error: {}", e.getMessage());
        }
        return null;
    }

    public V1PodList getFunctionPods(String tenant, String namespace, String componentName,
                                     V1alpha1FunctionStatus v1alpha1FunctionStatus) {
        V1PodList podList = null;
        try {
            String nameSpaceName = worker().getJobNamespace();
            String functionLabelSelector = v1alpha1FunctionStatus.getSelector();
            podList = worker().getCoreV1Api().listNamespacedPod(
                    nameSpaceName, null, null, null, null,
                    functionLabelSelector, null, null, null, null,
                    null);
        } catch (Exception e) {
            log.error("get function pods failed, {}/{}/{}", tenant, namespace, componentName, e);
        }
        return podList;
    }

    @VisibleForTesting
    protected Set<CompletableFuture<InstanceCommunication.FunctionStatus>> fetchFunctionStatusFromGRPC(List<V1Pod> pods,
                                                                                                       String subdomain,
                                                                                                       String statefulSetName,
                                                                                                       String nameSpaceName,
                                                                                                       FunctionStatus functionStatus,
                                                                                                       V1alpha1Function v1alpha1Function,
                                                                                                       ManagedChannel[] channel,
                                                                                                       InstanceControlGrpc.InstanceControlFutureStub[] stub) {
        Set<CompletableFuture<InstanceCommunication.FunctionStatus>> completableFutureSet = new HashSet<>();
        pods.forEach(pod -> {
            String podName = KubernetesUtils.getPodName(pod);
            int shardId = CommonUtil.getShardIdFromPodName(podName);
            int podIndex = pods.indexOf(pod);
            String address = KubernetesUtils.getServiceUrl(podName, subdomain, nameSpaceName);
            if (shardId == -1) {
                log.warn("shardId invalid {}", podName);
                return;
            }
            FunctionStatus.FunctionInstanceStatus functionInstanceStatus = null;
            for (FunctionStatus.FunctionInstanceStatus ins : functionStatus.getInstances()) {
                if (ins.getInstanceId() == shardId) {
                    functionInstanceStatus = ins;
                    break;
                }
            }
            if (functionInstanceStatus != null) {
                FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData
                        functionInstanceStatusData = functionInstanceStatus.getStatus();
                V1PodStatus podStatus = pod.getStatus();
                if (v1alpha1Function.getSpec() != null && StringUtils.isNotEmpty(
                        v1alpha1Function.getSpec().getClusterName())) {
                    functionInstanceStatusData.setWorkerId(v1alpha1Function.getSpec().getClusterName());
                }
                if (podStatus != null) {
                    functionInstanceStatusData.setRunning(KubernetesUtils.isPodRunning(pod));
                    V1ContainerStatus containerStatus =
                            KubernetesUtils.extractDefaultContainerStatus(pod);
                    if (containerStatus != null) {
                        functionInstanceStatusData.setNumRestarts(containerStatus.getRestartCount());
                    } else {
                        log.warn("containerStatus is null, cannot get restart count for pod {}",
                                podName);
                        log.debug("existing containerStatus: {}", podStatus.getContainerStatuses());
                    }
                }
                // get status from grpc
                if (channel[podIndex] == null && stub[podIndex] == null) {
                    channel[podIndex] = ManagedChannelBuilder.forAddress(address, 9093)
                            .usePlaintext()
                            .build();
                    stub[podIndex] = InstanceControlGrpc.newFutureStub(channel[podIndex]);
                }
                CompletableFuture<InstanceCommunication.FunctionStatus> future =
                        CommonUtil.getFunctionStatusAsync(stub[podIndex]);
                future.whenComplete((fs, e) -> {
                    if (channel[podIndex] != null) {
                        log.debug("closing channel {}", podIndex);
                        channel[podIndex].shutdown();
                    }
                    if (e != null) {
                        log.error("Get function {}-{} status from grpc failed from namespace {}",
                                statefulSetName,
                                shardId,
                                nameSpaceName,
                                e);
                        functionInstanceStatusData.setError(e.getMessage());
                    } else if (fs != null) {
                        FunctionsUtil.convertFunctionStatusToInstanceStatusData(fs,
                                functionInstanceStatusData);
                    }
                });
                completableFutureSet.add(future);
            } else {
                log.error(
                        "Get function {}-{} status failed from namespace {}, cannot find status for "
                                + "shardId {}",
                        statefulSetName,
                        shardId,
                        nameSpaceName,
                        shardId);
            }
        });
        return completableFutureSet;
    }

    @VisibleForTesting
    protected void fillFunctionStatusByPendingPod(List<V1Pod> pods,
                                                  String statefulSetName,
                                                  String nameSpaceName,
                                                  FunctionStatus functionStatus,
                                                  V1alpha1Function v1alpha1Function) {
        pods.forEach(pod -> {
            String podName = KubernetesUtils.getPodName(pod);
            int shardId = CommonUtil.getShardIdFromPodName(podName);
            if (shardId == -1) {
                log.warn("shardId invalid {}", podName);
                return;
            }
            FunctionStatus.FunctionInstanceStatus functionInstanceStatus = null;
            for (FunctionStatus.FunctionInstanceStatus ins : functionStatus.getInstances()) {
                if (ins.getInstanceId() == shardId) {
                    functionInstanceStatus = ins;
                    break;
                }
            }
            if (functionInstanceStatus != null) {
                FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData
                        functionInstanceStatusData = functionInstanceStatus.getStatus();
                V1PodStatus podStatus = pod.getStatus();
                if (podStatus != null) {
                    List<V1ContainerStatus> containerStatuses = podStatus.getContainerStatuses();
                    if (containerStatuses != null && !containerStatuses.isEmpty()) {
                        V1ContainerStatus containerStatus = null;
                        for (V1ContainerStatus s : containerStatuses) {
                            if (s.getImage().contains(v1alpha1Function.getSpec().getImage())) {
                                containerStatus = s;
                                break;
                            }
                        }
                        if (containerStatus != null) {
                            V1ContainerState state = containerStatus.getState();
                            if (state != null && state.getTerminated() != null) {
                                functionInstanceStatusData.setError(state.getTerminated().getMessage());
                            } else if (state != null && state.getWaiting() != null) {
                                functionInstanceStatusData.setError(state.getWaiting().getMessage());
                            } else {
                                V1ContainerState lastState = containerStatus.getLastState();
                                if (lastState != null && lastState.getTerminated() != null) {
                                    functionInstanceStatusData.setError(
                                            lastState.getTerminated().getMessage());
                                } else if (lastState != null && lastState.getWaiting() != null) {
                                    functionInstanceStatusData.setError(
                                            lastState.getWaiting().getMessage());
                                }
                            }
                            if (containerStatus.getRestartCount() != null) {
                                functionInstanceStatusData.setNumRestarts(
                                        containerStatus.getRestartCount());
                            }
                        } else {
                            functionInstanceStatusData.setError(podStatus.getPhase());
                        }
                    }
                }
            } else {
                log.error(
                        "Get function {}-{} status failed from namespace {}, cannot find status for "
                                + "shardId {}",
                        statefulSetName,
                        shardId,
                        nameSpaceName,
                        shardId);
            }
        });
    }

}
