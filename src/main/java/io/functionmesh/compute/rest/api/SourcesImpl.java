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
import io.functionmesh.compute.models.MeshWorkerServiceCustomConfig;
import io.functionmesh.compute.sources.models.V1alpha1Source;
import io.functionmesh.compute.sources.models.V1alpha1SourceList;
import io.functionmesh.compute.sources.models.V1alpha1SourceSpecJava;
import io.functionmesh.compute.sources.models.V1alpha1SourceSpecPod;
import io.functionmesh.compute.sources.models.V1alpha1SourceSpecPodInitContainers;
import io.functionmesh.compute.sources.models.V1alpha1SourceSpecPodVolumeMounts;
import io.functionmesh.compute.sources.models.V1alpha1SourceSpecPodVolumes;
import io.functionmesh.compute.sources.models.V1alpha1SourceStatus;
import io.functionmesh.compute.util.CommonUtil;
import io.functionmesh.compute.util.KubernetesUtils;
import io.functionmesh.compute.util.PackageManagementServiceUtil;
import io.functionmesh.compute.util.SourcesUtil;
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
import org.apache.pulsar.common.functions.UpdateOptionsImpl;
import org.apache.pulsar.common.io.ConfigFieldDefinition;
import org.apache.pulsar.common.io.ConnectorDefinition;
import org.apache.pulsar.common.io.SourceConfig;
import org.apache.pulsar.common.policies.data.FunctionInstanceStatsImpl;
import org.apache.pulsar.common.policies.data.SourceStatus;
import org.apache.pulsar.common.policies.data.SourceStatus.SourceInstanceStatus.SourceInstanceStatusData;
import org.apache.pulsar.common.util.RestException;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.InstanceCommunication;
import org.apache.pulsar.functions.proto.InstanceControlGrpc;
import org.apache.pulsar.functions.utils.ComponentTypeUtils;
import org.apache.pulsar.functions.worker.service.api.Sources;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;

@Slf4j
public class SourcesImpl extends MeshComponentImpl<V1alpha1Source, V1alpha1SourceList>
        implements Sources<MeshWorkerService> {
    private final String kind = "Source";

    private final String plural = "sources";

    public SourcesImpl(Supplier<MeshWorkerService> meshWorkerServiceSupplier) {
        super(meshWorkerServiceSupplier, Function.FunctionDetails.ComponentType.SOURCE);
        super.apiPlural = this.plural;
        super.apiKind = this.kind;
        this.resourceApi = new GenericKubernetesApi<>(
                V1alpha1Source.class, V1alpha1SourceList.class, API_GROUP, apiVersion, apiPlural,
                meshWorkerServiceSupplier.get().getApiClient());
    }

    private void validateSourceEnabled() {
        MeshWorkerServiceCustomConfig customConfig = worker().getMeshWorkerServiceCustomConfig();
        if (customConfig != null && !customConfig.isSourceEnabled()) {
            throw new RestException(Response.Status.BAD_REQUEST, "Source API is disabled");
        }
    }

    private void validateRegisterSourceRequestParams(String tenant, String namespace, String sourceName,
                                                     SourceConfig sourceConfig, boolean jarUploaded) {
        if (tenant == null) {
            throw new RestException(Response.Status.BAD_REQUEST, "Tenant is not provided");
        }
        if (namespace == null) {
            throw new RestException(Response.Status.BAD_REQUEST, "Namespace is not provided");
        }
        if (sourceName == null) {
            throw new RestException(Response.Status.BAD_REQUEST, "Source name is not provided");
        }
        if (sourceConfig == null) {
            throw new RestException(Response.Status.BAD_REQUEST, "Source config is not provided");
        }
        MeshWorkerServiceCustomConfig customConfig = worker().getMeshWorkerServiceCustomConfig();
        if (jarUploaded && customConfig != null && !customConfig.isUploadEnabled()) {
            throw new RestException(Response.Status.BAD_REQUEST, "Uploading Jar File is not enabled");
        }
        this.validateResources(sourceConfig.getResources(),
                worker().getWorkerConfig().getFunctionInstanceMinResources(),
                worker().getWorkerConfig().getFunctionInstanceMaxResources());
    }

    private void validateUpdateSourceRequestParams(String tenant, String namespace, String sourceName,
                                                   SourceConfig sourceConfig, boolean jarUploaded) {
        this.validateRegisterSourceRequestParams(tenant, namespace, sourceName, sourceConfig, jarUploaded);
    }

    private void validateGetSourceInfoRequestParams(String tenant, String namespace, String sourceName) {
        if (tenant == null) {
            throw new RestException(Response.Status.BAD_REQUEST, "Tenant is not provided");
        }
        if (namespace == null) {
            throw new RestException(Response.Status.BAD_REQUEST, "Namespace is not provided");
        }
        if (sourceName == null) {
            throw new RestException(Response.Status.BAD_REQUEST, "Source name is not provided");
        }
    }

    public void registerSource(final String tenant,
                               final String namespace,
                               final String sourceName,
                               final InputStream uploadedInputStream,
                               final FormDataContentDisposition fileDetail,
                               final String sourcePkgUrl,
                               final SourceConfig sourceConfig,
                               final String clientRole,
                               AuthenticationDataHttps clientAuthenticationDataHttps) {
        validateSourceEnabled();
        validateRegisterSourceRequestParams(tenant, namespace, sourceName, sourceConfig, uploadedInputStream != null);
        this.validatePermission(tenant,
                namespace,
                clientRole,
                clientAuthenticationDataHttps,
                ComponentTypeUtils.toString(componentType));
        this.validateTenantIsExist(tenant, namespace, sourceName, clientRole);
        String packageURL = sourcePkgUrl;
        if (uploadedInputStream != null && worker().getMeshWorkerServiceCustomConfig().isUploadEnabled()) {
            try {
                String tempDirectory = System.getProperty("java.io.tmpdir");
                packageURL = PackageManagementServiceUtil.uploadPackageToPackageService(
                        worker().getBrokerAdmin(), PackageManagementServiceUtil.PACKAGE_TYPE_SOURCE, tenant,
                        namespace, sourceName, uploadedInputStream, fileDetail, tempDirectory);
            } catch (Exception e) {
                log.error("register {}/{}/{} source failed", tenant, namespace, sourceName, e);
                throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
            }
        }
        String cluster = worker().getWorkerConfig().getPulsarFunctionsCluster();
        V1alpha1Source v1alpha1Source = SourcesUtil
                .createV1alpha1SourceFromSourceConfig(
                        apiKind,
                        API_GROUP,
                        apiVersion,
                        sourceName,
                        packageURL,
                        uploadedInputStream,
                        sourceConfig,
                        this.meshWorkerServiceSupplier.get().getConnectorsManager(),
                        cluster, worker());

        v1alpha1Source.getMetadata().setNamespace(worker().getJobNamespace());
        try {
            this.upsertSource(tenant, namespace, sourceName, sourceConfig, v1alpha1Source,
                    clientAuthenticationDataHttps);
            extractResponse(getResourceApi().create(v1alpha1Source));
        } catch (RestException restException) {
            log.error(
                    "register {}/{}/{} source failed",
                    tenant,
                    namespace,
                    sourceConfig,
                    restException);
            throw restException;
        } catch (Exception e) {
            log.error("register {}/{}/{} source failed", tenant, namespace, sourceConfig, e);
            throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    public void updateSource(final String tenant,
                             final String namespace,
                             final String sourceName,
                             final InputStream uploadedInputStream,
                             final FormDataContentDisposition fileDetail,
                             final String sourcePkgUrl,
                             final SourceConfig sourceConfig,
                             final String clientRole,
                             AuthenticationDataHttps clientAuthenticationDataHttps,
                             UpdateOptionsImpl updateOptions) {
        validateSourceEnabled();
        validateUpdateSourceRequestParams(tenant, namespace, sourceName, sourceConfig, uploadedInputStream != null);
        this.validatePermission(tenant,
                namespace,
                clientRole,
                clientAuthenticationDataHttps,
                ComponentTypeUtils.toString(componentType));
        this.validateTenantIsExist(tenant, namespace, sourceName, clientRole);
        String packageURL = sourcePkgUrl;
        if (uploadedInputStream != null && worker().getMeshWorkerServiceCustomConfig().isUploadEnabled()) {
            try {
                String tempDirectory = System.getProperty("java.io.tmpdir");
                packageURL = PackageManagementServiceUtil.uploadPackageToPackageService(
                        worker().getBrokerAdmin(), PackageManagementServiceUtil.PACKAGE_TYPE_SOURCE, tenant,
                        namespace, sourceName, uploadedInputStream, fileDetail, tempDirectory);
            } catch (Exception e) {
                log.error("update {}/{}/{} source failed", tenant, namespace, sourceName, e);
                throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
            }
        }
        try {
            String cluster = worker().getWorkerConfig().getPulsarFunctionsCluster();
            V1alpha1Source v1alpha1Source = SourcesUtil
                    .createV1alpha1SourceFromSourceConfig(
                            apiKind,
                            API_GROUP,
                            apiVersion,
                            sourceName,
                            packageURL,
                            uploadedInputStream,
                            sourceConfig,
                            this.meshWorkerServiceSupplier.get().getConnectorsManager(),
                            cluster, worker());

            String nameSpaceName = worker().getJobNamespace();
            String hashName = CommonUtil.generateObjectName(worker(), tenant, namespace, sourceName);
            V1alpha1Source v1alpha1SourcePre = extractResponse(getResourceApi().get(nameSpaceName, hashName));
            if (v1alpha1SourcePre.getMetadata() == null || v1alpha1SourcePre.getMetadata().getLabels() == null) {
                log.error("update {}/{}/{} source failed, the source resource cannot be found", tenant, namespace,
                        sourceName);
                throw new RestException(Response.Status.NOT_FOUND, "This source resource was not found");
            }

            v1alpha1Source.getMetadata().setNamespace(worker().getJobNamespace());
            v1alpha1Source.getMetadata().setResourceVersion(v1alpha1SourcePre.getMetadata().getResourceVersion());
            this.upsertSource(tenant, namespace, sourceName, sourceConfig, v1alpha1Source,
                    clientAuthenticationDataHttps);
            extractResponse(getResourceApi().update(v1alpha1Source));
        } catch (Exception e) {
            log.error("update {}/{}/{} source failed", tenant, namespace, sourceConfig, e);
            throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    public SourceStatus getSourceStatus(final String tenant,
                                        final String namespace,
                                        final String componentName,
                                        final URI uri,
                                        final String clientRole,
                                        final AuthenticationDataSource clientAuthenticationDataHttps) {
        validateSourceEnabled();
        SourceStatus sourceStatus = new SourceStatus();
        this.validatePermission(tenant,
                namespace,
                clientRole,
                clientAuthenticationDataHttps,
                ComponentTypeUtils.toString(componentType));

        try {
            String hashName = CommonUtil.generateObjectName(worker(), tenant, namespace, componentName);
            String nameSpaceName = worker().getJobNamespace();
            V1alpha1Source v1alpha1Source = extractResponse(getResourceApi().get(nameSpaceName, hashName));
            V1alpha1SourceStatus v1alpha1SourceStatus = v1alpha1Source.getStatus();
            if (v1alpha1SourceStatus == null) {
                log.error(
                        "get status {}/{}/{} source failed, no SourceStatus exists",
                        tenant,
                        namespace,
                        componentName);
                throw new RestException(Response.Status.NOT_FOUND, "no SourceStatus exists");
            }
            if (v1alpha1Source.getMetadata() == null) {
                log.error(
                        "get status {}/{}/{} source failed, no Metadata exists",
                        tenant,
                        namespace,
                        componentName);
                throw new RestException(Response.Status.NOT_FOUND, "no Metadata exists");
            }
            String sourceLabelSelector = v1alpha1SourceStatus.getSelector();
            String jobName =
                    CommonUtil.makeJobName(v1alpha1Source.getMetadata().getName(), CommonUtil.COMPONENT_SOURCE);
            V1StatefulSet v1StatefulSet =
                    worker().getAppsV1Api().readNamespacedStatefulSet(jobName, nameSpaceName, null, null, null);
            String statefulSetName = "";
            String subdomain = "";
            if (v1StatefulSet == null) {
                log.error(
                        "get status {}/{}/{} source failed, no StatefulSet exists",
                        tenant,
                        namespace,
                        componentName);
                throw new RestException(Response.Status.NOT_FOUND, "no StatefulSet exists");
            }
            if (v1StatefulSet.getMetadata() != null &&
                    StringUtils.isNotEmpty(v1StatefulSet.getMetadata().getName())) {
                statefulSetName = v1StatefulSet.getMetadata().getName();
            } else {
                log.error(
                        "get status {}/{}/{} source failed, no statefulSetName exists",
                        tenant,
                        namespace,
                        componentName);
                throw new RestException(Response.Status.NOT_FOUND, "no statefulSetName exists");
            }
            if (v1StatefulSet.getSpec() != null &&
                    StringUtils.isNotEmpty(v1StatefulSet.getSpec().getServiceName())) {
                subdomain = v1StatefulSet.getSpec().getServiceName();
            } else {
                log.error(
                        "get status {}/{}/{} source failed, no ServiceName exists",
                        tenant,
                        namespace,
                        componentName);
                throw new RestException(Response.Status.NOT_FOUND, "no ServiceName exists");
            }
            if (v1StatefulSet.getStatus() != null) {
                Integer replicas = v1StatefulSet.getStatus().getReplicas();
                if (replicas != null) {
                    sourceStatus.setNumInstances(replicas);
                    for (int i = 0; i < replicas; i++) {
                        SourceStatus.SourceInstanceStatus sourceInstanceStatus =
                                new SourceStatus.SourceInstanceStatus();
                        SourceInstanceStatusData sourceInstanceStatusData = new SourceInstanceStatusData();
                        sourceInstanceStatus.setInstanceId(i);
                        sourceInstanceStatus.setStatus(sourceInstanceStatusData);
                        sourceStatus.addInstance(sourceInstanceStatus);
                    }
                    if (v1StatefulSet.getStatus().getReadyReplicas() != null) {
                        sourceStatus.setNumRunning(v1StatefulSet.getStatus().getReadyReplicas());
                    }
                }
            } else {
                log.error(
                        "no StatefulSet status exists when get status of source {}/{}/{}",
                        tenant,
                        namespace,
                        componentName);
                throw new RestException(Response.Status.NOT_FOUND, "no StatefulSet status exists");
            }
            V1PodList podList = worker().getCoreV1Api().listNamespacedPod(
                    nameSpaceName, null, null, null, null,
                    sourceLabelSelector, null, null, null, null,
                    null);
            if (podList != null) {
                List<V1Pod> runningPods = podList.getItems().stream().
                        filter(KubernetesUtils::isPodRunning).collect(Collectors.toList());
                List<V1Pod> pendingPods = podList.getItems().stream().
                        filter(pod -> !KubernetesUtils.isPodRunning(pod)).collect(Collectors.toList());
                String finalStatefulSetName = statefulSetName;
                if (!runningPods.isEmpty()) {
                    int podsCount = runningPods.size();
                    ManagedChannel[] channel = new ManagedChannel[podsCount];
                    InstanceControlGrpc.InstanceControlFutureStub[] stub =
                            new InstanceControlGrpc.InstanceControlFutureStub[podsCount];
                    Set<CompletableFuture<InstanceCommunication.FunctionStatus>> completableFutureSet =
                            fetchSourceStatusFromGRPC(runningPods, subdomain, statefulSetName, nameSpaceName,
                                    sourceStatus, v1alpha1Source, channel, stub);
                    completableFutureSet.forEach(CompletableFuture::join);
                }
                if (!pendingPods.isEmpty()) {
                    fillSourceStatusByPendingPod(pendingPods, statefulSetName, nameSpaceName, sourceStatus,
                            v1alpha1Source);
                }
            }
        } catch (Exception e) {
            log.error("Get source {} status failed from namespace {}: ",
                    componentName, namespace, e);
        }

        return sourceStatus;
    }

    public SourceInstanceStatusData getSourceInstanceStatus(final String tenant,
                                                            final String namespace,
                                                            final String sourceName,
                                                            final String instanceId,
                                                            final URI uri,
                                                            final String clientRole,
                                                            final AuthenticationDataSource dataSource) {
        validateSourceEnabled();
        return new SourceInstanceStatusData();
    }

    public SourceConfig getSourceInfo(final String tenant,
                                      final String namespace,
                                      final String componentName) {
        validateSourceEnabled();
        this.validateGetInfoRequestParams(tenant, namespace, componentName, apiKind);

        try {
            String nameSpaceName = worker().getJobNamespace();
            String hashName = CommonUtil.generateObjectName(worker(), tenant, namespace, componentName);
            V1alpha1Source v1alpha1Source = extractResponse(getResourceApi().get(nameSpaceName, hashName));

            return SourcesUtil.createSourceConfigFromV1alpha1Source(tenant, namespace, componentName, v1alpha1Source);
        } catch (Exception e) {
            log.error("Get source info {}/{}/{} {} failed", tenant, namespace, componentName, apiPlural, e);
            throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    @Override
    public List<ConnectorDefinition> getSourceList() {
        validateSourceEnabled();
        List<ConnectorDefinition> connectorDefinitions = getListOfConnectors();
        List<ConnectorDefinition> retval = new ArrayList<>();
        for (ConnectorDefinition connectorDefinition : connectorDefinitions) {
            if (!StringUtils.isEmpty(connectorDefinition.getSourceClass())) {
                retval.add(connectorDefinition);
            }
        }
        return retval;
    }

    public List<ConfigFieldDefinition> getSourceConfigDefinition(String name) {
        validateSourceEnabled();
        return new ArrayList<>();
    }

    private void upsertSource(final String tenant,
                              final String namespace,
                              final String sourceName,
                              SourceConfig sourceConfig,
                              V1alpha1Source v1alpha1Source,
                              AuthenticationDataHttps clientAuthenticationDataHttps) {
        if (worker().getWorkerConfig().isAuthenticationEnabled()) {
            if (clientAuthenticationDataHttps != null) {
                try {
                    V1alpha1SourceSpecPod podPolicy = v1alpha1Source.getSpec().getPod();
                    if (podPolicy == null) {
                        podPolicy = new V1alpha1SourceSpecPod();
                        v1alpha1Source.getSpec().setPod(podPolicy);
                    }
                    MeshWorkerServiceCustomConfig customConfig = worker().getMeshWorkerServiceCustomConfig();
                    List<V1alpha1SourceSpecPodVolumes> volumesList = customConfig.asV1alpha1SourceSpecPodVolumesList();
                    if (volumesList != null && !volumesList.isEmpty()) {
                        podPolicy.setVolumes(volumesList);
                    }
                    List<V1alpha1SourceSpecPodVolumeMounts> volumeMountsList =
                            customConfig.asV1alpha1SourceSpecPodVolumeMountsList();
                    if (volumeMountsList != null && !volumeMountsList.isEmpty()) {
                        v1alpha1Source.getSpec().setVolumeMounts(volumeMountsList);
                    }
                    if (customConfig != null && StringUtils.isNotEmpty(customConfig.getExtraDependenciesDir())) {
                        V1alpha1SourceSpecJava v1alpha1SourceSpecJava = null;
                        if (v1alpha1Source.getSpec() != null && v1alpha1Source.getSpec().getJava() != null) {
                            v1alpha1SourceSpecJava = v1alpha1Source.getSpec().getJava();
                        } else if (v1alpha1Source.getSpec() != null && v1alpha1Source.getSpec().getJava() == null &&
                                v1alpha1Source.getSpec().getPython() == null &&
                                v1alpha1Source.getSpec().getGolang() == null) {
                            v1alpha1SourceSpecJava = new V1alpha1SourceSpecJava();
                        }
                        if (v1alpha1SourceSpecJava != null && StringUtils.isEmpty(
                                v1alpha1SourceSpecJava.getExtraDependenciesDir())) {
                            v1alpha1SourceSpecJava.setExtraDependenciesDir(customConfig.getExtraDependenciesDir());
                            v1alpha1Source.getSpec().setJava(v1alpha1SourceSpecJava);
                        }
                    }
                    if (!StringUtils.isEmpty(worker().getWorkerConfig().getBrokerClientAuthenticationPlugin())
                            && !StringUtils.isEmpty(
                            worker().getWorkerConfig().getBrokerClientAuthenticationParameters())) {
                        String authSecretName = KubernetesUtils.upsertSecret(apiKind.toLowerCase(), "auth",
                                v1alpha1Source.getSpec().getClusterName(), tenant, namespace, sourceName, worker());
                        v1alpha1Source.getSpec().getPulsar().setAuthSecret(authSecretName);
                    }
                    if (worker().getWorkerConfig().getTlsEnabled()) {
                        String tlsSecretName = KubernetesUtils.upsertSecret(apiKind.toLowerCase(), "tls",
                                v1alpha1Source.getSpec().getClusterName(), tenant, namespace, sourceName, worker());
                        v1alpha1Source.getSpec().getPulsar().setTlsSecret(tlsSecretName);
                    }
                    if (!StringUtils.isEmpty(customConfig.getDefaultServiceAccountName())
                            && StringUtils.isEmpty(podPolicy.getServiceAccountName())) {
                        podPolicy.setServiceAccountName(customConfig.getDefaultServiceAccountName());
                    }
                    if (customConfig.getImagePullSecrets() != null && !customConfig.getImagePullSecrets().isEmpty()) {
                        podPolicy.setImagePullSecrets(customConfig.asV1alpha1SourceSpecPodImagePullSecrets());
                    }
                    List<V1alpha1SourceSpecPodInitContainers> initContainersList =
                            customConfig.asV1alpha1SourceSpecPodInitContainers();
                    if (initContainersList != null && !initContainersList.isEmpty()) {
                        podPolicy.setInitContainers(initContainersList);
                    }
                    v1alpha1Source.getSpec().setPod(podPolicy);
                } catch (Exception e) {
                    log.error("Error create or update auth or tls secret for {} {}/{}/{}",
                            ComponentTypeUtils.toString(componentType), tenant, namespace, sourceName, e);


                    throw new RestException(Response.Status.INTERNAL_SERVER_ERROR,
                            String.format("Error create or update auth or tls secret %s %s:- %s",
                                    ComponentTypeUtils.toString(componentType), sourceName, e.getMessage()));
                }
            }
        }
        if (worker().getMeshWorkerServiceCustomConfig().isEnableTrustedMode()) {
            SourcesUtil.mergeTrustedConfigs(sourceConfig, v1alpha1Source);
        }
    }

    @Override
    List<FunctionInstanceStatsImpl> getComponentInstancesStats(String tenant, String namespace, String componentName) {
        validateSourceEnabled();
        List<FunctionInstanceStatsImpl> functionInstanceStatsList = new ArrayList<>();
        try {
            String nameSpaceName = worker().getJobNamespace();
            String hashName = CommonUtil.generateObjectName(worker(), tenant, namespace, componentName);
            V1alpha1Source v1alpha1Source = extractResponse(getResourceApi().get(nameSpaceName, hashName));
            try {
                validateResourceObject(v1alpha1Source);
            } catch (IllegalArgumentException e) {
                log.warn("get stats {}/{}/{} source failed", tenant, namespace, componentName, e);
                return functionInstanceStatsList;
            }
            V1alpha1SourceStatus v1alpha1SourceStatus = v1alpha1Source.getStatus();
            final V1StatefulSet v1StatefulSet = getFunctionStatefulSet(v1alpha1Source);
            try {
                validateStatefulSet(v1StatefulSet);
            } catch (IllegalArgumentException e) {
                log.warn("get stats {}/{}/{} source failed", tenant, namespace, componentName, e);
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
            V1PodList podList = getFunctionPods(tenant, namespace, componentName, v1alpha1SourceStatus);
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
            log.warn("Get source {} stats failed from namespace {}",
                    componentName, namespace, e);
        }
        return functionInstanceStatsList;
    }

    @Override
    void validateResourceObject(V1alpha1Source obj) throws IllegalArgumentException {
        if (obj == null) {
            throw new IllegalArgumentException("Source Resource is null");
        }
        if (obj.getMetadata() == null) {
            throw new IllegalArgumentException("Source Resource metadata is null");
        }
        if (obj.getSpec() == null) {
            throw new IllegalArgumentException("Source Resource spec is null");
        }
        if (obj.getStatus() == null) {
            throw new IllegalArgumentException("Source Resource status is null");
        }
    }

    public V1StatefulSet getFunctionStatefulSet(V1alpha1Source v1alpha1Source) {
        try {
            String nameSpaceName = worker().getJobNamespace();
            String jobName =
                    CommonUtil.makeJobName(v1alpha1Source.getMetadata().getName(), CommonUtil.COMPONENT_SOURCE);
            V1StatefulSet v1StatefulSet =
                    worker().getAppsV1Api().readNamespacedStatefulSet(jobName, nameSpaceName, null, null, null);
            if (validateResourceOwner(v1StatefulSet, v1alpha1Source)) {
                return v1StatefulSet;
            } else {
                log.warn("get source statefulset failed, not owned by the resource");
                return null;
            }
        } catch (Exception e) {
            log.error("get source statefulset failed, error: {}", e.getMessage());
        }
        return null;
    }

    public V1PodList getFunctionPods(String tenant, String namespace, String componentName,
                                     V1alpha1SourceStatus v1alpha1SourceStatus) {
        V1PodList podList = null;
        try {
            String nameSpaceName = worker().getJobNamespace();
            String functionLabelSelector = v1alpha1SourceStatus.getSelector();
            podList = worker().getCoreV1Api().listNamespacedPod(
                    nameSpaceName, null, null, null, null,
                    functionLabelSelector, null, null, null, null,
                    null);
        } catch (Exception e) {
            log.error("get source pods failed, {}/{}/{}", tenant, namespace, componentName, e);
        }
        return podList;
    }

    @VisibleForTesting
    protected Set<CompletableFuture<InstanceCommunication.FunctionStatus>> fetchSourceStatusFromGRPC(List<V1Pod> pods,
                                                                                                   String subdomain,
                                                                                                   String statefulSetName,
                                                                                                   String nameSpaceName,
                                                                                                   SourceStatus sourceStatus,
                                                                                                   V1alpha1Source v1alpha1Source,
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
            SourceStatus.SourceInstanceStatus sourceInstanceStatus = null;
            for (SourceStatus.SourceInstanceStatus ins : sourceStatus.getInstances()) {
                if (ins.getInstanceId() == shardId) {
                    sourceInstanceStatus = ins;
                    break;
                }
            }
            if (sourceInstanceStatus != null) {
                SourceInstanceStatusData sourceInstanceStatusData = sourceInstanceStatus.getStatus();
                V1PodStatus podStatus = pod.getStatus();
                if (v1alpha1Source.getSpec() != null && StringUtils.isNotEmpty(
                        v1alpha1Source.getSpec().getClusterName())) {
                    sourceInstanceStatusData.setWorkerId(v1alpha1Source.getSpec().getClusterName());
                }
                if (podStatus != null) {
                    sourceInstanceStatusData.setRunning(KubernetesUtils.isPodRunning(pod));
                    V1ContainerStatus containerStatus =
                            KubernetesUtils.extractDefaultContainerStatus(pod);
                    if (containerStatus != null) {
                        sourceInstanceStatusData.setNumRestarts(containerStatus.getRestartCount());
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
                        log.error("Get source {}-{} status from grpc failed from namespace {}: ",
                                statefulSetName,
                                shardId,
                                nameSpaceName,
                                e);
                        sourceInstanceStatusData.setError(e.getMessage());
                    } else if (fs != null) {
                        SourcesUtil.convertFunctionStatusToInstanceStatusData(fs, sourceInstanceStatusData);
                    }
                });
                completableFutureSet.add(future);
            } else {
                log.error(
                        "Get source {}-{} status failed from namespace {}, cannot find status for shardId"
                                + " {}",
                        statefulSetName,
                        shardId,
                        nameSpaceName,
                        shardId);
            }
        });
        return completableFutureSet;
    }

    @VisibleForTesting
    protected void fillSourceStatusByPendingPod(List<V1Pod> pods,
                                              String statefulSetName,
                                              String nameSpaceName,
                                              SourceStatus sourceStatus,
                                              V1alpha1Source v1alpha1Source) {
        pods.forEach(pod -> {
            String podName = KubernetesUtils.getPodName(pod);
            int shardId = CommonUtil.getShardIdFromPodName(podName);
            if (shardId == -1) {
                log.warn("shardId invalid {}", podName);
                return;
            }
            SourceStatus.SourceInstanceStatus sourceInstanceStatus = null;
            for (SourceStatus.SourceInstanceStatus ins : sourceStatus.getInstances()) {
                if (ins.getInstanceId() == shardId) {
                    sourceInstanceStatus = ins;
                    break;
                }
            }
            if (sourceInstanceStatus != null) {
                SourceInstanceStatusData sourceInstanceStatusData = sourceInstanceStatus.getStatus();
                V1PodStatus podStatus = pod.getStatus();
                if (podStatus != null) {
                    List<V1ContainerStatus> containerStatuses = podStatus.getContainerStatuses();
                    if (containerStatuses != null && !containerStatuses.isEmpty()) {
                        V1ContainerStatus containerStatus = null;
                        for (V1ContainerStatus s : containerStatuses) {
                            // TODO need more precise way to find the status
                            if (s.getImage().contains(v1alpha1Source.getSpec().getImage())) {
                                containerStatus = s;
                                break;
                            }
                        }
                        if (containerStatus != null) {
                            V1ContainerState state = containerStatus.getState();
                            if (state != null && state.getTerminated() != null) {
                                sourceInstanceStatusData.setError(state.getTerminated().getMessage());
                            } else if (state != null && state.getWaiting() != null) {
                                sourceInstanceStatusData.setError(state.getWaiting().getMessage());
                            } else {
                                V1ContainerState lastState = containerStatus.getLastState();
                                if (lastState != null && lastState.getTerminated() != null) {
                                    sourceInstanceStatusData.setError(
                                            lastState.getTerminated().getMessage());
                                } else if (lastState != null && lastState.getWaiting() != null) {
                                    sourceInstanceStatusData.setError(lastState.getWaiting().getMessage());
                                }
                            }
                            if (containerStatus.getRestartCount() != null) {
                                sourceInstanceStatusData.setNumRestarts(containerStatus.getRestartCount());
                            }
                        } else {
                            sourceInstanceStatusData.setError(podStatus.getPhase());
                        }
                    }
                }
            } else {
                log.error(
                        "Get source {}-{} status failed from namespace {}, cannot find status for shardId"
                                + " {}",
                        statefulSetName,
                        shardId,
                        nameSpaceName,
                        shardId);
            }
        });
    }
}
