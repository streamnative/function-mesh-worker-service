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

import static io.functionmesh.compute.util.FunctionsUtil.CPU_KEY;
import static io.functionmesh.compute.util.FunctionsUtil.MEMORY_KEY;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import com.google.gson.Gson;
import io.functionmesh.compute.MeshWorkerService;
import io.functionmesh.compute.models.CustomRuntimeOptions;
import io.functionmesh.compute.models.MeshWorkerServiceCustomConfig;
import io.functionmesh.compute.sources.models.V1alpha1Source;
import io.functionmesh.compute.sources.models.V1alpha1SourceList;
import io.functionmesh.compute.sources.models.V1alpha1SourceSpec;
import io.functionmesh.compute.sources.models.V1alpha1SourceSpecJava;
import io.functionmesh.compute.sources.models.V1alpha1SourceSpecPod;
import io.functionmesh.compute.sources.models.V1alpha1SourceSpecPodResources;
import io.functionmesh.compute.sources.models.V1alpha1SourceStatus;
import io.functionmesh.compute.util.SourcesUtil;
import io.kubernetes.client.openapi.apis.AppsV1Api;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1ContainerStatus;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodList;
import io.kubernetes.client.openapi.models.V1PodStatus;
import io.kubernetes.client.openapi.models.V1StatefulSet;
import io.kubernetes.client.openapi.models.V1StatefulSetSpec;
import io.kubernetes.client.openapi.models.V1StatefulSetStatus;
import io.kubernetes.client.util.generic.GenericKubernetesApi;
import io.kubernetes.client.util.generic.KubernetesApiResponse;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.pulsar.client.admin.Namespaces;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.Tenants;
import org.apache.pulsar.common.functions.ConsumerConfig;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.functions.Resources;
import org.apache.pulsar.common.io.SourceConfig;
import org.apache.pulsar.common.policies.data.SourceStatus;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.util.RestException;
import org.apache.pulsar.functions.proto.InstanceCommunication;
import org.apache.pulsar.functions.worker.WorkerConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.management.*"})
public class SourcesImplV2Test {
    private static final String tenant = "test-tenant";
    private static final String namespace = "test-namespace";
    private static final String sourceName = "test-source";
    private static final String inputTopic = "test-input-topic";
    private static final String outputTopic = "test-output-topic";
    private static final String logTopic = "test-log-topic";
    private static final String pulsarFunctionCluster = "test-pulsar";
    private static final String kubernetesNamespace = "test";
    private static final String serviceAccount = "test-account";

    private static final String apiGroup = "compute.functionmesh.io";
    private static final String apiVersion = "v1alpha1";
    private static final String apiSourceKind = "Source";
    private static final String runnerImage = "custom-image";
    private static final String serviceAccountName = "custom-account-name";
    private static final String resourceVersionPre = "899291";

    private MeshWorkerService meshWorkerService;
    private AppsV1Api appsV1Api;
    private CoreV1Api coreV1Api;
    private PulsarAdmin mockedPulsarAdmin;
    private Tenants mockedTenants;
    private Namespaces mockedNamespaces;
    private TenantInfo mockedTenantInfo;
    private Namespace mockedNamespace;
    private final List<String> namespaceList = new LinkedList<>();
    private SourcesImpl resource;

    @Mock
    private GenericKubernetesApi<V1alpha1Source, V1alpha1SourceList> mockedKubernetesApi;

    @Mock
    private KubernetesApiResponse<V1alpha1Source> mockedKubernetesApiResponse;

    private V1StatefulSet sourceStatefulSet;
    private V1StatefulSetSpec sourceStatefulSetSpec;
    private V1StatefulSetStatus sourceStatefulSetStatus;
    private V1ObjectMeta sourceStatefulSetMetadata;
    private V1PodList sourcePodList;

    @Before
    public void setup() throws Exception {
        MockitoAnnotations.initMocks(this);

        this.mockedTenantInfo = mock(TenantInfo.class);
        this.mockedPulsarAdmin = mock(PulsarAdmin.class);
        this.mockedNamespace = mock(Namespace.class);
        this.mockedTenants = mock(Tenants.class);
        this.mockedNamespaces = mock(Namespaces.class);
        namespaceList.add(tenant + "/" + namespace);
        when(mockedPulsarAdmin.tenants()).thenReturn(mockedTenants);
        when(mockedPulsarAdmin.namespaces()).thenReturn(mockedNamespaces);
        when(mockedTenants.getTenantInfo(any())).thenReturn(mockedTenantInfo);
        when(mockedNamespaces.getNamespaces(any())).thenReturn(namespaceList);

        WorkerConfig workerConfig = mockWorkerConfig();
        this.meshWorkerService = mock(MeshWorkerService.class);
        this.appsV1Api = mock(AppsV1Api.class);
        this.coreV1Api = mock(CoreV1Api.class);
        when(meshWorkerService.getAppsV1Api()).thenReturn(appsV1Api);
        when(meshWorkerService.getCoreV1Api()).thenReturn(coreV1Api);
        when(meshWorkerService.getWorkerConfig()).thenReturn(workerConfig);
        MeshWorkerServiceCustomConfig meshWorkerServiceCustomConfig = mockMeshWorkerServiceCustomConfig();
        when(meshWorkerService.getMeshWorkerServiceCustomConfig()).thenReturn(meshWorkerServiceCustomConfig);
        when(meshWorkerService.isInitialized()).thenReturn(true);
        when(meshWorkerService.getBrokerAdmin()).thenReturn(mockedPulsarAdmin);
        when(meshWorkerService.getJobNamespace()).thenReturn(kubernetesNamespace);

        initFunctionStatefulSet();

        this.resource = spy(new SourcesImpl(() -> this.meshWorkerService));
        doReturn(mockedKubernetesApi).when(resource).getResourceApi();
        doReturn(sourceStatefulSet).when(appsV1Api).readNamespacedStatefulSet(any(), any(), any(), any(), any());
        doReturn(sourcePodList).when(coreV1Api)
                .listNamespacedPod(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any());

        when(mockedKubernetesApi.get(anyString(), anyString())).thenReturn(mockedKubernetesApiResponse);
        when(mockedKubernetesApi.create(any())).thenReturn(mockedKubernetesApiResponse);
        when(mockedKubernetesApi.update(any())).thenReturn(mockedKubernetesApiResponse);
        when(mockedKubernetesApiResponse.isSuccess()).thenReturn(true);
    }

    private void initFunctionStatefulSet() {
        this.sourceStatefulSet = mock(V1StatefulSet.class);
        this.sourceStatefulSetMetadata = mock(V1ObjectMeta.class);
        this.sourceStatefulSetSpec = mock(V1StatefulSetSpec.class);
        this.sourceStatefulSetStatus = mock(V1StatefulSetStatus.class);
        this.sourcePodList = mock(V1PodList.class);

        when(sourceStatefulSet.getMetadata()).thenReturn(sourceStatefulSetMetadata);
        when(sourceStatefulSet.getSpec()).thenReturn(sourceStatefulSetSpec);
        when(sourceStatefulSet.getStatus()).thenReturn(sourceStatefulSetStatus);

        when(sourceStatefulSetMetadata.getName()).thenReturn(sourceName);

        when(sourceStatefulSetSpec.getServiceName()).thenReturn(sourceName);

        when(sourceStatefulSetStatus.getReplicas()).thenReturn(1);

        V1Pod pod = createPod();
        List<V1Pod> pods = Collections.singletonList(pod);
        when(sourcePodList.getItems()).thenReturn(pods);
    }

    private V1Pod createPod() {
        V1Pod pod = mock(V1Pod.class);
        V1PodStatus podStatus = mock(V1PodStatus.class);
        V1ContainerStatus containerStatus = mock(V1ContainerStatus.class);
        when(pod.getStatus()).thenReturn(podStatus);
        when(podStatus.getPhase()).thenReturn("Running");
        when(containerStatus.getReady()).thenReturn(true);
        when(podStatus.getContainerStatuses()).thenReturn(Collections.singletonList(containerStatus));
        return pod;
    }

    private WorkerConfig mockWorkerConfig() {
        WorkerConfig workerConfig = mock(WorkerConfig.class);
        when(workerConfig.isAuthorizationEnabled()).thenReturn(false);
        when(workerConfig.isAuthenticationEnabled()).thenReturn(false);
        when(workerConfig.getPulsarFunctionsCluster()).thenReturn(pulsarFunctionCluster);

        Resources minResources = mockResources(1.0, 1024L, 1024L * 10);
        Resources maxResources = mockResources(16.0, 1024L * 32, 1024L * 100);
        when(workerConfig.getFunctionInstanceMinResources()).thenReturn(minResources);
        when(workerConfig.getFunctionInstanceMaxResources()).thenReturn(maxResources);
        when(workerConfig.getDownloadDirectory()).thenReturn("/tmp");
        return workerConfig;
    }

    private MeshWorkerServiceCustomConfig mockMeshWorkerServiceCustomConfig() {
        MeshWorkerServiceCustomConfig meshWorkerServiceCustomConfig = mock(MeshWorkerServiceCustomConfig.class);
        when(meshWorkerServiceCustomConfig.isUploadEnabled()).thenReturn(true);
        when(meshWorkerServiceCustomConfig.isSourceEnabled()).thenReturn(true);
        when(meshWorkerServiceCustomConfig.isEnableTrustedMode()).thenReturn(true);
        return meshWorkerServiceCustomConfig;
    }

    @Test
    public void registerSourceTest() {
        SourceConfig sourceConfig = buildSourceConfig();
        V1alpha1Source mockV1alpha1Source = mock(V1alpha1Source.class);
        when(mockedKubernetesApiResponse.getObject()).thenReturn(mockV1alpha1Source);
        try {
            this.resource.registerSource(tenant, namespace, sourceName, null, null, null, sourceConfig, null, null);
        } catch (RestException restException) {
            Assert.fail(
                    String.format("register {}/{}/{} source failed, error message: {}", tenant, namespace, sourceName,
                            restException.getMessage()));
        }

        V1alpha1Source v1alpha1SourceOrigin =
                SourcesUtil.createV1alpha1SourceFromSourceConfig(apiSourceKind, apiGroup, apiVersion, sourceName, null,
                        null,
                        sourceConfig, null, meshWorkerService.getWorkerConfig().getPulsarFunctionsCluster(),
                        meshWorkerService);
        ArgumentCaptor<V1alpha1Source> v1alpha1SourceArgumentCaptor = ArgumentCaptor.forClass(V1alpha1Source.class);
        verify(mockedKubernetesApi).create(v1alpha1SourceArgumentCaptor.capture());
        V1alpha1Source v1alpha1SourceFinal = v1alpha1SourceArgumentCaptor.getValue();
        verifyParameterForCreate(v1alpha1SourceOrigin, v1alpha1SourceFinal);
    }

    @Test
    public void updateSourceTest() {
        SourceConfig sourceConfig = buildSourceConfig();
        V1alpha1Source mockV1alpha1Source = mock(V1alpha1Source.class);
        V1ObjectMeta mockV1ObjectMeta = mock(V1ObjectMeta.class);

        when(mockV1alpha1Source.getMetadata()).thenReturn(mockV1ObjectMeta);
        when(mockV1alpha1Source.getMetadata().getResourceVersion()).thenReturn(resourceVersionPre);
        when(mockV1alpha1Source.getMetadata().getLabels()).thenReturn(Collections.singletonMap("foo", "bar"));
        when(mockedKubernetesApiResponse.getObject()).thenReturn(mockV1alpha1Source);
        try {
            this.resource.updateSource(tenant, namespace, sourceName, null, null, null, sourceConfig, null, null, null);
        } catch (RestException restException) {
            Assert.fail(String.format("update {}/{}/{} source failed, error message: {}", tenant, namespace, sourceName,
                    restException.getMessage()));
        }

        V1alpha1Source v1alpha1SourceOrigin =
                SourcesUtil.createV1alpha1SourceFromSourceConfig(apiSourceKind, apiGroup, apiVersion, sourceName, null,
                        null,
                        sourceConfig, null, meshWorkerService.getWorkerConfig().getPulsarFunctionsCluster(),
                        meshWorkerService);
        ArgumentCaptor<V1alpha1Source> v1alpha1SourceArgumentCaptor = ArgumentCaptor.forClass(V1alpha1Source.class);
        verify(mockedKubernetesApi).update(v1alpha1SourceArgumentCaptor.capture());
        V1alpha1Source v1alpha1SourceFinal = v1alpha1SourceArgumentCaptor.getValue();
        verifyParameterForUpdate(v1alpha1SourceOrigin, v1alpha1SourceFinal);
    }

    @Test
    public void getSourceStatusTest() {
        V1alpha1Source mockV1alpha1Source = mock(V1alpha1Source.class);
        V1alpha1SourceStatus mockV1alpha1SourceStatus = mock(V1alpha1SourceStatus.class);
        V1ObjectMeta mockV1ObjectMeta = mock(V1ObjectMeta.class);
        V1alpha1SourceSpec mockV1alpha1SourceSpec = mock(V1alpha1SourceSpec.class);

        when(mockV1alpha1Source.getMetadata()).thenReturn(mockV1ObjectMeta);
        when(mockV1alpha1Source.getStatus()).thenReturn(mockV1alpha1SourceStatus);
        when(mockV1alpha1Source.getSpec()).thenReturn(mockV1alpha1SourceSpec);
        when(mockedKubernetesApiResponse.getObject()).thenReturn(mockV1alpha1Source);

        doReturn(Collections.singleton(CompletableFuture.completedFuture(
                InstanceCommunication.MetricsData.newBuilder().build()))).when(resource)
                .fetchSourceStatusFromGRPC(any(), any(), any(), any(), any(), any(), any(), any());
        SourceStatus sourceStatus = this.resource.getSourceStatus(tenant, namespace, sourceName, null, null, null);
        Assert.assertNotNull(sourceStatus);
        Assert.assertEquals(1, sourceStatus.instances.size());
    }

    @Test
    public void getSourceInfoTest() {
        V1alpha1Source mockV1alpha1Source = mock(V1alpha1Source.class);
        V1alpha1SourceSpec mockV1alpha1SourceSpec = buildV1alpha1SourceSpecForGetSourceInfo();

        when(mockV1alpha1Source.getSpec()).thenReturn(mockV1alpha1SourceSpec);
        when(mockedKubernetesApiResponse.getObject()).thenReturn(mockV1alpha1Source);

        SourceConfig sourceConfig = this.resource.getSourceInfo(tenant, namespace, sourceName);
        Assert.assertNotNull(sourceConfig);
        Assert.assertEquals(expectSourceConfigForSourceInfo(), sourceConfig);
    }

    private SourceConfig buildSourceConfig() {
        SourceConfig sourceConfig = new SourceConfig();
        sourceConfig.setTenant(tenant);
        sourceConfig.setNamespace(namespace);
        sourceConfig.setName(sourceName);
        sourceConfig.setTopicName(inputTopic);
        sourceConfig.setClassName("org.example.functions.testSource");
        sourceConfig.setParallelism(1);
        sourceConfig.setConfigs(new HashMap<>());
        sourceConfig.setArchive("connectors/pulsar-io-elastic-search-2.7.0-rc-pm-3.nar");
        sourceConfig.setResources(new Resources(2.0, 4096L, 1024L * 10));

        CustomRuntimeOptions customRuntimeOptions = new CustomRuntimeOptions();
        customRuntimeOptions.setClusterName(pulsarFunctionCluster);
        customRuntimeOptions.setRunnerImage(runnerImage);
        customRuntimeOptions.setServiceAccountName(serviceAccountName);
        sourceConfig.setCustomRuntimeOptions(new Gson().toJson(customRuntimeOptions));
        return sourceConfig;
    }

    private V1alpha1SourceSpec buildV1alpha1SourceSpecForGetSourceInfo() {
        V1alpha1SourceSpec mockSourceSpec = mock(V1alpha1SourceSpec.class);
        V1alpha1SourceSpecPod mockSourceSpecPod = mock(V1alpha1SourceSpecPod.class);
        V1alpha1SourceSpecJava mockSourceSpecJava = mock(V1alpha1SourceSpecJava.class);
        V1alpha1SourceSpecPodResources mockSourceSpecPodResources = createResource();

        when(mockSourceSpec.getReplicas()).thenReturn(1);
        when(mockSourceSpec.getProcessingGuarantee()).thenReturn(
                V1alpha1SourceSpec.ProcessingGuaranteeEnum.ATLEAST_ONCE);

        when(mockSourceSpec.getClusterName()).thenReturn(pulsarFunctionCluster);
        when(mockSourceSpec.getMaxReplicas()).thenReturn(2);

        when(mockSourceSpec.getPod()).thenReturn(mockSourceSpecPod);
        when(mockSourceSpecPod.getServiceAccountName()).thenReturn(serviceAccount);

        when(mockSourceSpec.getJava()).thenReturn(mockSourceSpecJava);
        when(mockSourceSpecJava.getJar()).thenReturn("test.jar");
        when(mockSourceSpecJava.getJarLocation()).thenReturn("public/default/test");

        when(mockSourceSpec.getClassName()).thenReturn("org.example.functions.testFunction");

        when(mockSourceSpec.getResources()).thenReturn(mockSourceSpecPodResources);

        return mockSourceSpec;
    }

    private void verifyParameterForCreate(V1alpha1Source v1alpha1SourceOrigin, V1alpha1Source v1alpha1SourceFinal) {
        v1alpha1SourceOrigin.getSpec().setImage(runnerImage);
        v1alpha1SourceOrigin.getSpec().getPod().setServiceAccountName(serviceAccountName);
        //if authenticationEnabled=true,v1alpha1SourceOrigin should set pod policy

        Assert.assertEquals(v1alpha1SourceOrigin, v1alpha1SourceFinal);
    }


    private void verifyParameterForUpdate(V1alpha1Source v1alpha1SourceOrigin, V1alpha1Source v1alpha1SourceFinal) {
        v1alpha1SourceOrigin.getSpec().setImage(runnerImage);
        v1alpha1SourceOrigin.getSpec().getPod().setServiceAccountName(serviceAccountName);
        v1alpha1SourceOrigin.getMetadata().setResourceVersion("899291");
        //if authenticationEnabled=true,v1alpha1SourceOrigin should set pod policy

        Assert.assertEquals(v1alpha1SourceOrigin, v1alpha1SourceFinal);
    }

    private Resources mockResources(Double cpu, Long ram, Long disk) {
        Resources resources = mock(Resources.class);
        when(resources.getCpu()).thenReturn(cpu);
        when(resources.getRam()).thenReturn(ram);
        when(resources.getDisk()).thenReturn(disk);
        return resources;
    }

    private V1alpha1SourceSpecPodResources createResource() {
        V1alpha1SourceSpecPodResources mockSourceSpecPodResources = mock(V1alpha1SourceSpecPodResources.class);
        when(mockSourceSpecPodResources.getLimits()).thenReturn(new HashMap<String, Object>() {{
            put(CPU_KEY, "0.1");
            put(MEMORY_KEY, "2048");
        }});
        when(mockSourceSpecPodResources.getRequests()).thenReturn(new HashMap<String, Object>() {{
            put(CPU_KEY, "0.1");
            put(MEMORY_KEY, "2048");
        }});
        return mockSourceSpecPodResources;
    }

    private SourceConfig expectSourceConfigForSourceInfo() {
        CustomRuntimeOptions customRuntimeOptionsExpect = new CustomRuntimeOptions();
        customRuntimeOptionsExpect.setClusterName(pulsarFunctionCluster);
        customRuntimeOptionsExpect.setMaxReplicas(2);
        customRuntimeOptionsExpect.setServiceAccountName(serviceAccount);
        String customRuntimeOptionsJSON = new Gson().toJson(customRuntimeOptionsExpect, CustomRuntimeOptions.class);

        Resources resourcesExpect = new Resources();
        resourcesExpect.setCpu(0.1);
        resourcesExpect.setRam(2048L);

        Map<String, ConsumerConfig> inputSpecsExpect = new HashMap<>();
        inputSpecsExpect.put(inputTopic, new ConsumerConfig());

        return SourceConfig.builder()
                .name(sourceName)
                .namespace(namespace)
                .tenant(tenant)
                .parallelism(1)
                .processingGuarantees(FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE)
                .archive("test.jar")
                .className("org.example.functions.testFunction")
                .resources(resourcesExpect)
                .customRuntimeOptions(customRuntimeOptionsJSON)
                .build();
    }

}
