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
import io.functionmesh.compute.sinks.models.V1alpha1Sink;
import io.functionmesh.compute.sinks.models.V1alpha1SinkList;
import io.functionmesh.compute.sinks.models.V1alpha1SinkSpec;
import io.functionmesh.compute.sinks.models.V1alpha1SinkSpecInput;
import io.functionmesh.compute.sinks.models.V1alpha1SinkSpecJava;
import io.functionmesh.compute.sinks.models.V1alpha1SinkSpecPod;
import io.functionmesh.compute.sinks.models.V1alpha1SinkSpecPodResources;
import io.functionmesh.compute.sinks.models.V1alpha1SinkStatus;
import io.functionmesh.compute.util.SinksUtil;
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
import org.apache.pulsar.common.io.SinkConfig;
import org.apache.pulsar.common.policies.data.SinkStatus;
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
public class SinksImplV2Test {
    private static final String tenant = "test-tenant";
    private static final String namespace = "test-namespace";
    private static final String sinkName = "test-sink";
    private static final String inputTopic = "test-input-topic";
    private static final String outputTopic = "test-output-topic";
    private static final String logTopic = "test-log-topic";
    private static final String pulsarFunctionCluster = "test-pulsar";
    private static final String kubernetesNamespace = "test";
    private static final String serviceAccount = "test-account";

    private static final String apiGroup = "compute.functionmesh.io";
    private static final String apiVersion = "v1alpha1";
    private static final String apiSinkKind = "Sink";
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
    private SinksImpl resource;

    @Mock
    private GenericKubernetesApi<V1alpha1Sink, V1alpha1SinkList> mockedKubernetesApi;

    @Mock
    private KubernetesApiResponse<V1alpha1Sink> mockedKubernetesApiResponse;

    private V1StatefulSet sinkStatefulSet;
    private V1StatefulSetSpec sinkStatefulSetSpec;
    private V1StatefulSetStatus sinkStatefulSetStatus;
    private V1ObjectMeta sinkStatefulSetMetadata;
    private V1PodList sinkPodList;

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

        this.resource = spy(new SinksImpl(() -> this.meshWorkerService));
        doReturn(mockedKubernetesApi).when(resource).getResourceApi();
        doReturn(sinkStatefulSet).when(appsV1Api).readNamespacedStatefulSet(any(), any(), any(), any(), any());
        doReturn(sinkPodList).when(coreV1Api)
                .listNamespacedPod(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any());

        when(mockedKubernetesApi.get(anyString(), anyString())).thenReturn(mockedKubernetesApiResponse);
        when(mockedKubernetesApi.create(any())).thenReturn(mockedKubernetesApiResponse);
        when(mockedKubernetesApi.update(any())).thenReturn(mockedKubernetesApiResponse);
        when(mockedKubernetesApiResponse.isSuccess()).thenReturn(true);
    }

    private void initFunctionStatefulSet() {
        this.sinkStatefulSet = mock(V1StatefulSet.class);
        this.sinkStatefulSetMetadata = mock(V1ObjectMeta.class);
        this.sinkStatefulSetSpec = mock(V1StatefulSetSpec.class);
        this.sinkStatefulSetStatus = mock(V1StatefulSetStatus.class);
        this.sinkPodList = mock(V1PodList.class);

        when(sinkStatefulSet.getMetadata()).thenReturn(sinkStatefulSetMetadata);
        when(sinkStatefulSet.getSpec()).thenReturn(sinkStatefulSetSpec);
        when(sinkStatefulSet.getStatus()).thenReturn(sinkStatefulSetStatus);

        when(sinkStatefulSetMetadata.getName()).thenReturn(sinkName);

        when(sinkStatefulSetSpec.getServiceName()).thenReturn(sinkName);

        when(sinkStatefulSetStatus.getReplicas()).thenReturn(1);

        V1Pod pod = createPod();
        List<V1Pod> pods = Collections.singletonList(pod);
        when(sinkPodList.getItems()).thenReturn(pods);
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
        when(meshWorkerServiceCustomConfig.isSinkEnabled()).thenReturn(true);
        when(meshWorkerServiceCustomConfig.isEnableTrustedMode()).thenReturn(true);
        return meshWorkerServiceCustomConfig;
    }

    @Test
    public void registerSinkTest() {
        SinkConfig sinkConfig = buildSinkConfig();
        V1alpha1Sink mockV1alpha1Sink = mock(V1alpha1Sink.class);
        when(mockedKubernetesApiResponse.getObject()).thenReturn(mockV1alpha1Sink);
        try {
            this.resource.registerSink(tenant, namespace, sinkName, null, null, null, sinkConfig, null, null);
        } catch (RestException restException) {
            Assert.fail(String.format("register {}/{}/{} sink failed, error message: {}", tenant, namespace, sinkName,
                    restException.getMessage()));
        }

        V1alpha1Sink v1alpha1SinkOrigin =
                SinksUtil.createV1alpha1SkinFromSinkConfig(apiSinkKind, apiGroup, apiVersion, sinkName, null, null,
                        sinkConfig, null, meshWorkerService.getWorkerConfig().getPulsarFunctionsCluster(),
                        meshWorkerService);
        ArgumentCaptor<V1alpha1Sink> v1alpha1SinkArgumentCaptor = ArgumentCaptor.forClass(V1alpha1Sink.class);
        verify(mockedKubernetesApi).create(v1alpha1SinkArgumentCaptor.capture());
        V1alpha1Sink v1alpha1SinkFinal = v1alpha1SinkArgumentCaptor.getValue();
        verifyParameterForCreate(v1alpha1SinkOrigin, v1alpha1SinkFinal);
    }

    @Test
    public void updateSinkTest() {
        SinkConfig sinkConfig = buildSinkConfig();
        V1alpha1Sink mockV1alpha1Sink = mock(V1alpha1Sink.class);
        V1ObjectMeta mockV1ObjectMeta = mock(V1ObjectMeta.class);

        when(mockV1alpha1Sink.getMetadata()).thenReturn(mockV1ObjectMeta);
        when(mockV1alpha1Sink.getMetadata().getResourceVersion()).thenReturn(resourceVersionPre);
        when(mockV1alpha1Sink.getMetadata().getLabels()).thenReturn(Collections.singletonMap("foo", "bar"));
        when(mockedKubernetesApiResponse.getObject()).thenReturn(mockV1alpha1Sink);
        try {
            this.resource.updateSink(tenant, namespace, sinkName, null, null, null, sinkConfig, null, null, null);
        } catch (RestException restException) {
            Assert.fail(String.format("update {}/{}/{} sink failed, error message: {}", tenant, namespace, sinkName,
                    restException.getMessage()));
        }

        V1alpha1Sink v1alpha1SinkOrigin =
                SinksUtil.createV1alpha1SkinFromSinkConfig(apiSinkKind, apiGroup, apiVersion, sinkName, null, null,
                        sinkConfig, null, meshWorkerService.getWorkerConfig().getPulsarFunctionsCluster(),
                        meshWorkerService);
        ArgumentCaptor<V1alpha1Sink> v1alpha1SinkArgumentCaptor = ArgumentCaptor.forClass(V1alpha1Sink.class);
        verify(mockedKubernetesApi).update(v1alpha1SinkArgumentCaptor.capture());
        V1alpha1Sink v1alpha1SinkFinal = v1alpha1SinkArgumentCaptor.getValue();
        verifyParameterForUpdate(v1alpha1SinkOrigin, v1alpha1SinkFinal);
    }

    @Test
    public void getSinkStatusTest() {
        V1alpha1Sink mockV1alpha1Sink = mock(V1alpha1Sink.class);
        V1alpha1SinkStatus mockV1alpha1SinkStatus = mock(V1alpha1SinkStatus.class);
        V1ObjectMeta mockV1ObjectMeta = mock(V1ObjectMeta.class);
        V1alpha1SinkSpec mockV1alpha1SinkSpec = mock(V1alpha1SinkSpec.class);

        when(mockV1alpha1Sink.getMetadata()).thenReturn(mockV1ObjectMeta);
        when(mockV1alpha1Sink.getStatus()).thenReturn(mockV1alpha1SinkStatus);
        when(mockV1alpha1Sink.getSpec()).thenReturn(mockV1alpha1SinkSpec);
        when(mockedKubernetesApiResponse.getObject()).thenReturn(mockV1alpha1Sink);

        doReturn(Collections.singleton(CompletableFuture.completedFuture(
                InstanceCommunication.MetricsData.newBuilder().build()))).when(resource)
                .fetchSinkStatusFromGRPC(any(), any(), any(), any(), any(), any(), any(), any());
        SinkStatus sinkStatus = this.resource.getSinkStatus(tenant, namespace, sinkName, null, null, null);
        Assert.assertNotNull(sinkStatus);
        Assert.assertEquals(1, sinkStatus.instances.size());
    }

    @Test
    public void getSinkInfoTest() {
        V1alpha1Sink mockV1alpha1Sink = mock(V1alpha1Sink.class);
        V1alpha1SinkSpec mockV1alpha1SinkSpec = buildV1alpha1SinkSpecForGetSinkInfo();

        when(mockV1alpha1Sink.getSpec()).thenReturn(mockV1alpha1SinkSpec);
        when(mockedKubernetesApiResponse.getObject()).thenReturn(mockV1alpha1Sink);

        SinkConfig sinkConfig = this.resource.getSinkInfo(tenant, namespace, sinkName);
        Assert.assertNotNull(sinkConfig);
        Assert.assertEquals(expectSinkConfigForSinkInfo(), sinkConfig);
    }

    private SinkConfig buildSinkConfig() {
        SinkConfig sinkConfig = new SinkConfig();
        sinkConfig.setTenant(tenant);
        sinkConfig.setNamespace(namespace);
        sinkConfig.setName(sinkName);
        sinkConfig.setInputs(Collections.singletonList(inputTopic));
        sinkConfig.setClassName("org.example.functions.testSink");
        sinkConfig.setParallelism(1);
        sinkConfig.setConfigs(new HashMap<>());
        sinkConfig.setArchive("connectors/pulsar-io-elastic-search-2.7.0-rc-pm-3.nar");
        sinkConfig.setAutoAck(true);
        sinkConfig.setResources(new Resources(2.0, 4096L, 1024L * 10));

        CustomRuntimeOptions customRuntimeOptions = new CustomRuntimeOptions();
        customRuntimeOptions.setClusterName(pulsarFunctionCluster);
        customRuntimeOptions.setRunnerImage(runnerImage);
        customRuntimeOptions.setServiceAccountName(serviceAccountName);
        sinkConfig.setCustomRuntimeOptions(new Gson().toJson(customRuntimeOptions));
        return sinkConfig;
    }

    private V1alpha1SinkSpec buildV1alpha1SinkSpecForGetSinkInfo() {
        V1alpha1SinkSpec mockSinkSpec = mock(V1alpha1SinkSpec.class);
        V1alpha1SinkSpecInput mockSinkSpecInput = mock(V1alpha1SinkSpecInput.class);
        V1alpha1SinkSpecPod mockSinkSpecPod = mock(V1alpha1SinkSpecPod.class);
        V1alpha1SinkSpecJava mockSinkSpecJava = mock(V1alpha1SinkSpecJava.class);
        V1alpha1SinkSpecPodResources mockSinkSpecPodResources = createResource();

        when(mockSinkSpec.getReplicas()).thenReturn(1);
        when(mockSinkSpec.getProcessingGuarantee()).thenReturn(
                V1alpha1SinkSpec.ProcessingGuaranteeEnum.ATLEAST_ONCE);

        when(mockSinkSpec.getInput()).thenReturn(mockSinkSpecInput);
        when(mockSinkSpecInput.getTopics()).thenReturn(Collections.singletonList(inputTopic));

        when(mockSinkSpec.getClusterName()).thenReturn(pulsarFunctionCluster);
        when(mockSinkSpec.getMaxReplicas()).thenReturn(2);

        when(mockSinkSpec.getPod()).thenReturn(mockSinkSpecPod);
        when(mockSinkSpecPod.getServiceAccountName()).thenReturn(serviceAccount);

        when(mockSinkSpec.getSubscriptionName()).thenReturn(outputTopic);
        when(mockSinkSpec.getRetainOrdering()).thenReturn(false);
        when(mockSinkSpec.getCleanupSubscription()).thenReturn(false);
        when(mockSinkSpec.getAutoAck()).thenReturn(false);
        when(mockSinkSpec.getTimeout()).thenReturn(100);

        when(mockSinkSpec.getJava()).thenReturn(mockSinkSpecJava);
        when(mockSinkSpecJava.getJar()).thenReturn("test.jar");
        when(mockSinkSpecJava.getJarLocation()).thenReturn("public/default/test");

        when(mockSinkSpec.getMaxMessageRetry()).thenReturn(3);
        when(mockSinkSpec.getClassName()).thenReturn("org.example.functions.testFunction");

        when(mockSinkSpec.getResources()).thenReturn(mockSinkSpecPodResources);

        return mockSinkSpec;
    }

    private void verifyParameterForCreate(V1alpha1Sink v1alpha1SinkOrigin, V1alpha1Sink v1alpha1SinkFinal) {
        v1alpha1SinkOrigin.getSpec().setImage(runnerImage);
        v1alpha1SinkOrigin.getSpec().getPod().setServiceAccountName(serviceAccountName);
        //if authenticationEnabled=true,v1alpha1SinkOrigin should set pod policy

        Assert.assertEquals(v1alpha1SinkOrigin, v1alpha1SinkFinal);
    }


    private void verifyParameterForUpdate(V1alpha1Sink v1alpha1SinkOrigin, V1alpha1Sink v1alpha1SinkFinal) {
        v1alpha1SinkOrigin.getSpec().setImage(runnerImage);
        v1alpha1SinkOrigin.getSpec().getPod().setServiceAccountName(serviceAccountName);
        v1alpha1SinkOrigin.getMetadata().setResourceVersion("899291");
        //if authenticationEnabled=true,v1alpha1SinkOrigin should set pod policy

        Assert.assertEquals(v1alpha1SinkOrigin, v1alpha1SinkFinal);
    }

    private Resources mockResources(Double cpu, Long ram, Long disk) {
        Resources resources = mock(Resources.class);
        when(resources.getCpu()).thenReturn(cpu);
        when(resources.getRam()).thenReturn(ram);
        when(resources.getDisk()).thenReturn(disk);
        return resources;
    }

    private V1alpha1SinkSpecPodResources createResource() {
        V1alpha1SinkSpecPodResources mockSinkSpecPodResources = mock(V1alpha1SinkSpecPodResources.class);
        when(mockSinkSpecPodResources.getLimits()).thenReturn(new HashMap<String, Object>() {{
            put(CPU_KEY, "0.1");
            put(MEMORY_KEY, "2048");
        }});
        when(mockSinkSpecPodResources.getRequests()).thenReturn(new HashMap<String, Object>() {{
            put(CPU_KEY, "0.1");
            put(MEMORY_KEY, "2048");
        }});
        return mockSinkSpecPodResources;
    }

    private SinkConfig expectSinkConfigForSinkInfo() {
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

        return SinkConfig.builder()
                .name(sinkName)
                .namespace(namespace)
                .tenant(tenant)
                .parallelism(1)
                .processingGuarantees(FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE)
                .retainOrdering(false)
                .cleanupSubscription(false)
                .autoAck(false)
                .timeoutMs(100L)
                .sourceSubscriptionName(outputTopic)
                .archive("test.jar")
                .inputSpecs(inputSpecsExpect)
                .inputs(inputSpecsExpect.keySet())
                .maxMessageRetries(3)
                .className("org.example.functions.testFunction")
                .resources(resourcesExpect)
                .customRuntimeOptions(customRuntimeOptionsJSON)
                .build();
    }
}
