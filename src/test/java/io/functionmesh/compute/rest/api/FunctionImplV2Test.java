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
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import com.google.gson.Gson;
import io.functionmesh.compute.MeshWorkerService;
import io.functionmesh.compute.functions.models.V1alpha1Function;
import io.functionmesh.compute.functions.models.V1alpha1FunctionList;
import io.functionmesh.compute.functions.models.V1alpha1FunctionSpec;
import io.functionmesh.compute.functions.models.V1alpha1FunctionSpecInput;
import io.functionmesh.compute.functions.models.V1alpha1FunctionSpecJava;
import io.functionmesh.compute.functions.models.V1alpha1FunctionSpecOutput;
import io.functionmesh.compute.functions.models.V1alpha1FunctionSpecPod;
import io.functionmesh.compute.functions.models.V1alpha1FunctionSpecPodResources;
import io.functionmesh.compute.functions.models.V1alpha1FunctionStatus;
import io.functionmesh.compute.models.CustomRuntimeOptions;
import io.functionmesh.compute.models.MeshWorkerServiceCustomConfig;
import io.functionmesh.compute.util.CommonUtil;
import io.functionmesh.compute.util.FunctionsUtil;
import io.functionmesh.compute.util.PackageManagementServiceUtil;
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
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.common.functions.ConsumerConfig;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.functions.Resources;
import org.apache.pulsar.common.policies.data.FunctionStatsImpl;
import org.apache.pulsar.common.policies.data.FunctionStatus;
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
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({
        PackageManagementServiceUtil.class,
        CommonUtil.class})
@PowerMockIgnore({"javax.management.*"})
public class FunctionImplV2Test {
    private static final String tenant = "test-tenant";
    private static final String namespace = "test-namespace";
    private static final String function = "test-function";
    private static final String inputTopic = "test-input-topic";
    private static final String outputTopic = "test-output-topic";
    private static final String logTopic = "test-log-topic";
    private static final String pulsarFunctionCluster = "test-pulsar";
    private static final String kubernetesNamespace = "test";
    private static final String serviceAccount = "test-account";

    private static final String API_GROUP = "compute.functionmesh.io";
    private static final String apiVersion = "v1alpha1";
    private static final String apiFunctionKind = "Function";
    private static final String runnerImage = "custom-image";
    private static final String serviceAccountName = "custom-account-name";

    private MeshWorkerService meshWorkerService;
    private PulsarAdmin mockedPulsarAdmin;
    private Tenants mockedTenants;
    private Namespaces mockedNamespaces;
    private TenantInfo mockedTenantInfo;
    private Namespace mockedNamespace;
    private final List<String> namespaceList = new LinkedList<>();
    private FunctionsImpl resource;

    @Mock
    private GenericKubernetesApi<V1alpha1Function, V1alpha1FunctionList> mockedKubernetesApi;
    @Mock
    private CoreV1Api coreV1Api;

    @Mock
    private KubernetesApiResponse<V1alpha1Function> mockedKubernetesApiResponse;

    private V1StatefulSet functionStatefulSet;
    private V1StatefulSetSpec functionStatefulSetSpec;
    private V1StatefulSetStatus functionStatefulSetStatus;
    private V1ObjectMeta functionStatefulSetMetadata;
    private V1PodList functionPodList;

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
        MeshWorkerServiceCustomConfig meshWorkerServiceCustomConfig = mockMeshWorkerServiceCustomConfig();
        this.meshWorkerService = mock(MeshWorkerService.class);
        when(meshWorkerService.getWorkerConfig()).thenReturn(workerConfig);
        when(meshWorkerService.isInitialized()).thenReturn(true);
        when(meshWorkerService.getBrokerAdmin()).thenReturn(mockedPulsarAdmin);
        when(meshWorkerService.getJobNamespace()).thenReturn(kubernetesNamespace);
        when(meshWorkerService.getMeshWorkerServiceCustomConfig()).thenReturn(meshWorkerServiceCustomConfig);
        when(meshWorkerService.getCoreV1Api()).thenReturn(coreV1Api);

        initFunctionStatefulSet();

        this.resource = spy(new FunctionsImpl(() -> this.meshWorkerService));
        doReturn(mockedKubernetesApi).when(resource).getResourceApi();
        doReturn(functionStatefulSet).when(resource).getFunctionStatefulSet(any());
        doReturn(functionPodList).when(resource).getFunctionPods(any(), any(), any(), any());

        when(mockedKubernetesApi.get(anyString(), anyString())).thenReturn(mockedKubernetesApiResponse);
        when(mockedKubernetesApi.create(any())).thenReturn(mockedKubernetesApiResponse);
        when(mockedKubernetesApi.update(any())).thenReturn(mockedKubernetesApiResponse);
        when(mockedKubernetesApiResponse.isSuccess()).thenReturn(true);

        mockStaticMethod();
    }

    private void initFunctionStatefulSet() {
        this.functionStatefulSet = mock(V1StatefulSet.class);
        this.functionStatefulSetMetadata = mock(V1ObjectMeta.class);
        this.functionStatefulSetSpec = mock(V1StatefulSetSpec.class);
        this.functionStatefulSetStatus = mock(V1StatefulSetStatus.class);
        this.functionPodList = mock(V1PodList.class);

        when(functionStatefulSet.getMetadata()).thenReturn(functionStatefulSetMetadata);
        when(functionStatefulSet.getSpec()).thenReturn(functionStatefulSetSpec);
        when(functionStatefulSet.getStatus()).thenReturn(functionStatefulSetStatus);

        when(functionStatefulSetMetadata.getName()).thenReturn(function);

        when(functionStatefulSetSpec.getServiceName()).thenReturn(function);

        when(functionStatefulSetStatus.getReplicas()).thenReturn(1);

        V1Pod pod = createPod();
        List<V1Pod> pods = Collections.singletonList(pod);
        when(functionPodList.getItems()).thenReturn(pods);
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
        when(meshWorkerServiceCustomConfig.isFunctionEnabled()).thenReturn(true);
        when(meshWorkerServiceCustomConfig.isEnableTrustedMode()).thenReturn(true);
        return meshWorkerServiceCustomConfig;
    }

    private void mockStaticMethod() {
        PowerMockito.stub(PowerMockito.method(PackageManagementServiceUtil.class, "uploadPackageToPackageService"))
                .toReturn("test.jar");
        PowerMockito.stub(PowerMockito.method(PackageManagementServiceUtil.class, "deletePackageFromPackageService"))
                .toReturn(null);
        PowerMockito.stub(PowerMockito.method(CommonUtil.class, "downloadPackageFile")).toReturn(null);
        PowerMockito.stub(PowerMockito.method(CommonUtil.class, "getFilenameFromPackageMetadata"))
                .toReturn("test.jar");
    }

    @Test
    public void getFunctionStatsTest() {
        V1alpha1Function functionResource = mock(V1alpha1Function.class);
        V1alpha1FunctionStatus functionStatus = mock(V1alpha1FunctionStatus.class);
        V1ObjectMeta functionMeta = mock(V1ObjectMeta.class);
        V1alpha1FunctionSpec functionSpec = mock(V1alpha1FunctionSpec.class);

        when(functionResource.getStatus()).thenReturn(functionStatus);
        when(functionResource.getMetadata()).thenReturn(functionMeta);
        when(functionResource.getSpec()).thenReturn(functionSpec);

        when(mockedKubernetesApiResponse.getObject()).thenReturn(functionResource);
        doReturn(Collections.singleton(CompletableFuture.completedFuture(
                InstanceCommunication.MetricsData.newBuilder().build()))).when(resource)
                .fetchStatsFromGRPC(any(), any(), any(), any(), any(), any(), any());
        FunctionStatsImpl functionStats = this.resource.getFunctionStats(tenant, namespace, function, null, null, null);
        Assert.assertNotNull(functionStats);
        assertEquals(functionStats.instances.size(), 1);
    }

    @Test
    public void registerFunctionTest() {
        FunctionConfig functionConfig = mockFunctionConfig();

        V1alpha1Function functionResource = mock(V1alpha1Function.class);
        when(mockedKubernetesApiResponse.getObject()).thenReturn(functionResource);
        try {
            this.resource.registerFunction(tenant, namespace, function, null, null, functionConfig.getJar(),
                    functionConfig, null, null);
        } catch (
                RestException restException) {
            Assert.fail(String.format(
                    "register {}/{}/{} function failed, error message: {}",
                    tenant,
                    namespace,
                    function,
                    restException.getMessage()));
        }

        V1alpha1Function v1alpha1FunctionOrigin =
                FunctionsUtil.createV1alpha1FunctionFromFunctionConfig(apiFunctionKind, API_GROUP, apiVersion, function,
                        functionConfig.getJar(), functionConfig,
                        meshWorkerService.getWorkerConfig().getPulsarFunctionsCluster(), meshWorkerService);

        ArgumentCaptor<V1alpha1Function> v1alpha1FunctionArgumentCaptor =
                ArgumentCaptor.forClass(V1alpha1Function.class);
        verify(mockedKubernetesApi).create(v1alpha1FunctionArgumentCaptor.capture());
        V1alpha1Function v1alpha1FunctionFinal = v1alpha1FunctionArgumentCaptor.getValue();

        verifyParameterForCreate(v1alpha1FunctionOrigin, v1alpha1FunctionFinal);
    }

    @Test
    public void deregisterFunctionTest() throws Exception {
        V1alpha1Function functionResource = mock(V1alpha1Function.class);
        when(mockedKubernetesApiResponse.getObject()).thenReturn(functionResource);

        doReturn(functionResource).when(resource).executeCall(any(), any());
        when(meshWorkerService.getWorkerConfig().getBrokerClientAuthenticationPlugin()).thenReturn("auth-enable");
        when(meshWorkerService.getWorkerConfig().getBrokerClientAuthenticationParameters()).thenReturn(
                "auth-param-test");

        try {
            this.resource.deregisterFunction(tenant, namespace, function, null, null);
        } catch (Exception exception) {
            Assert.fail("Expected no exception to be thrown but got exception: " + exception);
        }
    }

    @Test
    public void updateFunctionTest() {
        FunctionConfig functionConfig = mockFunctionConfig();

        V1alpha1Function functionResource = mock(V1alpha1Function.class);
        V1ObjectMeta functionMeta = mock(V1ObjectMeta.class);

        when(functionResource.getMetadata()).thenReturn(functionMeta);
        when(functionResource.getMetadata().getResourceVersion()).thenReturn("899291");
        when(functionResource.getMetadata().getLabels()).thenReturn(Collections.singletonMap("foo", "bar"));

        when(mockedKubernetesApiResponse.getObject()).thenReturn(functionResource);

        try {
            this.resource.updateFunction(tenant, namespace, function, null, null, functionConfig.getJar(),
                    functionConfig, null, null, null);
        } catch (
                RestException restException) {
            Assert.fail(String.format(
                    "updateFunction {}/{}/{} sink failed, error message: {}",
                    tenant,
                    namespace,
                    function,
                    restException.getMessage()));
        }

        V1alpha1Function v1alpha1FunctionOrigin =
                FunctionsUtil.createV1alpha1FunctionFromFunctionConfig(apiFunctionKind, API_GROUP, apiVersion, function,
                        functionConfig.getJar(), functionConfig,
                        meshWorkerService.getWorkerConfig().getPulsarFunctionsCluster(), meshWorkerService);

        ArgumentCaptor<V1alpha1Function> v1alpha1FunctionArgumentCaptor =
                ArgumentCaptor.forClass(V1alpha1Function.class);
        verify(mockedKubernetesApi).update(v1alpha1FunctionArgumentCaptor.capture());
        V1alpha1Function v1alpha1FunctionFinal = v1alpha1FunctionArgumentCaptor.getValue();
        verifyParameterForUpdate(v1alpha1FunctionOrigin, v1alpha1FunctionFinal);
    }

    @Test
    public void getFunctionInfoTest() {
        V1alpha1Function functionResource = mock(V1alpha1Function.class);
        V1alpha1FunctionSpec functionSpec = buildV1alpha1FunctionSpecForGetFunctionInfo();

        when(functionResource.getSpec()).thenReturn(functionSpec);
        when(mockedKubernetesApiResponse.getObject()).thenReturn(functionResource);

        FunctionConfig functionConfig = this.resource.getFunctionInfo(tenant, namespace, function, null, null);
        Assert.assertNotNull(functionConfig);
        assertEquals(expectFunctionConfig(), functionConfig);
    }

    @Test
    public void getFunctionStatusTest() {
        V1alpha1Function functionResource = mock(V1alpha1Function.class);
        V1alpha1FunctionStatus v1alpha1FunctionStatus = mock(V1alpha1FunctionStatus.class);
        V1ObjectMeta v1ObjectMeta = mock(V1ObjectMeta.class);
        V1alpha1FunctionSpec v1alpha1FunctionSpec = mock(V1alpha1FunctionSpec.class);

        when(functionResource.getStatus()).thenReturn(v1alpha1FunctionStatus);
        when(functionResource.getMetadata()).thenReturn(v1ObjectMeta);
        when(functionResource.getSpec()).thenReturn(v1alpha1FunctionSpec);

        when(mockedKubernetesApiResponse.getObject()).thenReturn(functionResource);

        doReturn(Collections.singleton(CompletableFuture.completedFuture(
                InstanceCommunication.MetricsData.newBuilder().build()))).when(resource)
                .fetchFunctionStatusFromGRPC(any(), any(), any(), any(), any(), any(), any(), any());
        FunctionStatus functionStatus = this.resource.getFunctionStatus(tenant, namespace, function, null, null, null);
        Assert.assertNotNull(functionStatus);
        assertEquals(1, functionStatus.instances.size());
    }

    private FunctionConfig mockFunctionConfig() {
        FunctionConfig functionConfig = mock(FunctionConfig.class);

        when(functionConfig.getTenant()).thenReturn(tenant);
        when(functionConfig.getNamespace()).thenReturn(namespace);
        when(functionConfig.getName()).thenReturn(function);

        Resources resources = mockResources(2.0, 4096L, 1024L * 10);
        when(functionConfig.getResources()).thenReturn(resources);

        when(functionConfig.getJar()).thenReturn(String.format("function://public/default/%s@1.0", function));
        when(functionConfig.getClassName()).thenReturn("org.example.functions.testFunction");
        when(functionConfig.getInputs()).thenReturn(Collections.singletonList(inputTopic));
        when(functionConfig.getOutput()).thenReturn(outputTopic);
        when(functionConfig.getMaxPendingAsyncRequests()).thenReturn(1000);
        when(functionConfig.getLogTopic()).thenReturn(logTopic);
        when(functionConfig.getAutoAck()).thenReturn(false);

        when(functionConfig.getRetainKeyOrdering()).thenReturn(true);
        when(functionConfig.getSubscriptionPosition()).thenReturn(SubscriptionInitialPosition.Latest);
        when(functionConfig.getTimeoutMs()).thenReturn(1000L);
        when(functionConfig.getForwardSourceMessageProperty()).thenReturn(true);
        when(functionConfig.getRuntime()).thenReturn(FunctionConfig.Runtime.JAVA);
        when(functionConfig.getProcessingGuarantees()).thenReturn(FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE);
        when(functionConfig.getMaxMessageRetries()).thenReturn(3);
        when(functionConfig.getParallelism()).thenReturn(2);

        CustomRuntimeOptions customRuntimeOptions = new CustomRuntimeOptions();
        customRuntimeOptions.setRunnerImage(runnerImage);
        customRuntimeOptions.setServiceAccountName(serviceAccountName);
        when(functionConfig.getCustomRuntimeOptions()).thenReturn(new Gson().toJson(customRuntimeOptions));

        return functionConfig;
    }

    private Resources mockResources(Double cpu, Long ram, Long disk) {
        Resources resources = mock(Resources.class);
        when(resources.getCpu()).thenReturn(cpu);
        when(resources.getRam()).thenReturn(ram);
        when(resources.getDisk()).thenReturn(disk);
        return resources;
    }

    private void verifyParameterForCreate(V1alpha1Function v1alpha1FunctionOrigin,
                                          V1alpha1Function v1alpha1FunctionFinal) {
        v1alpha1FunctionOrigin.getSpec().setImage(runnerImage);
        v1alpha1FunctionOrigin.getSpec().getPod().setServiceAccountName(serviceAccountName);
        //if authenticationEnabled=true,v1alpha1FunctionOrigin should set pod policy

        Assert.assertEquals(v1alpha1FunctionOrigin, v1alpha1FunctionFinal);
    }

    private void verifyParameterForUpdate(V1alpha1Function v1alpha1FunctionOrigin,
                                          V1alpha1Function v1alpha1FunctionFinal) {
        v1alpha1FunctionOrigin.getSpec().setImage(runnerImage);
        v1alpha1FunctionOrigin.getSpec().getPod().setServiceAccountName(serviceAccountName);
        v1alpha1FunctionOrigin.getMetadata().setResourceVersion("899291");
        //if authenticationEnabled=true,v1alpha1FunctionOrigin should set pod policy

        Assert.assertEquals(v1alpha1FunctionOrigin, v1alpha1FunctionFinal);
    }

    private V1alpha1FunctionSpec buildV1alpha1FunctionSpecForGetFunctionInfo() {
        V1alpha1FunctionSpec functionSpec = mock(V1alpha1FunctionSpec.class);
        V1alpha1FunctionSpecInput functionSpecInput = mock(V1alpha1FunctionSpecInput.class);
        V1alpha1FunctionSpecOutput functionSpecOutput = mock(V1alpha1FunctionSpecOutput.class);
        V1alpha1FunctionSpecPod functionSpecPod = mock(V1alpha1FunctionSpecPod.class);
        V1alpha1FunctionSpecJava functionSpecJava = mock(V1alpha1FunctionSpecJava.class);
        V1alpha1FunctionSpecPodResources functionSpecPodResources = createResource();

        when(functionSpec.getReplicas()).thenReturn(1);
        when(functionSpec.getProcessingGuarantee()).thenReturn(
                V1alpha1FunctionSpec.ProcessingGuaranteeEnum.ATLEAST_ONCE);

        when(functionSpec.getInput()).thenReturn(functionSpecInput);
        when(functionSpecInput.getTopics()).thenReturn(Collections.singletonList(inputTopic));
        when(functionSpec.getOutput()).thenReturn(functionSpecOutput);
        when(functionSpecOutput.getTopic()).thenReturn(outputTopic);

        when(functionSpec.getClusterName()).thenReturn(pulsarFunctionCluster);
        when(functionSpec.getMaxReplicas()).thenReturn(2);

        when(functionSpec.getPod()).thenReturn(functionSpecPod);
        when(functionSpecPod.getServiceAccountName()).thenReturn(serviceAccount);

        when(functionSpec.getSubscriptionName()).thenReturn(outputTopic);
        when(functionSpec.getRetainKeyOrdering()).thenReturn(false);
        when(functionSpec.getRetainOrdering()).thenReturn(false);
        when(functionSpec.getCleanupSubscription()).thenReturn(false);
        when(functionSpec.getAutoAck()).thenReturn(false);
        when(functionSpec.getTimeout()).thenReturn(100);
        when(functionSpec.getLogTopic()).thenReturn(logTopic);
        when(functionSpec.getForwardSourceMessageProperty()).thenReturn(true);

        when(functionSpec.getJava()).thenReturn(functionSpecJava);
        when(functionSpecJava.getJar()).thenReturn("test.jar");
        when(functionSpecJava.getJarLocation()).thenReturn("public/default/test");

        when(functionSpec.getMaxMessageRetry()).thenReturn(3);
        when(functionSpec.getClassName()).thenReturn("org.example.functions.testFunction");

        when(functionSpec.getResources()).thenReturn(functionSpecPodResources);

        return functionSpec;
    }

    private V1alpha1FunctionSpecPodResources createResource() {
        V1alpha1FunctionSpecPodResources functionSpecPodResources = mock(V1alpha1FunctionSpecPodResources.class);
        when(functionSpecPodResources.getLimits()).thenReturn(new HashMap<String, Object>() {{
            put(CPU_KEY, "0.1");
            put(MEMORY_KEY, "2048");
        }});
        return functionSpecPodResources;
    }

    private FunctionConfig expectFunctionConfig() {
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

        return FunctionConfig.builder()
                .name(function)
                .namespace(namespace)
                .tenant(tenant)
                .parallelism(1)
                .processingGuarantees(FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE)
                .subName(outputTopic)
                .retainKeyOrdering(false)
                .retainOrdering(false)
                .cleanupSubscription(false)
                .autoAck(false)
                .timeoutMs(100L)
                .inputSpecs(inputSpecsExpect)
                .inputs(inputSpecsExpect.keySet())
                .output(outputTopic)
                .logTopic(logTopic)
                .forwardSourceMessageProperty(true)
                .runtime(FunctionConfig.Runtime.JAVA)
                .jar("public/default/test")
                .maxMessageRetries(3)
                .className("org.example.functions.testFunction")
                .resources(resourcesExpect)
                .customRuntimeOptions(customRuntimeOptionsJSON)
                .build();
    }
}
