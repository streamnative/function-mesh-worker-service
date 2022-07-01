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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import io.functionmesh.compute.MeshWorkerService;
import io.functionmesh.compute.functions.models.V1alpha1Function;
import io.functionmesh.compute.functions.models.V1alpha1FunctionList;
import io.functionmesh.compute.functions.models.V1alpha1FunctionSpec;
import io.functionmesh.compute.functions.models.V1alpha1FunctionStatus;
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
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.pulsar.client.admin.Namespaces;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.Tenants;
import org.apache.pulsar.common.policies.data.FunctionStatsImpl;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.functions.proto.InstanceCommunication;
import org.apache.pulsar.functions.worker.WorkerConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class FunctionImplV2Test {

    private static final String tenant = "test-tenant";
    private static final String namespace = "test-namespace";
    private static final String function = "test-function";
    private static final String outputTopic = "test-output-topic";
    private static final String pulsarFunctionCluster = "test-pulsar";
    private static final String kubernetesNamespace = "test";

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

        this.meshWorkerService = mock(MeshWorkerService.class);
        when(meshWorkerService.getWorkerConfig()).thenReturn(workerConfig);
        when(meshWorkerService.isInitialized()).thenReturn(true);
        when(meshWorkerService.getBrokerAdmin()).thenReturn(mockedPulsarAdmin);
        when(meshWorkerService.getJobNamespace()).thenReturn(kubernetesNamespace);

        initFunctionStatefulSet();

        this.resource = spy(new FunctionsImpl(() -> this.meshWorkerService));
        doReturn(mockedKubernetesApi).when(resource).getResourceApi();
        doReturn(functionStatefulSet).when(resource).getFunctionStatefulSet(any());
        doReturn(functionPodList).when(resource).getFunctionPods(any(), any(), any(), any());

        when(mockedKubernetesApi.get(anyString(), anyString())).thenReturn(mockedKubernetesApiResponse);
        when(mockedKubernetesApiResponse.isSuccess()).thenReturn(true);
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
        return workerConfig;
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
        Assert.assertEquals(functionStats.instances.size(), 1);
    }
}
