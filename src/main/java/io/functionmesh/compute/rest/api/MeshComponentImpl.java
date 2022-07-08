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

import static com.google.common.base.Preconditions.checkNotNull;
import static io.functionmesh.compute.util.CommonUtil.COMPONENT_LABEL_CLAIM;
import static io.functionmesh.compute.util.CommonUtil.getCustomLabelClaimsSelector;
import static io.functionmesh.compute.util.PackageManagementServiceUtil.getPackageTypeFromComponentType;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.pulsar.functions.worker.rest.RestUtils.throwUnavailableException;
import io.functionmesh.compute.MeshWorkerService;
import io.functionmesh.compute.functions.models.V1alpha1FunctionList;
import io.functionmesh.compute.util.CommonUtil;
import io.functionmesh.compute.util.KubernetesUtils;
import io.functionmesh.compute.util.PackageManagementServiceUtil;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.util.generic.GenericKubernetesApi;
import io.kubernetes.client.util.generic.KubernetesApiResponse;
import java.io.InputStream;
import java.net.URI;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import javax.ws.rs.core.StreamingOutput;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import okhttp3.Call;
import okhttp3.Response;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.authentication.AuthenticationDataHttps;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.functions.FunctionState;
import org.apache.pulsar.common.functions.Resources;
import org.apache.pulsar.common.io.ConnectorDefinition;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.policies.data.FunctionInstanceStatsDataImpl;
import org.apache.pulsar.common.policies.data.FunctionInstanceStatsImpl;
import org.apache.pulsar.common.policies.data.FunctionStatsImpl;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.util.RestException;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.InstanceCommunication.MetricsData;
import org.apache.pulsar.functions.proto.InstanceControlGrpc;
import org.apache.pulsar.functions.proto.InstanceControlGrpc.InstanceControlFutureStub;
import org.apache.pulsar.functions.utils.ComponentTypeUtils;
import org.apache.pulsar.functions.worker.WorkerService;
import org.apache.pulsar.functions.worker.service.api.Component;

@Slf4j
public abstract class MeshComponentImpl<T extends io.kubernetes.client.common.KubernetesObject,
        K extends io.kubernetes.client.common.KubernetesListObject> implements Component<MeshWorkerService> {

    static final String API_GROUP = "compute.functionmesh.io";
    protected final Supplier<MeshWorkerService> meshWorkerServiceSupplier;
    protected final Function.FunctionDetails.ComponentType componentType;
    protected String apiVersion = "v1alpha1";
    protected String apiKind = "Function";
    protected String apiPlural = "functions";
    @Getter
    protected GenericKubernetesApi<T, K> resourceApi;

    MeshComponentImpl(Supplier<MeshWorkerService> meshWorkerServiceSupplier,
                      Function.FunctionDetails.ComponentType componentType) {
        this.meshWorkerServiceSupplier = meshWorkerServiceSupplier;
        // If you want to support function-mesh, this type needs to be changed
        this.componentType = componentType;
    }

    @Override
    public FunctionConfig getFunctionInfo(final String tenant,
                                          final String namespace,
                                          final String componentName,
                                          final String clientRole,
                                          final AuthenticationDataSource clientAuthenticationDataHttps) {

        FunctionConfig functionConfig = new FunctionConfig();
        return functionConfig;
    }

    @Override
    public void deregisterFunction(final String tenant,
                                   final String namespace,
                                   final String componentName,
                                   final String clientRole,
                                   AuthenticationDataHttps clientAuthenticationDataHttps) {
        this.validateGetInfoRequestParams(tenant, namespace, componentName, apiKind);

        this.validatePermission(tenant,
                namespace,
                clientRole,
                clientAuthenticationDataHttps,
                ComponentTypeUtils.toString(componentType));
        try {
            String clusterName = worker().getWorkerConfig().getPulsarFunctionsCluster();
            String nameSpaceName = worker().getJobNamespace();
            String hashName = CommonUtil.createObjectName(clusterName, tenant, namespace, componentName);
            getResourceApi().delete(nameSpaceName, hashName);

            if (worker().getMeshWorkerServiceCustomConfig().isUploadEnabled()) {
                PackageManagementServiceUtil.deletePackageFromPackageService(
                        worker().getBrokerAdmin(), getPackageTypeFromComponentType(componentType),
                        tenant, namespace, componentName);
            }

            if (!StringUtils.isEmpty(worker().getWorkerConfig().getBrokerClientAuthenticationPlugin())
                    && !StringUtils.isEmpty(worker().getWorkerConfig().getBrokerClientAuthenticationParameters())) {
                Call deleteAuthSecretCall = worker().getCoreV1Api()
                        .deleteNamespacedSecretCall(
                                KubernetesUtils.getUniqueSecretName(
                                        apiKind.toLowerCase(),
                                        "auth",
                                        DigestUtils.sha256Hex(
                                                KubernetesUtils.getSecretName(
                                                        clusterName, tenant, namespace, componentName))),
                                worker().getJobNamespace(),
                                null,
                                null,
                                30,
                                false,
                                null,
                                null,
                                null
                        );
                executeCall(deleteAuthSecretCall, null);
            }
            if (worker().getWorkerConfig().getTlsEnabled()) {
                Call deleteTlsSecretCall = worker().getCoreV1Api()
                        .deleteNamespacedSecretCall(
                                KubernetesUtils.getUniqueSecretName(
                                        apiKind.toLowerCase(),
                                        "tls",
                                        DigestUtils.sha256Hex(
                                                KubernetesUtils.getSecretName(
                                                        clusterName, tenant, namespace, componentName))),
                                worker().getJobNamespace(),
                                null,
                                null,
                                30,
                                false,
                                null,
                                null,
                                null
                        );
                executeCall(deleteTlsSecretCall, null);
            }
        } catch (Exception e) {
            log.error("deregister {}/{}/{} {} failed", tenant, namespace, componentName, apiPlural, e);
            throw new RestException(javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    public <R> R executeCall(Call call, Class<R> c) throws Exception {
        Response response;
        response = call.execute();
        if (response.isSuccessful() && response.body() != null) {
            String data = response.body().string();
            if (c == null) {
                return null;
            }
            return worker().getApiClient().getJSON().getGson().fromJson(data, c);
        } else if (response.code() == 409) {
            throw new RestException(javax.ws.rs.core.Response.Status.CONFLICT,
                    "This resource already exists, please change the name");
        } else {
            String body = response.body() != null ? response.body().string() : "";
            String err = String.format(
                    "failed to perform the request: responseCode: %s, responseMessage: %s, responseBody: %s",
                    response.code(), response.message(), body);
            throw new RestException(javax.ws.rs.core.Response.Status.BAD_REQUEST, err);
        }
    }

    public T extractResponse(KubernetesApiResponse<T> response) throws RestException {
        if (response.isSuccess()) {
            return response.getObject();
        } else if (response.getHttpStatusCode() == 409) {
            throw new RestException(javax.ws.rs.core.Response.Status.CONFLICT,
                    "This resource already exists, please change the name");
        } else {
            String err = String.format(
                    "failed to perform the request: responseCode: %s, responseMessage: %s",
                    response.getHttpStatusCode(), response.getStatus().getMessage());
            throw new RestException(javax.ws.rs.core.Response.Status.BAD_REQUEST, err);
        }
    }

    @Override
    public MeshWorkerService worker() {
        try {
            return checkNotNull(meshWorkerServiceSupplier.get());
        } catch (Throwable t) {
            log.info("Failed to get worker service", t);
            throw t;
        }
    }

    @Override
    public void stopFunctionInstance(final String tenant,
                                     final String namespace,
                                     final String componentName,
                                     final String instanceId,
                                     final URI uri,
                                     final String clientRole,
                                     final AuthenticationDataSource clientAuthenticationDataHttps) {

    }

    @Override
    public void startFunctionInstance(final String tenant,
                                      final String namespace,
                                      final String componentName,
                                      final String instanceId,
                                      final URI uri,
                                      final String clientRole,
                                      final AuthenticationDataSource clientAuthenticationDataHttps) {

    }

    @Override
    public void restartFunctionInstance(final String tenant,
                                        final String namespace,
                                        final String componentName,
                                        final String instanceId,
                                        final URI uri,
                                        final String clientRole,
                                        final AuthenticationDataSource clientAuthenticationDataHttps) {

    }

    @Override
    public void startFunctionInstances(final String tenant,
                                       final String namespace,
                                       final String componentName,
                                       final String clientRole,
                                       final AuthenticationDataSource clientAuthenticationDataHttps) {

    }

    @Override
    public void stopFunctionInstances(final String tenant,
                                      final String namespace,
                                      final String componentName,
                                      final String clientRole,
                                      final AuthenticationDataSource clientAuthenticationDataHttps) {

    }

    @Override
    public void restartFunctionInstances(final String tenant,
                                         final String namespace,
                                         final String componentName,
                                         final String clientRole,
                                         final AuthenticationDataSource clientAuthenticationDataHttps) {

    }

    @Override
    public FunctionStatsImpl getFunctionStats(final String tenant,
                                              final String namespace,
                                              final String componentName,
                                              final URI uri,
                                              final String clientRole,
                                              final AuthenticationDataSource clientAuthenticationDataHttps) {
        if (!isWorkerServiceAvailable()) {
            throwUnavailableException();
        }

        this.validatePermission(tenant,
                namespace,
                clientRole,
                clientAuthenticationDataHttps,
                ComponentTypeUtils.toString(componentType));
        this.validateTenantIsExist(tenant, namespace, componentName, clientRole);
        this.validateGetInfoRequestParams(tenant, namespace, componentName, ComponentTypeUtils.toString(componentType));

        FunctionStatsImpl functionStats = new FunctionStatsImpl();
        try {
            List<FunctionInstanceStatsImpl> instanceStatsList =
                    getComponentInstancesStats(tenant, namespace, componentName);
            for (FunctionInstanceStatsImpl instanceStats : instanceStatsList) {
                if (instanceStats != null) {
                    functionStats.addInstance(instanceStats);
                }
            }

            return functionStats.calculateOverall();
        } catch (Exception e) {
            log.error("{}/{}/{} Got Exception Getting Stats", tenant, namespace, componentName, e);
            throw new RestException(javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    @Override
    public FunctionInstanceStatsDataImpl getFunctionsInstanceStats(final String tenant,
                                                                   final String namespace,
                                                                   final String componentName,
                                                                   final String instanceId,
                                                                   final URI uri,
                                                                   final String clientRole,
                                                                   final AuthenticationDataSource
                                                                           clientAuthenticationDataHttps) {
        return new FunctionInstanceStatsDataImpl();
    }

    @Override
    public String triggerFunction(final String tenant,
                                  final String namespace,
                                  final String functionName,
                                  final String input,
                                  final InputStream uploadedInputStream,
                                  final String topic,
                                  final String clientRole,
                                  final AuthenticationDataSource clientAuthenticationDataHttps) {
        return "";
    }

    @Override
    public List<String> listFunctions(final String tenant,
                                      final String namespace,
                                      final String clientRole,
                                      final AuthenticationDataSource clientAuthenticationDataHttps) {
        List<String> result = new LinkedList<>();
        try {
            String labelSelector;
            String cluster = worker().getWorkerConfig().getPulsarFunctionsCluster();
            labelSelector = getCustomLabelClaimsSelector(cluster, tenant, namespace);
            Call call = worker().getCustomObjectsApi().listNamespacedCustomObjectCall(
                    API_GROUP,
                    apiVersion,
                    worker().getJobNamespace(), apiPlural,
                    "false",
                    null,
                    null,
                    labelSelector,
                    null,
                    null,
                    null,
                    false,
                    null);

            V1alpha1FunctionList list = executeCall(call, V1alpha1FunctionList.class);
            list.getItems().forEach(n -> result.add(n.getMetadata().getLabels().get(COMPONENT_LABEL_CLAIM)));
        } catch (Exception e) {
            log.error("failed to fetch functions list from namespace {}", namespace, e);
        }

        return result;
    }

    @Override
    public FunctionState getFunctionState(final String tenant,
                                          final String namespace,
                                          final String functionName,
                                          final String key,
                                          final String clientRole,
                                          final AuthenticationDataSource clientAuthenticationDataHttps) {
        // To do
        return new FunctionState();
    }

    @Override
    public void putFunctionState(final String tenant,
                                 final String namespace,
                                 final String functionName,
                                 final String key,
                                 final FunctionState state,
                                 final String clientRole,
                                 final AuthenticationDataSource clientAuthenticationDataHttps) {

    }

    @Override
    public void uploadFunction(final InputStream uploadedInputStream,
                               final String path,
                               String clientRole,
                               final AuthenticationDataSource clientAuthenticationDataHttps) {

    }

    @Override
    public StreamingOutput downloadFunction(String path,
                                            String clientRole,
                                            AuthenticationDataHttps clientAuthenticationDataHttps) {
        // To do
        return null;
    }

    @Override
    public StreamingOutput downloadFunction(String tenant,
                                            String namespace,
                                            String componentName,
                                            String clientRole,
                                            AuthenticationDataHttps clientAuthenticationDataHttps) {
        // To do
        return null;
    }

    @Override
    public List<ConnectorDefinition> getListOfConnectors() {
        return meshWorkerServiceSupplier.get().getConnectorsManager().getConnectorDefinitions();
    }

    @Override
    public void reloadConnectors(String clientRole, final AuthenticationDataSource clientAuthenticationDataHttps) {
        meshWorkerServiceSupplier.get().getConnectorsManager().reloadConnectors();
    }

    public boolean isSuperUser(String clientRole, AuthenticationDataSource authenticationDataSource) {
        if (clientRole != null) {
            try {
                if ((worker().getWorkerConfig().getSuperUserRoles() != null
                        && worker().getWorkerConfig().getSuperUserRoles().contains(clientRole))) {
                    return true;
                }
                return worker().getAuthorizationService().isSuperUser(clientRole, authenticationDataSource)
                        .get(worker().getWorkerConfig().getZooKeeperOperationTimeoutSeconds(), SECONDS);
            } catch (InterruptedException e) {
                log.warn("Time-out {} sec while checking the role {} is a super user role ",
                        worker().getWorkerConfig().getZooKeeperOperationTimeoutSeconds(), clientRole);
                throw new RestException(javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
            } catch (Exception e) {
                log.warn("Admin-client with Role - failed to check the role {} is a super user role {} ", clientRole,
                        e.getMessage(), e);
                throw new RestException(javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
            }
        }
        return false;
    }

    public boolean isAuthorizedRole(String tenant, String namespace, String clientRole,
                                    AuthenticationDataSource authenticationData) throws PulsarAdminException {
        if (worker().getWorkerConfig().isAuthorizationEnabled()) {
            // skip authorization if client role is super-user
            if (isSuperUser(clientRole, authenticationData)) {
                return true;
            }

            if (clientRole != null) {
                try {
                    TenantInfo tenantInfo = worker().getBrokerAdmin().tenants().getTenantInfo(tenant);
                    if (tenantInfo != null && worker().getAuthorizationService()
                            .isTenantAdmin(tenant, clientRole, tenantInfo, authenticationData).get()) {
                        return true;
                    }
                } catch (PulsarAdminException.NotFoundException | InterruptedException | ExecutionException e) {

                }
            }

            // check if role has permissions granted
            if (clientRole != null && authenticationData != null) {
                return allowFunctionOps(NamespaceName.get(tenant, namespace), clientRole, authenticationData);
            } else {
                return false;
            }
        }
        return true;
    }

    public boolean allowFunctionOps(NamespaceName namespaceName, String role,
                                    AuthenticationDataSource authenticationData) {
        try {
            switch (componentType) {
                case SINK:
                    return worker().getAuthorizationService().allowSinkOpsAsync(
                                    namespaceName, role, authenticationData)
                            .get(worker().getWorkerConfig().getZooKeeperOperationTimeoutSeconds(), SECONDS);
                case SOURCE:
                    return worker().getAuthorizationService().allowSourceOpsAsync(
                                    namespaceName, role, authenticationData)
                            .get(worker().getWorkerConfig().getZooKeeperOperationTimeoutSeconds(), SECONDS);
                case FUNCTION:
                default:
                    return worker().getAuthorizationService().allowFunctionOpsAsync(
                                    namespaceName, role, authenticationData)
                            .get(worker().getWorkerConfig().getZooKeeperOperationTimeoutSeconds(), SECONDS);
            }
        } catch (InterruptedException e) {
            log.warn("Time-out {} sec while checking function authorization on {} ",
                    worker().getWorkerConfig().getZooKeeperOperationTimeoutSeconds(), namespaceName);
            throw new RestException(javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
        } catch (Exception e) {
            log.warn("Admin-client with Role - {} failed to get function permissions for namespace - {}. {}", role,
                    namespaceName,
                    e.getMessage(), e);
            throw new RestException(javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    void validatePermission(String tenant,
                            String namespace,
                            String clientRole,
                            AuthenticationDataSource clientAuthenticationDataHttps,
                            String componentName) {
        try {
            if (!isAuthorizedRole(tenant, namespace, clientRole, clientAuthenticationDataHttps)) {
                log.warn("{}/{}/{} Client [{}] is not authorized to get {}", tenant, namespace,
                        componentName, clientRole, ComponentTypeUtils.toString(componentType));
                throw new RestException(javax.ws.rs.core.Response.Status.UNAUTHORIZED,
                        "client is not authorize to perform operation");
            }
        } catch (PulsarAdminException e) {
            log.error("{}/{}/{} Failed to authorize", tenant, namespace, componentName, e);
            throw new RestException(javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    void validateGetInfoRequestParams(
            String tenant, String namespace, String name, String type) {
        if (tenant == null) {
            throw new RestException(javax.ws.rs.core.Response.Status.BAD_REQUEST, "Tenant is not provided");
        }
        if (namespace == null) {
            throw new RestException(javax.ws.rs.core.Response.Status.BAD_REQUEST, "Namespace is not provided");
        }
        if (name == null) {
            throw new RestException(javax.ws.rs.core.Response.Status.BAD_REQUEST, type + " name is not provided");
        }
    }

    void validateTenantIsExist(String tenant, String namespace, String name, String clientRole) {
        try {
            // Check tenant exists
            worker().getBrokerAdmin().tenants().getTenantInfo(tenant);

        } catch (PulsarAdminException.NotAuthorizedException e) {
            log.error("{}/{}/{} Client [{}] is not authorized to operate {} on tenant", tenant, namespace,
                    name, clientRole, ComponentTypeUtils.toString(componentType));
            throw new RestException(javax.ws.rs.core.Response.Status.UNAUTHORIZED,
                    "client is not authorize to perform operation");
        } catch (PulsarAdminException.NotFoundException e) {
            log.error("{}/{}/{} Tenant {} does not exist", tenant, namespace, name, tenant);
            throw new RestException(javax.ws.rs.core.Response.Status.BAD_REQUEST, "Tenant does not exist");
        } catch (PulsarAdminException e) {
            log.error("{}/{}/{} Issues getting tenant data", tenant, namespace, name, e);
            throw new RestException(javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    void validateResources(Resources componentResources, Resources minResource, Resources maxResource) {
        if (componentResources != null) {
            if (minResource != null && (componentResources.getCpu() < minResource.getCpu()
                    || componentResources.getRam() < minResource.getRam())) {
                throw new RestException(javax.ws.rs.core.Response.Status.BAD_REQUEST,
                        "Resource is less than minimum requirement");
            }
            if (maxResource != null && (componentResources.getCpu() > maxResource.getCpu()
                    || componentResources.getRam() > maxResource.getRam())) {
                throw new RestException(javax.ws.rs.core.Response.Status.BAD_REQUEST,
                        "Resource is larger than max requirement");
            }
        }
    }

    boolean isWorkerServiceAvailable() {
        WorkerService workerService = meshWorkerServiceSupplier.get();
        if (workerService == null) {
            return false;
        }
        return workerService.isInitialized();
    }

    abstract List<FunctionInstanceStatsImpl> getComponentInstancesStats(String tenant, String namespace,
                                                                        String componentName);

    abstract void validateResourceObject(T obj) throws IllegalArgumentException;

    public Set<CompletableFuture<MetricsData>> fetchStatsFromGRPC(List<V1Pod> pods,
                                                                  String subdomain,
                                                                  String statefulSetName,
                                                                  String nameSpaceName,
                                                                  List<FunctionInstanceStatsImpl> statsList,
                                                                  ManagedChannel[] channel,
                                                                  InstanceControlFutureStub[] stub) {
        Set<CompletableFuture<MetricsData>> completableFutureSet = new HashSet<>();
        pods.forEach(pod -> {
            String podName = KubernetesUtils.getPodName(pod);
            int shardId = CommonUtil.getShardIdFromPodName(podName);
            int podIndex = pods.indexOf(pod);
            String address = KubernetesUtils.getServiceUrl(podName, subdomain, nameSpaceName);
            if (shardId == -1) {
                log.warn("shardId invalid {}", podName);
                return;
            }
            final FunctionInstanceStatsImpl functionInstanceStats =
                    statsList.stream().filter(v -> v.getInstanceId() == shardId).findFirst()
                            .orElse(null);
            if (functionInstanceStats != null) {
                // get status from grpc
                if (channel[podIndex] == null && stub[podIndex] == null) {
                    channel[podIndex] = ManagedChannelBuilder.forAddress(address, 9093)
                            .usePlaintext()
                            .build();
                    stub[podIndex] = InstanceControlGrpc.newFutureStub(channel[podIndex]);
                }
                CompletableFuture<MetricsData> future =
                        CommonUtil.getFunctionMetricsAsync(stub[podIndex]);
                future.whenComplete((fs, e) -> {
                    if (channel[podIndex] != null) {
                        log.debug("closing channel {}", podIndex);
                        channel[podIndex].shutdown();
                    }
                    if (e != null) {
                        log.warn("Get {}-{} stats from grpc failed from namespace {}",
                                statefulSetName,
                                shardId,
                                nameSpaceName,
                                e);
                    } else if (fs != null) {
                        CommonUtil.convertFunctionMetricsToFunctionInstanceStats(fs, functionInstanceStats);
                    }
                });
                completableFutureSet.add(future);
            } else {
                log.warn("Get {}-{} stats failed from namespace {}, cannot find status for shardId {}",
                        statefulSetName,
                        shardId,
                        nameSpaceName,
                        shardId);
            }
        });
        return completableFutureSet;
    }
}

