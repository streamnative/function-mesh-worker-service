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
package io.functionmesh.compute.models;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.functionmesh.compute.functions.models.V1alpha1FunctionSpecPodImagePullSecrets;
import io.functionmesh.compute.functions.models.V1alpha1FunctionSpecPodInitContainers;
import io.functionmesh.compute.functions.models.V1alpha1FunctionSpecPodVolumeMounts;
import io.functionmesh.compute.functions.models.V1alpha1FunctionSpecPodVolumes;
import io.functionmesh.compute.sinks.models.V1alpha1SinkSpecPodImagePullSecrets;
import io.functionmesh.compute.sinks.models.V1alpha1SinkSpecPodInitContainers;
import io.functionmesh.compute.sinks.models.V1alpha1SinkSpecPodVolumeMounts;
import io.functionmesh.compute.sinks.models.V1alpha1SinkSpecPodVolumes;
import io.functionmesh.compute.sources.models.V1alpha1SourceSpecPodImagePullSecrets;
import io.functionmesh.compute.sources.models.V1alpha1SourceSpecPodInitContainers;
import io.functionmesh.compute.sources.models.V1alpha1SourceSpecPodVolumeMounts;
import io.functionmesh.compute.sources.models.V1alpha1SourceSpecPodVolumes;
import io.kubernetes.client.openapi.models.V1LocalObjectReference;
import io.kubernetes.client.openapi.models.V1Volume;
import io.kubernetes.client.openapi.models.V1VolumeMount;
import java.util.List;
import java.util.Map;
import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.pulsar.common.configuration.FieldContext;
import org.apache.pulsar.common.functions.Resources;
import org.apache.pulsar.common.util.ObjectMapperFactory;

@Data
@Accessors(chain = true)
public class MeshWorkerServiceCustomConfig {
    @FieldContext(
            doc = "Enable user to upload custom function/source/sink jar/nar"
    )
    protected boolean uploadEnabled = false;

    @FieldContext(
            doc = "Enable the function api endpoint"
    )
    protected boolean functionEnabled = true;

    @FieldContext(
            doc = "Enable the sink api endpoint"
    )
    protected boolean sinkEnabled = true;

    @FieldContext(
            doc = "Enable the source api endpoint"
    )
    protected boolean sourceEnabled = true;

    @FieldContext(
            doc = "the directory for dropping extra function dependencies. "
    )
    protected String extraDependenciesDir = "/pulsar/lib/";

    @FieldContext(
            doc = "VolumeMount describes a mounting of a Volume within a container."
    )
    protected List<V1VolumeMount> volumeMounts;

    @FieldContext(
            doc = "List of volumes that can be mounted by containers belonging to the function/connector pod."
    )
    protected List<V1Volume> volumes;

    @FieldContext(
            doc = "ownerReference"
    )
    protected Map<String, Object> ownerReference;

    @FieldContext(
            doc = "if allow user to change the service account name with custom-runtime-options"
    )
    @Deprecated
    protected boolean allowUserDefinedServiceAccountName = false;

    @FieldContext(
            doc = "ServiceAccountName is the name of the ServiceAccount to use to run function/connector pod."
    )
    protected String defaultServiceAccountName;

    @FieldContext(
            doc = "The image pull policy for image used to run function instance. By default it is `IfNotPresent`"
    )
    protected String imagePullPolicy;

    @FieldContext(
            doc = "the runner image to run function instance. By default it is "
                    + "streamnative/pulsar-functions-java-runner."
    )
    protected Map<String, String> functionRunnerImages;

    @FieldContext(
            doc = "ImagePullSecrets is an optional list of references to secrets in the same namespace to use for "
                    + "pulling any of the images used by this PodSpec."
    )
    protected List<V1LocalObjectReference> imagePullSecrets;

    @FieldContext(
            doc = "Labels specifies the labels to attach to pod the operator creates for the cluster."
    )
    protected Map<String, String> labels;

    @FieldContext(
            doc = "FunctionLabels specifies the labels to attach to function's pod, will override the labels if "
                    + "specified."
    )
    protected Map<String, String> functionLabels;

    @FieldContext(
            doc = "SinkLabels specifies the labels to attach to sink's pod, will override the labels if specified."
    )
    protected Map<String, String> sinkLabels;

    @FieldContext(
            doc = "SourceLabels specifies the labels to attach to source's pod, will override the labels if specified."
    )
    protected Map<String, String> sourceLabels;

    @FieldContext(
            doc = "Annotations specifies the annotations to attach to pods the operator creates"
    )
    protected Map<String, String> annotations;

    @FieldContext(
            doc = "FunctionAnnotations specifies the annotations to attach to function's pod, will override the "
                    + "annotations if specified."
    )
    protected Map<String, String> functionAnnotations;

    @FieldContext(
            doc = "SinkAnnotations specifies the annotations to attach to sink's pod, will override the annotations "
                    + "if specified."
    )
    protected Map<String, String> sinkAnnotations;

    @FieldContext(
            doc = "SourceAnnotations specifies the annotations to attach to source's pod, will override the "
                    + "annotations if specified."
    )
    protected Map<String, String> sourceAnnotations;

    @FieldContext(
            doc = "PodInitContainers specifies the initContainers to attach to function's pod, will override the "
                    + "initContainers if specified."
    )
    protected List<V1alpha1FunctionSpecPodInitContainers> functionInitContainers;

    @FieldContext(
            doc = "SourceInitContainers specifies the initContainers to attach to source's pod, will override the "
                    + "initContainers if specified."
    )
    protected List<V1alpha1SourceSpecPodInitContainers> sourceInitContainers;


    @FieldContext(
            doc = "SinkInitContainers specifies the initContainers to attach to sink's pod, will override the "
                    + "initContainers if specified."
    )
    protected List<V1alpha1SinkSpecPodInitContainers> sinkInitContainers;

    @FieldContext(
            doc = "The Kubernetes namespace to run the function instances. It is `default`, if this setting is left "
                    + "to be empty"
    )
    protected String jobNamespace;

    @FieldContext(
            doc = "The default resources for each function instance, if not specified, it will use the default "
                    + "resources (cpu: 1 core, ram: 1GB, disk: 10GB). Available configs are "
                    + "(cpu: in cores, ram: in bytes, disk: in bytes)."
    )
    protected Resources defaultResources;

    @FieldContext(
            doc = "Enable the trusted mode, by default it is false. With trusted mode enabled, "
                    + "the mesh worker service will allow user to override some of the default "
                    + "configs across the cluster. For example, with trusted mode, user can submit "
                    + "the function running on a customized runner image."
    )
    protected boolean enableTrustedMode = false;

    public List<V1alpha1SinkSpecPodVolumes> asV1alpha1SinkSpecPodVolumesList() throws JsonProcessingException {
        ObjectMapper objectMapper = ObjectMapperFactory.getThreadLocal();
        TypeReference<List<V1alpha1SinkSpecPodVolumes>> typeRef =
                new TypeReference<List<V1alpha1SinkSpecPodVolumes>>() {};
        String j = objectMapper.writeValueAsString(volumes);
        return objectMapper.readValue(j, typeRef);
    }

    public List<V1alpha1SourceSpecPodVolumes> asV1alpha1SourceSpecPodVolumesList() throws JsonProcessingException {
        ObjectMapper objectMapper = ObjectMapperFactory.getThreadLocal();
        TypeReference<List<V1alpha1SourceSpecPodVolumes>> typeRef =
                new TypeReference<List<V1alpha1SourceSpecPodVolumes>>() {};
        String j = objectMapper.writeValueAsString(volumes);
        return objectMapper.readValue(j, typeRef);
    }

    public List<V1alpha1FunctionSpecPodVolumes> asV1alpha1FunctionSpecPodVolumesList() throws JsonProcessingException {
        ObjectMapper objectMapper = ObjectMapperFactory.getThreadLocal();
        TypeReference<List<V1alpha1FunctionSpecPodVolumes>> typeRef =
                new TypeReference<List<V1alpha1FunctionSpecPodVolumes>>() {};
        String j = objectMapper.writeValueAsString(volumes);
        return objectMapper.readValue(j, typeRef);
    }

    public List<V1alpha1SinkSpecPodVolumeMounts> asV1alpha1SinkSpecPodVolumeMountsList()
            throws JsonProcessingException {
        ObjectMapper objectMapper = ObjectMapperFactory.getThreadLocal();
        TypeReference<List<V1alpha1SinkSpecPodVolumeMounts>> typeRef =
                new TypeReference<List<V1alpha1SinkSpecPodVolumeMounts>>() {};
        String j = objectMapper.writeValueAsString(volumeMounts);
        return objectMapper.readValue(j, typeRef);
    }

    public List<V1alpha1SourceSpecPodVolumeMounts> asV1alpha1SourceSpecPodVolumeMountsList()
            throws JsonProcessingException {
        ObjectMapper objectMapper = ObjectMapperFactory.getThreadLocal();
        TypeReference<List<V1alpha1SourceSpecPodVolumeMounts>> typeRef =
                new TypeReference<List<V1alpha1SourceSpecPodVolumeMounts>>() {};
        String j = objectMapper.writeValueAsString(volumeMounts);
        return objectMapper.readValue(j, typeRef);
    }

    public List<V1alpha1FunctionSpecPodVolumeMounts> asV1alpha1FunctionSpecPodVolumeMounts()
            throws JsonProcessingException {
        ObjectMapper objectMapper = ObjectMapperFactory.getThreadLocal();
        TypeReference<List<V1alpha1FunctionSpecPodVolumeMounts>> typeRef =
                new TypeReference<List<V1alpha1FunctionSpecPodVolumeMounts>>() {};
        String j = objectMapper.writeValueAsString(volumeMounts);
        return objectMapper.readValue(j, typeRef);
    }

    public List<V1alpha1FunctionSpecPodImagePullSecrets> asV1alpha1FunctionSpecPodImagePullSecrets()
            throws JsonProcessingException {
        ObjectMapper objectMapper = ObjectMapperFactory.getThreadLocal();
        TypeReference<List<V1alpha1FunctionSpecPodImagePullSecrets>> typeRef =
                new TypeReference<List<V1alpha1FunctionSpecPodImagePullSecrets>>() {};
        String j = objectMapper.writeValueAsString(imagePullSecrets);
        return objectMapper.readValue(j, typeRef);
    }

    public List<V1alpha1SinkSpecPodImagePullSecrets> asV1alpha1SinkSpecPodImagePullSecrets()
            throws JsonProcessingException {
        ObjectMapper objectMapper = ObjectMapperFactory.getThreadLocal();
        TypeReference<List<V1alpha1SinkSpecPodImagePullSecrets>> typeRef =
                new TypeReference<List<V1alpha1SinkSpecPodImagePullSecrets>>() {};
        String j = objectMapper.writeValueAsString(imagePullSecrets);
        return objectMapper.readValue(j, typeRef);
    }

    public List<V1alpha1SourceSpecPodImagePullSecrets> asV1alpha1SourceSpecPodImagePullSecrets()
            throws JsonProcessingException {
        ObjectMapper objectMapper = ObjectMapperFactory.getThreadLocal();
        TypeReference<List<V1alpha1SourceSpecPodImagePullSecrets>> typeRef =
                new TypeReference<List<V1alpha1SourceSpecPodImagePullSecrets>>() {};
        String j = objectMapper.writeValueAsString(imagePullSecrets);
        return objectMapper.readValue(j, typeRef);
    }

    public List<V1alpha1FunctionSpecPodInitContainers> asV1alpha1FunctionSpecPodInitContainers()
            throws JsonProcessingException {
        ObjectMapper objectMapper = ObjectMapperFactory.getThreadLocal();
        TypeReference<List<V1alpha1FunctionSpecPodInitContainers>> typeRef =
                new TypeReference<List<V1alpha1FunctionSpecPodInitContainers>>() {};
        String j = objectMapper.writeValueAsString(functionInitContainers);
        return objectMapper.readValue(j, typeRef);
    }

    public List<V1alpha1SourceSpecPodInitContainers> asV1alpha1SourceSpecPodInitContainers()
            throws JsonProcessingException {
        ObjectMapper objectMapper = ObjectMapperFactory.getThreadLocal();
        TypeReference<List<V1alpha1SourceSpecPodInitContainers>> typeRef =
                new TypeReference<List<V1alpha1SourceSpecPodInitContainers>>() {};
        String j = objectMapper.writeValueAsString(sourceInitContainers);
        return objectMapper.readValue(j, typeRef);
    }

    public List<V1alpha1SinkSpecPodInitContainers> asV1alpha1SinkSpecPodInitContainers()
            throws JsonProcessingException {
        ObjectMapper objectMapper = ObjectMapperFactory.getThreadLocal();
        TypeReference<List<V1alpha1SinkSpecPodInitContainers>> typeRef =
                new TypeReference<List<V1alpha1SinkSpecPodInitContainers>>() {};
        String j = objectMapper.writeValueAsString(functionInitContainers);
        return objectMapper.readValue(j, typeRef);
    }

    public Resources getDefaultResources() {
        if (defaultResources == null) {
            defaultResources = Resources.getDefaultResources();
        }
        return defaultResources;
    }
}
