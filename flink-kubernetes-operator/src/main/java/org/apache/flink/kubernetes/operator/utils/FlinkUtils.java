/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.kubernetes.operator.utils;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.configuration.KubernetesDeploymentTarget;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.crd.spec.JobManagerSpec;
import org.apache.flink.kubernetes.operator.crd.spec.JobSpec;
import org.apache.flink.kubernetes.operator.crd.spec.Resource;
import org.apache.flink.kubernetes.operator.crd.spec.TaskManagerSpec;
import org.apache.flink.util.StringUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.internal.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Iterator;

/** Flink Utility methods used by the operator. */
public class FlinkUtils {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkUtils.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static Configuration getEffectiveConfig(FlinkDeployment flinkApp) {
        String namespace = flinkApp.getMetadata().getNamespace();
        String clusterId = flinkApp.getMetadata().getName();
        FlinkDeploymentSpec spec = flinkApp.getSpec();

        try {
            String flinkConfDir = System.getenv().get(ConfigConstants.ENV_FLINK_CONF_DIR);
            Configuration effectiveConfig =
                    flinkConfDir != null
                            ? GlobalConfiguration.loadConfiguration(flinkConfDir)
                            : new Configuration();

            // Parse config from spec's flinkConfiguration
            if (spec.getFlinkConfiguration() != null && !spec.getFlinkConfiguration().isEmpty()) {
                spec.getFlinkConfiguration().forEach(effectiveConfig::setString);
            }

            effectiveConfig.setString(KubernetesConfigOptions.NAMESPACE, namespace);
            effectiveConfig.setString(KubernetesConfigOptions.CLUSTER_ID, clusterId);

            // Web UI
            if (spec.getIngressDomain() != null) {
                effectiveConfig.set(
                        KubernetesConfigOptions.REST_SERVICE_EXPOSED_TYPE,
                        KubernetesConfigOptions.ServiceExposedType.ClusterIP);
            }

            // Image
            if (!StringUtils.isNullOrWhitespaceOnly(spec.getImage())) {
                effectiveConfig.set(KubernetesConfigOptions.CONTAINER_IMAGE, spec.getImage());
            }

            if (!StringUtils.isNullOrWhitespaceOnly(spec.getImagePullPolicy())) {
                effectiveConfig.set(
                        KubernetesConfigOptions.CONTAINER_IMAGE_PULL_POLICY,
                        KubernetesConfigOptions.ImagePullPolicy.valueOf(spec.getImagePullPolicy()));
            }

            // Common Pod template
            if (spec.getPodTemplate() != null) {
                effectiveConfig.set(
                        KubernetesConfigOptions.KUBERNETES_POD_TEMPLATE,
                        createTempFile(spec.getPodTemplate()));
            }

            // JM
            if (spec.getJobManager() != null) {
                setJobManager(spec.getJobManager(), spec.getPodTemplate(), effectiveConfig);
            }

            // TM
            if (spec.getTaskManager() != null) {
                setTaskManager(spec.getTaskManager(), spec.getPodTemplate(), effectiveConfig);
            }

            // Job or Session cluster
            setJobOrSessionCluster(spec.getJob(), effectiveConfig);

            return effectiveConfig;
        } catch (Exception e) {
            throw new RuntimeException("Failed to load configuration", e);
        }
    }

    private static void setJobOrSessionCluster(JobSpec jobSpec, Configuration effectiveConfig)
            throws URISyntaxException {
        if (jobSpec != null) {
            effectiveConfig.set(
                    DeploymentOptions.TARGET, KubernetesDeploymentTarget.APPLICATION.getName());
            final URI uri = new URI(jobSpec.getJarURI());
            effectiveConfig.set(PipelineOptions.JARS, Collections.singletonList(uri.toString()));

            if (jobSpec.getParallelism() > 0) {
                effectiveConfig.set(CoreOptions.DEFAULT_PARALLELISM, jobSpec.getParallelism());
            }
        } else {
            effectiveConfig.set(
                    DeploymentOptions.TARGET, KubernetesDeploymentTarget.SESSION.getName());
        }
    }

    private static void setJobManager(
            JobManagerSpec jobManagerSpec, Pod commonPod, Configuration effectiveConfig)
            throws IOException {
        if (jobManagerSpec != null) {
            setResource(jobManagerSpec.getResource(), effectiveConfig, true);
            setPodTemplate(commonPod, jobManagerSpec.getPodTemplate(), effectiveConfig, true);
            if (jobManagerSpec.getReplicas() > 0) {
                effectiveConfig.setInteger(
                        KubernetesConfigOptions.KUBERNETES_JOBMANAGER_REPLICAS,
                        jobManagerSpec.getReplicas());
            }
        }
    }

    private static void setTaskManager(
            TaskManagerSpec taskManagerSpec, Pod commonPod, Configuration effectiveConfig)
            throws IOException {
        if (taskManagerSpec != null) {
            setResource(taskManagerSpec.getResource(), effectiveConfig, false);
            setPodTemplate(commonPod, taskManagerSpec.getPodTemplate(), effectiveConfig, false);
            if (taskManagerSpec.getTaskSlots() > 0) {
                effectiveConfig.set(
                        TaskManagerOptions.NUM_TASK_SLOTS, taskManagerSpec.getTaskSlots());
            }
        }
    }

    private static void setResource(
            Resource resource, Configuration effectiveConfig, boolean isJM) {
        if (resource != null) {
            ConfigOption<MemorySize> memoryConfigOption =
                    isJM
                            ? JobManagerOptions.TOTAL_PROCESS_MEMORY
                            : TaskManagerOptions.TOTAL_PROCESS_MEMORY;
            ConfigOption<Double> cpuConfigOption =
                    isJM
                            ? KubernetesConfigOptions.JOB_MANAGER_CPU
                            : KubernetesConfigOptions.TASK_MANAGER_CPU;
            effectiveConfig.setString(memoryConfigOption.key(), resource.getMemory());
            effectiveConfig.setDouble(cpuConfigOption.key(), resource.getCpu());
        }
    }

    private static void setPodTemplate(
            Pod basicPod, Pod appendPod, Configuration effectiveConfig, boolean isJM)
            throws IOException {
        if (basicPod != null) {
            ConfigOption<String> podConfigOption =
                    isJM
                            ? KubernetesConfigOptions.JOB_MANAGER_POD_TEMPLATE
                            : KubernetesConfigOptions.TASK_MANAGER_POD_TEMPLATE;
            effectiveConfig.setString(
                    podConfigOption, createTempFile(mergePodTemplates(basicPod, appendPod)));
        }
    }

    private static String createTempFile(Pod podTemplate) throws IOException {
        File tmp = File.createTempFile("podTemplate_", ".yaml");
        Files.write(tmp.toPath(), SerializationUtils.dumpAsYaml(podTemplate).getBytes());
        tmp.deleteOnExit();
        return tmp.getAbsolutePath();
    }

    public static Pod mergePodTemplates(Pod toPod, Pod fromPod) {
        if (fromPod == null) {
            return toPod;
        } else if (toPod == null) {
            return fromPod;
        }
        JsonNode node1 = MAPPER.valueToTree(toPod);
        JsonNode node2 = MAPPER.valueToTree(fromPod);
        mergeInto(node1, node2);
        try {
            return MAPPER.treeToValue(node1, Pod.class);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private static void mergeInto(JsonNode toNode, JsonNode fromNode) {
        Iterator<String> fieldNames = fromNode.fieldNames();
        while (fieldNames.hasNext()) {
            String fieldName = fieldNames.next();
            JsonNode toChildNode = toNode.get(fieldName);
            JsonNode fromChildNode = fromNode.get(fieldName);

            if (toChildNode != null && toChildNode.isArray() && fromChildNode.isArray()) {
                // TODO: does merging arrays even make sense or should it just override?
                for (int i = 0; i < fromChildNode.size(); i++) {
                    JsonNode updatedChildNode = fromChildNode.get(i);
                    if (toChildNode.size() <= i) {
                        // append new node
                        ((ArrayNode) toChildNode).add(updatedChildNode);
                    }
                    mergeInto(toChildNode.get(i), updatedChildNode);
                }
            } else if (toChildNode != null && toChildNode.isObject()) {
                mergeInto(toChildNode, fromChildNode);
            } else {
                if (toNode instanceof ObjectNode) {
                    ((ObjectNode) toNode).replace(fieldName, fromChildNode);
                }
            }
        }
    }

    public static void deleteCluster(FlinkDeployment flinkApp, KubernetesClient kubernetesClient) {
        kubernetesClient
                .apps()
                .deployments()
                .inNamespace(flinkApp.getMetadata().getNamespace())
                .withName(flinkApp.getMetadata().getName())
                .cascading(true)
                .delete();
    }
}
