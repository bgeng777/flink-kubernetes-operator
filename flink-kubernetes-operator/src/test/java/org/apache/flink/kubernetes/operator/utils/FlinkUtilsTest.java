/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.configuration.KubernetesDeploymentTarget;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodSpec;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.kubernetes.operator.TestUtils.IMAGE;
import static org.apache.flink.kubernetes.operator.TestUtils.IMAGE_POLICY;
import static org.apache.flink.kubernetes.operator.TestUtils.SAMPLE_JAR;
import static org.apache.flink.kubernetes.operator.TestUtils.SERVICE_ACCOUNT;
import static org.apache.flink.kubernetes.operator.TestUtils.buildApplicationCluster;

/** FlinkUtilsTest. */
public class FlinkUtilsTest {

    @Test
    public void testMergePods() throws Exception {

        Container container1 = new Container();
        container1.setName("container1");
        Container container2 = new Container();
        container2.setName("container2");

        Pod pod1 = getTestPod("pod1 hostname", "pod1 api version", Arrays.asList(container2));

        Pod pod2 =
                getTestPod(
                        "pod2 hostname", "pod2 api version", Arrays.asList(container1, container2));

        Pod mergedPod = FlinkUtils.mergePodTemplates(pod1, pod2);

        Assert.assertEquals(pod2.getApiVersion(), mergedPod.getApiVersion());
        Assert.assertEquals(pod2.getSpec().getContainers(), mergedPod.getSpec().getContainers());
    }

    @Test
    public void testGetEffectiveConfig() throws Exception {
        FlinkDeployment flinkDeployment = buildApplicationCluster();
        Pod pod1 = getTestPod("pod1 hostname", "pod1 api version", new ArrayList<>());
        Pod pod2 = getTestPod("pod2 hostname", "pod2 api version", new ArrayList<>());

        flinkDeployment.getSpec().setPodTemplate(pod1);
        flinkDeployment.getSpec().setIngressDomain("test.com");
        flinkDeployment.getSpec().getTaskManager().setPodTemplate(pod2);
        flinkDeployment.getSpec().getJob().setParallelism(2);
        Configuration configuration = FlinkUtils.getEffectiveConfig(flinkDeployment);

        ObjectMapper om = new ObjectMapper(new YAMLFactory());
        Pod jmPod =
                om.readValue(
                        new File(
                                configuration.getString(
                                        KubernetesConfigOptions.JOB_MANAGER_POD_TEMPLATE)),
                        Pod.class);
        Pod tmPod =
                om.readValue(
                        new File(
                                configuration.getString(
                                        KubernetesConfigOptions.TASK_MANAGER_POD_TEMPLATE)),
                        Pod.class);

        // flinkConfiguration Map
        Assert.assertEquals(
                SERVICE_ACCOUNT,
                configuration.get(KubernetesConfigOptions.JOB_MANAGER_SERVICE_ACCOUNT));
        // Web UI
        Assert.assertEquals(
                KubernetesConfigOptions.ServiceExposedType.ClusterIP,
                configuration.get(KubernetesConfigOptions.REST_SERVICE_EXPOSED_TYPE));
        // Image
        Assert.assertEquals(IMAGE, configuration.get(KubernetesConfigOptions.CONTAINER_IMAGE));
        Assert.assertEquals(
                IMAGE_POLICY,
                configuration.get(KubernetesConfigOptions.CONTAINER_IMAGE_PULL_POLICY).toString());
        // Common/JM/TM Pod template
        Assert.assertEquals("pod1 api version", jmPod.getApiVersion());
        Assert.assertEquals("pod2 api version", tmPod.getApiVersion());

        // JM
        Assert.assertEquals(
                MemorySize.parse("2048m"),
                configuration.get(JobManagerOptions.TOTAL_PROCESS_MEMORY));
        Assert.assertEquals(
                Double.valueOf(1), configuration.get(KubernetesConfigOptions.JOB_MANAGER_CPU));
        Assert.assertEquals(
                Integer.valueOf(1),
                configuration.get(KubernetesConfigOptions.KUBERNETES_JOBMANAGER_REPLICAS));
        // TM
        Assert.assertEquals(
                MemorySize.parse("2048m"),
                configuration.get(TaskManagerOptions.TOTAL_PROCESS_MEMORY));
        Assert.assertEquals(
                Double.valueOf(1), configuration.get(KubernetesConfigOptions.TASK_MANAGER_CPU));
        Assert.assertEquals(
                Integer.valueOf(2), configuration.get(TaskManagerOptions.NUM_TASK_SLOTS));
        // Job
        Assert.assertEquals(
                KubernetesDeploymentTarget.APPLICATION.getName(),
                configuration.get(DeploymentOptions.TARGET));
        Assert.assertEquals(SAMPLE_JAR, configuration.get(PipelineOptions.JARS).get(0));
        Assert.assertEquals(Integer.valueOf(2), configuration.get(CoreOptions.DEFAULT_PARALLELISM));
    }

    private Pod getTestPod(String hostname, String apiVersion, List<Container> containers) {
        PodSpec podSpec = new PodSpec();
        podSpec.setHostname(hostname);
        podSpec.setContainers(containers);
        Pod pod = new Pod();
        pod.setApiVersion(apiVersion);
        pod.setSpec(podSpec);
        return pod;
    }
}
