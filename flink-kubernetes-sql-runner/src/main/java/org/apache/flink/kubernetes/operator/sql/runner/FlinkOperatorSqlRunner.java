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

package org.apache.flink.kubernetes.operator.sql.runner;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Arrays;

/** Main Class for FlinkOperatorSqlRunner. */
public class FlinkOperatorSqlRunner {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkOperatorSqlRunner.class);

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            return;
        }
        final String[] sqlStatements =
                Arrays.stream(args[0].split(";")).map(String::trim).toArray(String[]::new);
        final Configuration configuration = loadConfiguration(null);
        TableEnvironment tableEnvironment = TableEnvironment.create(configuration);
        LOG.info("Start the execution of SQL statements.");
        for (String statement : sqlStatements) {
            LOG.debug("SQL statement to execute: {}", statement);
            tableEnvironment.executeSql(statement);
        }
        LOG.info("Finish the execution of SQL statements.");
    }

    private static Configuration loadConfiguration(@Nullable Configuration dynamicParameters) {
        // TODO: check if it is possible to make
        // org.apache.flink.kubernetes.entrypoint.KubernetesEntrypointUtils.loadConfiguration public
        final String configDir = System.getenv(ConfigConstants.ENV_FLINK_CONF_DIR);
        Preconditions.checkNotNull(
                configDir,
                "Flink configuration directory (%s) in environment should not be null!",
                ConfigConstants.ENV_FLINK_CONF_DIR);

        final Configuration configuration =
                GlobalConfiguration.loadConfiguration(configDir, dynamicParameters);

        if (HighAvailabilityMode.isHighAvailabilityModeActivated(configuration)) {
            final String ipAddress = System.getenv().get(Constants.ENV_FLINK_POD_IP_ADDRESS);
            Preconditions.checkState(
                    ipAddress != null,
                    "JobManager ip address environment variable %s not set",
                    Constants.ENV_FLINK_POD_IP_ADDRESS);
            configuration.setString(JobManagerOptions.ADDRESS, ipAddress);
            configuration.setString(RestOptions.ADDRESS, ipAddress);
        }

        return configuration;
    }
}
