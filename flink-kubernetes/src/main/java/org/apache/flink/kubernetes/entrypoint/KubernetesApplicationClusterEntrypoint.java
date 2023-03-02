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

package org.apache.flink.kubernetes.entrypoint;

import org.apache.commons.cli.ParseException;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.deployment.application.ApplicationClusterEntryPoint;
import org.apache.flink.client.deployment.application.ApplicationConfiguration;
import org.apache.flink.client.program.DefaultPackagedProgramRetriever;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramRetriever;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.kubernetes.cli.CliParser;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.runtime.entrypoint.ClusterEntrypoint;
import org.apache.flink.runtime.entrypoint.ClusterEntrypointUtils;
import org.apache.flink.runtime.entrypoint.DynamicParametersConfigurationParserFactory;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.SignalHandler;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import red.data.platform.flink.desc.LocalJarInfoDesc;

import javax.annotation.Nullable;

import java.io.File;
import java.net.URL;
import java.util.Collections;
import java.util.List;

/** An {@link ApplicationClusterEntryPoint} for Kubernetes. */
@Internal
public final class KubernetesApplicationClusterEntrypoint extends ApplicationClusterEntryPoint {

    private KubernetesApplicationClusterEntrypoint(
            final Configuration configuration, final PackagedProgram program) {
        super(configuration, program, KubernetesResourceManagerFactory.getInstance());
    }

    public static void main(final String[] args) {
        // startup checks and logging
        EnvironmentInformation.logEnvironmentInfo(
                LOG, KubernetesApplicationClusterEntrypoint.class.getSimpleName(), args);
        SignalHandler.register(LOG);
        JvmShutdownSafeguard.installAsShutdownHook(LOG);

        final Configuration dynamicParameters =
                ClusterEntrypointUtils.parseParametersOrExit(
                        args,
                        new DynamicParametersConfigurationParserFactory(),
                        KubernetesApplicationClusterEntrypoint.class);
        final Configuration configuration =
                KubernetesEntrypointUtils.loadConfiguration(dynamicParameters);

        PackagedProgram program = null;
        try {
            program = getPackagedProgram(configuration);
        } catch (Exception e) {
            LOG.error("Could not create application program.", e);
            System.exit(1);
        }

        try {
            configureExecution(configuration, program);
        } catch (Exception e) {
            LOG.error("Could not apply application configuration.", e);
            System.exit(1);
        }

        final KubernetesApplicationClusterEntrypoint kubernetesApplicationClusterEntrypoint =
                new KubernetesApplicationClusterEntrypoint(configuration, program);

        ClusterEntrypoint.runClusterEntrypoint(kubernetesApplicationClusterEntrypoint);
    }

    private static PackagedProgram getPackagedProgram(final Configuration configuration)
            throws FlinkException {

        final ApplicationConfiguration applicationConfiguration =
                ApplicationConfiguration.fromConfiguration(configuration);

        final PackagedProgramRetriever programRetriever =
                getPackagedProgramRetriever(
                        configuration,
                        applicationConfiguration.getProgramArguments(),
                        applicationConfiguration.getApplicationClassName());
        return programRetriever.getPackagedProgram();
    }

    private static PackagedProgramRetriever getPackagedProgramRetriever(
            final Configuration configuration,
            final String[] programArguments,
            @Nullable final String jobClassName)
            throws FlinkException {

        final String jobType = configuration.getString(PipelineOptions.JOB_TYPE);

        final File userLibDir = ClusterEntrypointUtils.tryFindUserLibDirectory().orElse(null);
        Boolean connectorJarLoadFirst = false;
        List<String> addJarList = Collections.emptyList();
        List<String> removeJarList = Collections.emptyList();
        List<URL> externalJars = Collections.emptyList();


        if (!"JAR".equalsIgnoreCase(jobType)) {
            // sql-committer jar as default.
            LOG.info("running sql-committer sql task");
            connectorJarLoadFirst =
                    configuration.getBoolean(
                            ConfigOptions.key("connector.jar.load.first")
                                    .booleanType()
                                    .defaultValue(false));
            try {
                CliParser cliParser = new CliParser(programArguments);
                Tuple2<List<URL>, LocalJarInfoDesc> tuple2 =
                        cliParser.parseRemoteUrlAndLocalJarFromArgs();
                externalJars = tuple2.f0;
                addJarList = tuple2.f1.getAddJarList();
                removeJarList = tuple2.f1.getRemoveJarList();
            } catch (Exception e) {
                throw new FlinkException(e);
            }
        }

        // No need to do pipelineJars validation if it is a PyFlink job.
        if (!(PackagedProgramUtils.isPython(jobClassName)
                || PackagedProgramUtils.isPython(programArguments))) {
            final List<File> pipelineJars =
                    KubernetesUtils.checkJarFileForApplicationMode(configuration);
            Preconditions.checkArgument(pipelineJars.size() == 1, "Should only have one jar");
            return DefaultPackagedProgramRetriever.create(
                    userLibDir, pipelineJars.get(0), jobClassName, programArguments,externalJars,connectorJarLoadFirst,addJarList,removeJarList, configuration);
        }

        return DefaultPackagedProgramRetriever.create(
                userLibDir, jobClassName, programArguments,externalJars,connectorJarLoadFirst,addJarList,removeJarList, configuration);
    }
}
