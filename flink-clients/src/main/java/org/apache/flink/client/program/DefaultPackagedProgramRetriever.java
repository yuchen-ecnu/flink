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

package org.apache.flink.client.program;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.client.deployment.AddRemoveJarUtil;
import org.apache.flink.client.deployment.application.EntryClassInformationProvider;
import org.apache.flink.client.deployment.application.FromClasspathEntryClassInformationProvider;
import org.apache.flink.client.deployment.application.FromJarEntryClassInformationProvider;
import org.apache.flink.configuration.ConfigUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.FunctionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

/**
 * {@code PackageProgramRetrieverImpl} is the default implementation of {@link
 * PackagedProgramRetriever} that can either retrieve a {@link PackagedProgram} from a specific jar,
 * some provided user classpath or the system classpath.
 */
public class DefaultPackagedProgramRetriever implements PackagedProgramRetriever {

    private static final Logger LOG =
            LoggerFactory.getLogger(DefaultPackagedProgramRetriever.class);
    private final EntryClassInformationProvider entryClassInformationProvider;
    private final String[] programArguments;
    private final List<URL> userClasspath;
    private final Configuration configuration;

    /**
     * Creates a {@code PackageProgramRetrieverImpl} with the given parameters.
     *
     * @param userLibDir The user library directory that is used for generating the user classpath
     *     if specified. The system classpath is used if not specified.
     * @param jobClassName The job class that will be used if specified. The classpath is used to
     *     detect any main class if not specified.
     * @param programArgs The program arguments.
     * @param configuration The Flink configuration for the given job.
     * @return The {@code PackageProgramRetrieverImpl} that can be used to create a {@link
     *     PackagedProgram} instance.
     * @throws FlinkException If something goes wrong during instantiation.
     */
    public static DefaultPackagedProgramRetriever create(
            @Nullable File userLibDir,
            @Nullable String jobClassName,
            String[] programArgs,
            @Nonnull List<URL> externalJars,
            @Nonnull Boolean connectorJarLoadFirst,
            @Nonnull List<String> addJarList,
            @Nonnull List<String> removeJarList,
            Configuration configuration)
            throws FlinkException {
        return create(userLibDir, null, jobClassName, programArgs,externalJars,connectorJarLoadFirst,addJarList,removeJarList, configuration);
    }

    /**
     * Creates a {@code PackageProgramRetrieverImpl} with the given parameters.
     *
     * @param userLibDir The user library directory that is used for generating the user classpath
     *     if specified. The system classpath is used if not specified.
     * @param jarFile The jar archive expected to contain the job class included; {@code null} if
     *     the job class is on the system classpath.
     * @param jobClassName The job class to use; if {@code null} the user classpath (or, if not set,
     *     the system classpath) will be scanned for possible main class.
     * @param programArgs The program arguments.
     * @param configuration The Flink configuration for the given job.
     * @return The {@code PackageProgramRetrieverImpl} that can be used to create a {@link
     *     PackagedProgram} instance.
     * @throws FlinkException If something goes wrong during instantiation.
     */
    public static DefaultPackagedProgramRetriever create(
            @Nullable File userLibDir,
            @Nullable File jarFile,
            @Nullable String jobClassName,
            String[] programArgs,
            @Nonnull List<URL> externalJars,
            @Nonnull Boolean connectorJarLoadFirst,
            @Nonnull List<String> addJarList,
            @Nonnull List<String> removeJarList,
            Configuration configuration)
            throws FlinkException {
        List<URL> userClasspaths;
        try {
            List<URL> sortedUserClassPaths = new ArrayList<>();
            if (connectorJarLoadFirst) {
                LOG.info("add connector jars to userClassPaths first");
                sortedUserClassPaths.addAll(getClasspathsFromUserLibDir(userLibDir,addJarList,removeJarList));
                sortedUserClassPaths.addAll(externalJars);
            } else {
                LOG.info("add user-defined udf jars to userClassPaths first");
                sortedUserClassPaths.addAll(externalJars);
                sortedUserClassPaths.addAll(getClasspathsFromUserLibDir(userLibDir,addJarList,removeJarList));
            }

            final List<URL> classpathsFromConfiguration =
                    getClasspathsFromConfiguration(configuration);

            final List<URL> classpaths = new ArrayList<>();
            classpaths.addAll(sortedUserClassPaths);
            classpaths.addAll(classpathsFromConfiguration);

            userClasspaths = Collections.unmodifiableList(classpaths);
        } catch (IOException e) {
            throw new FlinkException("An error occurred while extracting the user classpath.", e);
        }

        final EntryClassInformationProvider entryClassInformationProvider =
                createEntryClassInformationProvider(
                        userLibDir == null ? null : userClasspaths,
                        jarFile,
                        jobClassName,
                        programArgs);
        return new DefaultPackagedProgramRetriever(
                entryClassInformationProvider, programArgs, userClasspaths, configuration);
    }

    @VisibleForTesting
    static EntryClassInformationProvider createEntryClassInformationProvider(
            @Nullable Iterable<URL> userClasspath,
            @Nullable File jarFile,
            @Nullable String jobClassName,
            String[] programArgs)
            throws FlinkException {
        if (PackagedProgramUtils.isPython(jobClassName)
                || PackagedProgramUtils.isPython(programArgs)) {
            return FromJarEntryClassInformationProvider.createFromPythonJar();
        }

        if (jarFile != null) {
            return FromJarEntryClassInformationProvider.createFromCustomJar(jarFile, jobClassName);
        }

        if (userClasspath != null) {
            return fromUserClasspath(jobClassName, userClasspath);
        }

        return fromSystemClasspath(jobClassName);
    }

    private static EntryClassInformationProvider fromSystemClasspath(@Nullable String jobClassName)
            throws FlinkException {
        if (jobClassName != null) {
            return FromClasspathEntryClassInformationProvider
                    .createWithJobClassAssumingOnSystemClasspath(jobClassName);
        }

        try {
            return FromClasspathEntryClassInformationProvider.createFromSystemClasspath();
        } catch (IOException | NoSuchElementException | IllegalArgumentException t) {
            throw createGenericFlinkException(t);
        }
    }

    private static EntryClassInformationProvider fromUserClasspath(
            @Nullable String jobClassName, Iterable<URL> userClasspath) throws FlinkException {
        try {
            if (jobClassName != null) {
                return FromClasspathEntryClassInformationProvider.create(
                        jobClassName, userClasspath);
            }

            return FromClasspathEntryClassInformationProvider.createFromClasspath(userClasspath);
        } catch (IOException e) {
            throw createGenericFlinkException(e);
        }
    }

    private static FlinkException createGenericFlinkException(Throwable t) {
        return new FlinkException("An error occurred while access the provided classpath.", t);
    }

    private DefaultPackagedProgramRetriever(
            EntryClassInformationProvider entryClassInformationProvider,
            String[] programArguments,
            List<URL> userClasspath,
            Configuration configuration) {
        this.entryClassInformationProvider =
                Preconditions.checkNotNull(
                        entryClassInformationProvider, "No EntryClassInformationProvider passed.");
        this.programArguments =
                Preconditions.checkNotNull(programArguments, "No program parameter array passed.");
        this.userClasspath = Preconditions.checkNotNull(userClasspath, "No user classpath passed.");
        this.configuration =
                Preconditions.checkNotNull(configuration, "No Flink configuration was passed.");
    }

    @Override
    public PackagedProgram getPackagedProgram() throws FlinkException {
        try {
            final PackagedProgram.Builder packagedProgramBuilder =
                    PackagedProgram.newBuilder()
                            .setUserClassPaths(userClasspath)
                            .setArguments(programArguments)
                            .setConfiguration(configuration);

            entryClassInformationProvider
                    .getJobClassName()
                    .ifPresent(packagedProgramBuilder::setEntryPointClassName);
            entryClassInformationProvider
                    .getJarFile()
                    .ifPresent(packagedProgramBuilder::setJarFile);

            return packagedProgramBuilder.build();
        } catch (ProgramInvocationException e) {
            throw new FlinkException("Could not load the provided entrypoint class.", e);
        }
    }

    private static List<URL> getClasspathsFromUserLibDir(@Nullable File userLibDir, List<String> addJarList, List<String> removeJarList)
            throws IOException {
        if (userLibDir == null) {
            return Collections.emptyList();
        }

        final Path workingDirectory = FileUtils.getCurrentWorkingDirectory();
        final List<URL> relativeJarURLs =
                FileUtils.listFilesInDirectory(userLibDir.toPath(), FileUtils::isJarFile).stream()
                        .filter(path -> !removeJarList.contains(path.toFile().getAbsolutePath()))
                        .map(path -> FileUtils.relativizePath(workingDirectory, path))
                        .map(FunctionUtils.uncheckedFunction(FileUtils::toURL))
                        .collect(Collectors.toList());
        relativeJarURLs.addAll(AddRemoveJarUtil.parseAddJar(addJarList, workingDirectory));
        return Collections.unmodifiableList(relativeJarURLs);
    }

    private static List<URL> getClasspathsFromConfiguration(Configuration configuration)
            throws MalformedURLException {
        if (configuration == null) {
            return Collections.emptyList();
        }
        return ConfigUtils.decodeListFromConfig(
                configuration, PipelineOptions.CLASSPATHS, URL::new);
    }
}
