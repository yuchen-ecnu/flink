package org.apache.flink.client.deployment;

import org.apache.flink.util.FileUtils;
import org.apache.flink.util.function.FunctionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class AddRemoveJarUtil {
    private static final Logger LOG = LoggerFactory.getLogger(AddRemoveJarUtil.class);

    public static Collection<URL> parseAddJar(List<String> addJarList, Path workingDirectory)
            throws IOException {
        Collection<URL> jarPathURLs = new ArrayList<>();
        for (String pathStr : addJarList) {
            if (pathStr.startsWith("http")) {
                // remote jar
                try {
                    jarPathURLs.add(new URL(pathStr));
                    LOG.info("add remote jar succeed: {}", pathStr);
                } catch (MalformedURLException e) {
                    LOG.error("add remote jar failed: {}", pathStr);
                    LOG.error(e.getMessage(), e);
                }
            } else if (pathStr.startsWith("/")) {
                if (pathStr.endsWith(".jar")) {
                    // a local jar
                    File file = new File(pathStr);
                    if (file.exists()) {
                        jarPathURLs.add(file.toURI().toURL());
                        LOG.error("add local jar succeed: {}", pathStr);
                    } else {
                        LOG.error("add local jar failed, file not found: {}", pathStr);
                    }
                } else {
                    // a local directory
                    File addJarDir = new File(pathStr);
                    jarPathURLs.addAll(
                            FileUtils.listFilesInDirectory(addJarDir.toPath(), FileUtils::isJarFile)
                                    .stream()
                                    .map(path -> FileUtils.relativizePath(workingDirectory, path))
                                    .map(FunctionUtils.uncheckedFunction(FileUtils::toURL))
                                    .collect(Collectors.toList()));
                    LOG.info("add local jar under directory {} succeed!", pathStr);
                }
            } else {
                throw new RuntimeException(
                        String.format("found a invalid add jar path: %s", pathStr));
            }
        }
        return jarPathURLs;
    }
}
