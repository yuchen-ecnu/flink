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

package org.apache.flink.kubernetes.cli;

import org.apache.commons.cli.*;

import org.apache.flink.api.java.tuple.Tuple2;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import red.data.platform.flink.conf.FlinkJobConf;
import red.data.platform.flink.desc.FlinkJobDesc;
import red.data.platform.flink.desc.JarInfoDesc;
import red.data.platform.flink.desc.LocalJarInfoDesc;

import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;

/** CLIParser. */
public class CliParser {

    private static final Logger logger = LoggerFactory.getLogger(CliParser.class);

    private final CommandLine cli;

    public CliParser(String[] args) throws ParseException {
        Options options = new Options();
        options.addOption(
                        Option.builder()
                                .longOpt(FlinkJobConf.ConfVars.PARAMS.varname)
                                .desc(FlinkJobConf.ConfVars.PARAMS.description)
                                .required()
                                .hasArg()
                                .build())
                .addOption(
                        Option.builder()
                                .longOpt(FlinkJobConf.ConfVars.ENV.varname)
                                .desc(FlinkJobConf.ConfVars.ENV.description)
                                .optionalArg(true)
                                .hasArg()
                                .build())
                .addOption(
                        Option.builder()
                                .longOpt(FlinkJobConf.ConfVars.ENCODE_FORMAT.varname)
                                .desc(FlinkJobConf.ConfVars.ENCODE_FORMAT.description)
                                .optionalArg(true)
                                .hasArg()
                                .build())
                .addOption(
                        Option.builder()
                                .longOpt(FlinkJobConf.ConfVars.OVERWRITTEN_STREAM_GRAPH.varname)
                                .desc(FlinkJobConf.ConfVars.OVERWRITTEN_STREAM_GRAPH.description)
                                .optionalArg(true)
                                .hasArg()
                                .build());
        this.cli = new DefaultParser().parse(options, args);
    }

    public boolean hasOption(String opt) {
        return cli.hasOption(opt);
    }

    public String getString(String opt, String defaultValue) {
        return cli.getOptionValue(opt, defaultValue);
    }

    public String getString(String opt) {
        return cli.getOptionValue(opt, "");
    }

    public int getInt(String opt, int defaultValue) {
        return Integer.parseInt(cli.getOptionValue(opt, Integer.toString(defaultValue)));
    }

    public Tuple2 parseRemoteUrlAndLocalJarFromArgs() throws Exception {
        String jobDescStr = getString(FlinkJobConf.ConfVars.PARAMS.varname);
        String encodeFormat = getString(FlinkJobConf.ConfVars.ENCODE_FORMAT.varname);

        if ("base64".equalsIgnoreCase(encodeFormat)) {
            jobDescStr = new String(Base64.getDecoder().decode(jobDescStr), StandardCharsets.UTF_8);
            logger.info("job params base64 encode result : {}", jobDescStr);
        }

        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, true);
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        FlinkJobDesc jobDesc = mapper.readValue(jobDescStr, new TypeReference<FlinkJobDesc>() {});
        Set<URL> result = new HashSet<>();
        List<JarInfoDesc> jarInfoDescs = jobDesc.getJarList();
        if (jarInfoDescs != null) {
            for (JarInfoDesc jarInfoDesc : jarInfoDescs) {
                result.add(new URL(jarInfoDesc.getUrl()));
            }
        }
        LocalJarInfoDesc localJarInfoDesc = jobDesc.getLocalJarInfoDesc();
        logger.info("LocalJarInfoDesc is: {}", localJarInfoDesc.toString());
        return Tuple2.of(new ArrayList<>(result), localJarInfoDesc);
    }
}
