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

package org.apache.flink.runtime.jobgraph.jsonplan;

import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.operators.testutils.DummyInvokable;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.graph.util.ImmutableStreamGraph;
import org.apache.flink.util.jackson.JacksonMapperFactory;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.TextNode;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

public class JsonGeneratorTest {

    @Test
    void testGeneratorWithoutAnyAttachements() {
        try {
            JobVertex source1 = new JobVertex("source 1");

            JobVertex source2 = new JobVertex("source 2");
            source2.setInvokableClass(DummyInvokable.class);

            JobVertex source3 = new JobVertex("source 3");

            JobVertex intermediate1 = new JobVertex("intermediate 1");
            JobVertex intermediate2 = new JobVertex("intermediate 2");

            JobVertex join1 = new JobVertex("join 1");
            JobVertex join2 = new JobVertex("join 2");

            JobVertex sink1 = new JobVertex("sink 1");
            JobVertex sink2 = new JobVertex("sink 2");

            intermediate1.connectNewDataSetAsInput(
                    source1, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
            intermediate2.connectNewDataSetAsInput(
                    source2, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

            join1.connectNewDataSetAsInput(
                    intermediate1, DistributionPattern.POINTWISE, ResultPartitionType.BLOCKING);
            join1.connectNewDataSetAsInput(
                    intermediate2, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);

            join2.connectNewDataSetAsInput(
                    join1, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
            join2.connectNewDataSetAsInput(
                    source3, DistributionPattern.POINTWISE, ResultPartitionType.BLOCKING);

            sink1.connectNewDataSetAsInput(
                    join2, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
            sink2.connectNewDataSetAsInput(
                    join1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

            JobGraph jg =
                    JobGraphTestUtils.batchJobGraph(
                            source1,
                            source2,
                            source3,
                            intermediate1,
                            intermediate2,
                            join1,
                            join2,
                            sink1,
                            sink2);

            String plan = JsonPlanGenerator.generatePlan(jg);
            assertThat(plan).isNotNull();

            // validate the produced JSON
            ObjectMapper m = JacksonMapperFactory.createObjectMapper();
            JsonNode rootNode = m.readTree(plan);

            // core fields
            assertThat(rootNode.get("jid")).isEqualTo(new TextNode(jg.getJobID().toString()));
            assertThat(rootNode.get("name")).isEqualTo(new TextNode(jg.getName()));
            assertThat(rootNode.get("type")).isEqualTo(new TextNode(jg.getJobType().name()));

            assertThat(rootNode.path("nodes").isArray()).isTrue();

            for (Iterator<JsonNode> iter = rootNode.path("nodes").elements(); iter.hasNext(); ) {
                JsonNode next = iter.next();

                JsonNode idNode = next.get("id");
                assertThat(idNode).isNotNull();
                assertThat(idNode.isTextual()).isTrue();
                checkVertexExists(idNode.asText(), jg);

                String description = next.get("description").asText();
                assertThat(
                                description.startsWith("source")
                                        || description.startsWith("sink")
                                        || description.startsWith("intermediate")
                                        || description.startsWith("join"))
                        .isTrue();
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    private void checkVertexExists(String vertexId, JobGraph graph) {
        // validate that the vertex has a valid
        JobVertexID id = JobVertexID.fromHexString(vertexId);
        for (JobVertex vertex : graph.getVertices()) {
            if (vertex.getID().equals(id)) {
                return;
            }
        }
        fail("could not find vertex with id " + vertexId + " in JobGraph");
    }

    @Test
    void testGenerateJsonStreamGraph() throws JsonProcessingException {
        StreamExecutionEnvironment env = new StreamExecutionEnvironment();
        env.fromSequence(0L, 1L).disableChaining().print();
        StreamGraph streamGraph = env.getStreamGraph();
        Map<Integer, JobVertexID> jobVertexIdMap = new HashMap<>();
        JsonStreamGraph jsonStreamGraph =
                JsonPlanGenerator.generateJsonStreamGraph(
                        new ImmutableStreamGraph(streamGraph), jobVertexIdMap);
        assertThat(jsonStreamGraph.getPendingOperators()).isEqualTo(2);

        ObjectMapper mapper = JacksonMapperFactory.createObjectMapper();

        JsonStreamGraphSchema parsedStreamGraph =
                mapper.readValue(jsonStreamGraph.getJsonStreamPlan(), JsonStreamGraphSchema.class);
        validateStreamGraph(streamGraph, parsedStreamGraph, new String[] {null, null});

        jobVertexIdMap.put(1, new JobVertexID());
        jobVertexIdMap.put(2, new JobVertexID());
        jsonStreamGraph =
                JsonPlanGenerator.generateJsonStreamGraph(
                        new ImmutableStreamGraph(streamGraph), jobVertexIdMap);
        assertThat(jsonStreamGraph.getPendingOperators()).isEqualTo(0);

        parsedStreamGraph =
                mapper.readValue(jsonStreamGraph.getJsonStreamPlan(), JsonStreamGraphSchema.class);
        validateStreamGraph(
                streamGraph,
                parsedStreamGraph,
                jobVertexIdMap.values().stream().map(JobVertexID::toString).toArray(String[]::new));
    }

    public static void validateStreamGraph(
            StreamGraph streamGraph,
            JsonStreamGraphSchema parsedStreamGraph,
            String[] expectedJobVertexIds) {
        List<String> realJobVertexIds = new ArrayList<>();
        parsedStreamGraph
                .getNodes()
                .forEach(
                        node -> {
                            StreamNode streamNode =
                                    streamGraph.getStreamNode(Integer.parseInt(node.getId()));
                            assertThat(node.getOperator()).isEqualTo(streamNode.getOperatorName());
                            assertThat(node.getParallelism())
                                    .isEqualTo(streamNode.getParallelism());
                            assertThat(node.getInputs().size())
                                    .isEqualTo(streamNode.getInEdges().size());
                            realJobVertexIds.add(node.getJobVertexId());
                        });
        assertThat(realJobVertexIds).containsExactly(expectedJobVertexIds);
    }
}
