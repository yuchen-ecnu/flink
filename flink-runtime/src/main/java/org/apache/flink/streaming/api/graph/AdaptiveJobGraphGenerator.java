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

package org.apache.flink.streaming.api.graph;

import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.jsonplan.JsonStreamGraph;

import java.util.List;
import java.util.function.Function;

public interface AdaptiveJobGraphGenerator {

    /**
     * Responds to an external notification of a finished JobVertex by creating new job vertices,
     * updating the JobGraph accordingly, and returning a list of the newly created JobVertex
     * instances.
     *
     * @param finishedJobVertexId The ID of the JobVertex that has finished.
     * @return A list of newly created {@link JobVertex} instances that were added to the JobGraph.
     */
    List<JobVertex> onJobVertexFinished(JobVertexID finishedJobVertexId);

    /**
     * Checks if the conversion from StreamGraph to JobGraph has been finished.
     *
     * @return {@code true} if the StreamGraph to JobGraph conversion is finished, {@code false}
     *     otherwise.
     */
    boolean isStreamGraphConversionFinished();

    /**
     * Retrieves the JobGraph representation of the current Flink job.
     *
     * @return The current {@link JobGraph} instance.
     */
    JobGraph getJobGraph();
    JsonStreamGraph getStreamGraph();

    /**
     * Updates the StreamGraph based on the specified update request information.
     *
     * @param updateFunc A method that implements the logic of modifying streamGraph through
     *     StreamGraphManagerContext and returns the modified result.
     * @return {@code true} if the StreamGraph update was successful, {@code false} otherwise.
     */
    boolean updateStreamGraph(Function<StreamGraphManagerContext, Boolean> updateFunc);
}
