package org.apache.flink.runtime.jobgraph.jsonplan;

import java.io.Serializable;

public class JsonStreamGraph implements Serializable {

    private final Integer pendingOperators;
    private final String jsonStreamPlan;

    public JsonStreamGraph(String jsonStreamPlan, Integer pendingOperators) {
        this.jsonStreamPlan = jsonStreamPlan;
        this.pendingOperators = pendingOperators;
    }

    public Integer getPendingOperators() {
        return pendingOperators;
    }

    public String getJsonStreamPlan() {
        return jsonStreamPlan;
    }
}
