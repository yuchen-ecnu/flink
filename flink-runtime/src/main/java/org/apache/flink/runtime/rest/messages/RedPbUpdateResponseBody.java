package org.apache.flink.runtime.rest.messages;

import org.apache.flink.runtime.rest.messages.ResponseBody;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import static java.util.Objects.requireNonNull;

public class RedPbUpdateResponseBody implements ResponseBody {

    @JsonProperty("pbAlias")
    private final String pbAlias;

    @JsonProperty("className")
    private final String className;

    @JsonProperty("currentVersion")
    private final String currentVersion;

    @JsonProperty("jarPath")
    private final String jarPath;

    @JsonCreator
    public RedPbUpdateResponseBody(
            @JsonProperty("pbAlias") final String pbAlias,
            @JsonProperty("className") final String className,
            @JsonProperty("jarPath") final String jarPath,
            @JsonProperty("currentVersion") final String currentVersion
    ) {
        this.pbAlias = requireNonNull(pbAlias);
        this.className = className;
        this.jarPath = jarPath;
        this.currentVersion = currentVersion;
    }

}
