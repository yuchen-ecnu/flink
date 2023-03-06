package org.apache.flink.runtime.rest.messages;

import com.google.gson.Gson;

import org.apache.flink.runtime.rest.messages.json.SerializedValueDeserializer;
import org.apache.flink.runtime.rest.messages.json.SerializedValueSerializer;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;

/**
 * a common request body with only one string value.
 */
public class StringRequestBody implements RequestBody {

    public static final String VALUE = "value";

    @JsonProperty(VALUE)
    private final String value;

    @JsonCreator
    public StringRequestBody(@JsonProperty(VALUE) String value) {
        this.value = value;
    }

    @JsonIgnore
    public String getValue() {
        return value;
    }


    public static void main(String[] args) throws Exception {
        System.out.println(new ObjectMapper().writeValueAsString(new StringRequestBody("xxx")));
    }
}
