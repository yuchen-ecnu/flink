package org.apache.flink.runtime.rest.messages;

import org.apache.flink.runtime.rest.HttpMethodWrapper;

import org.apache.flink.runtime.rest.versioning.RestAPIVersion;

import org.apache.flink.runtime.rest.versioning.RuntimeRestAPIVersion;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import java.util.Collection;
import java.util.Collections;

public class RedPbUpdateHeaders implements MessageHeaders<StringRequestBody, RedPbUpdateResponseBody, EmptyMessageParameters> {

    private static final RedPbUpdateHeaders INSTANCE = new RedPbUpdateHeaders();

    private RedPbUpdateHeaders() {
    }

    @Override
    public Class<RedPbUpdateResponseBody> getResponseClass() {
        return RedPbUpdateResponseBody.class;
    }

    @Override
    public HttpResponseStatus getResponseStatusCode() {
        return HttpResponseStatus.OK;
    }

    @Override
    public Class<StringRequestBody> getRequestClass() {
        return StringRequestBody.class;
    }

    @Override
    public EmptyMessageParameters getUnresolvedMessageParameters() {
        return EmptyMessageParameters.getInstance();
    }

    @Override
    public HttpMethodWrapper getHttpMethod() {
        return HttpMethodWrapper.POST;
    }

    @Override
    public String getTargetRestEndpointURL() {
        return "/red-pb";
    }

    @Override
    public Collection<? extends RestAPIVersion<?>> getSupportedAPIVersions() {
        return Collections.singleton(RuntimeRestAPIVersion.V1);
    }

    public static RedPbUpdateHeaders getInstance() {
        return INSTANCE;
    }

    @Override
    public String getDescription() {
        return "update pb schema by alias in scheduler, reduce qps in large task";
    }
}
