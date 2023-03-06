package org.apache.flink.runtime.util.red;

import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/** PbSchemaUtil. */
public class PbSchemaUtil {

    public static final String ALIAS = "alias";
    public static final String HASH_ID = "hash_id";
    public static final String CLASS_NAME = "class_name";
    public static final String JAR_PATH = "jar_path";
    public static final String BUSINESS = "business";
    public static final String CURRENT_VERSION = "current_version";

    /** 本地debug时将SCHEMA_SERVER_URL换成这个，办公网络可访问. */
    public static final String SCHEMA_SERVER_URL_OLD =
            "http://data-schema-v2-pub-356d9099e089a8b7.elb.cn-north-1.amazonaws.com.cn:8080/get/";
    /** 线上访问走内网lb. */
    public static final String SCHEMA_SERVER_URL_NEW = RestOptions.SCHEMA_SERVER_URL.defaultValue();

    /** 允许通过参数指定schema server的地址 {@link RestOptions#SCHEMA_SERVER_URL } */
    private static final String SCHEMA_SERVER_URL_PARAMETER =
            GlobalConfiguration.loadConfiguration().get(RestOptions.SCHEMA_SERVER_URL);

    public static final String SCHEMA_SERVER_URL = SCHEMA_SERVER_URL_PARAMETER;

    private static final Logger logger = LoggerFactory.getLogger(PbSchemaUtil.class);
    private static final int TIMEOUT_MILLIS = 10000;
    private static final int RETRIES = 10;
    private static final int RETRY_BACKOFF_MILLISECONDS = 2000;
    private static final RequestConfig requestConfig =
            RequestConfig.custom()
                    .setSocketTimeout(TIMEOUT_MILLIS)
                    .setConnectTimeout(TIMEOUT_MILLIS)
                    .setConnectionRequestTimeout(TIMEOUT_MILLIS)
                    .build();

    public static Map<String, String> querySchemaInfo(String alias, TYPE type) throws Exception {
        Map<String, String> schemaInfo = new HashMap<>();
        Map<String, String> schemaAlias = querySchemaAlias(alias, type);
        if (schemaAlias == null || schemaAlias.isEmpty() || !schemaAlias.containsKey(BUSINESS)) {
            return null;
        }
        String business = schemaAlias.get(BUSINESS);
        String className = schemaAlias.get(CLASS_NAME);
        Map<String, String> schema = querySchema(business);
        if (schema == null || schema.isEmpty() || !schema.containsKey(CURRENT_VERSION)) {
            return null;
        }
        String currentVersion = schema.get(CURRENT_VERSION);
        Map<String, String> version = queryVersion(business, currentVersion);
        if (version == null || version.isEmpty() || !version.containsKey(JAR_PATH)) {
            return null;
        }
        String jarPath = version.get(JAR_PATH);
        schemaInfo.put(CLASS_NAME, className);
        schemaInfo.put(JAR_PATH, jarPath);
        schemaInfo.put(CURRENT_VERSION, currentVersion);
        return schemaInfo;
    }

    public static Map<String, String> querySchemaAlias(String request, TYPE type) throws Exception {
        HttpPost post = getPreparedHttpPost();
        StringEntity stringEntity = null;
        switch (type) {
            case ALIAS:
            {
                stringEntity =
                        new StringEntity(
                                "{\"REDEMRBinarySchemaAlias\":{alias:\"" + request + "\"}}");
                break;
            }
            case HASHID:
            {
                stringEntity =
                        new StringEntity(
                                "{\"REDEMRBinarySchemaAlias\":{hash_id:" + request + "}}");
                break;
            }
            default:
                break;
        }
        post.setEntity(stringEntity);
        String classListStr = schemaServerRequest(post);
        logger.info("query schema alias result is {}", classListStr);
        Map<String, Object> classMap = new ObjectMapper().readValue(classListStr, Map.class);
        if (classMap.get("code").equals(200)) {
            return (Map<String, String>) classMap.get("REDEMRBinarySchemaAlias");
        } else {
            throw new FlinkRuntimeException(
                    String.format("unexpected code %s", classMap.get("code")));
        }
    }

    public static Map<String, String> querySchema(String business) throws Exception {
        HttpPost post = getPreparedHttpPost();
        post.setEntity(
                new StringEntity("{\"REDEMRBinarySchema\":{business:\"" + business + "\"}}"));
        String classListStr = schemaServerRequest(post);
        Map<String, Object> classMap = new ObjectMapper().readValue(classListStr, Map.class);
        if (classMap.get("code").equals(200)) {
            Map<String, String> item = (Map<String, String>) classMap.get("REDEMRBinarySchema");
            return item;
        } else {
            throw new FlinkRuntimeException(
                    String.format("unexpected code %s", classMap.get("code")));
        }
    }

    public static Map<String, String> querySchemaAlias(String alias, boolean byAlias)
            throws Exception {
        HttpPost post = getPreparedHttpPost();
        post.setEntity(new StringEntity("{\"REDEMRBinarySchemaAlias\":{alias:\"" + alias + "\"}}"));
        String classListStr = schemaServerRequest(post);
        Map<String, Object> classMap = new ObjectMapper().readValue(classListStr, Map.class);
        if (classMap.get("code").equals(200)) {
            Map<String, String> item =
                    (Map<String, String>) classMap.get("REDEMRBinarySchemaAlias");
            return item;
        } else {
            throw new FlinkRuntimeException(
                    String.format("unexpected code %s", classMap.get("code")));
        }
    }

    public static Map<String, String> querySchemaInfo(String alias) throws Exception {
        Map<String, String> schemaInfo = new HashMap<>();
        Map<String, String> schemaAlias = querySchemaAlias(alias, true);
        if (schemaAlias == null || schemaAlias.isEmpty() || !schemaAlias.containsKey(BUSINESS)) {
            return null;
        }
        String business = schemaAlias.get(BUSINESS);
        String className = schemaAlias.get(CLASS_NAME);
        Map<String, String> schema = querySchema(business);
        if (schema == null || schema.isEmpty() || !schema.containsKey(CURRENT_VERSION)) {
            return null;
        }
        String currentVersion = schema.get(CURRENT_VERSION);
        Map<String, String> version = queryVersion(business, currentVersion);
        if (version == null || version.isEmpty() || !version.containsKey(JAR_PATH)) {
            return null;
        }
        String jarPath = version.get(JAR_PATH);
        schemaInfo.put(CLASS_NAME, className);
        schemaInfo.put(JAR_PATH, jarPath);
        schemaInfo.put(CURRENT_VERSION, currentVersion);
        return schemaInfo;
    }

    public static Map<String, String> querySchemaAlias(int hashCode) throws Exception {
        HttpPost post = getPreparedHttpPost();
        post.setEntity(
                new StringEntity("{\"REDEMRBinarySchemaAlias\":{hash_id:" + hashCode + "}}"));
        String classListStr = schemaServerRequest(post);

        Map<String, Object> classMap = new ObjectMapper().readValue(classListStr, Map.class);
        if (classMap.get("code").equals(200)) {
            Map<String, String> item =
                    (Map<String, String>) classMap.get("REDEMRBinarySchemaAlias");
            return item;
        } else {
            throw new FlinkRuntimeException(
                    String.format("unexpected code %s", classMap.get("code")));
        }
    }

    public static Map<String, Object> querySchemaAlias(String className) throws Exception {
        HttpPost post = getPreparedHttpPost();
        post.setEntity(
                new StringEntity(
                        "{\"REDEMRBinarySchemaAlias\":{class_name:\"" + className + "\"}}"));
        String classListStr = schemaServerRequest(post);
        Map<String, Object> classMap = new ObjectMapper().readValue(classListStr, Map.class);
        if (classMap.get("code").equals(200)) {
            return (Map<String, Object>) classMap.get("REDEMRBinarySchemaAlias");
        } else {
            throw new FlinkRuntimeException(
                    String.format("unexpected code %s", classMap.get("code")));
        }
    }

    public static Map<String, String> queryVersion(String business, String currentVersion)
            throws Exception {
        HttpPost post = getPreparedHttpPost();
        post.setEntity(
                new StringEntity(
                        "{\"REDEMRBinarySchemaVersions\":{business:\""
                                + business
                                + "\",version:\""
                                + currentVersion
                                + "\"}}"));
        String classListStr = schemaServerRequest(post);
        Map<String, Object> classMap = new ObjectMapper().readValue(classListStr, Map.class);
        if (classMap.get("code").equals(200)) {
            return (Map<String, String>) classMap.get("REDEMRBinarySchemaVersions");
        } else {
            throw new FlinkRuntimeException(
                    String.format("unexpected code %s", classMap.get("code")));
        }
    }

    private static HttpPost getPreparedHttpPost() {
        HttpPost post = new HttpPost(SCHEMA_SERVER_URL);
        post.setConfig(requestConfig);
        post.setHeader("Content-Type", "application/json");
        return post;
    }

    public static String schemaServerRequest(HttpRequestBase request) throws Exception {
        return RetryUtil.retryFunc(
                RETRIES,
                RETRY_BACKOFF_MILLISECONDS,
                () -> {
                    CloseableHttpClient httpClient = HttpClients.createDefault();
                    try (CloseableHttpResponse httpResponse = httpClient.execute(request)) {
                        return EntityUtils.toString(httpResponse.getEntity());
                    } finally {
                        if (httpClient != null) {
                            httpClient.close();
                        }
                    }
                });
    }

    /** TYPE. */
    public enum TYPE {
        ALIAS,
        HASHID
    }
}
