package org.apache.flink.runtime.rest.handler;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.RedPbUpdateResponseBody;
import org.apache.flink.runtime.rest.messages.StringRequestBody;
import org.apache.flink.runtime.util.red.PbSchemaUtil;
import org.apache.flink.runtime.util.red.RetryUtil;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import javax.annotation.Nonnull;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class RedPbUpdateHandler extends AbstractRestHandler<
        RestfulGateway, StringRequestBody, RedPbUpdateResponseBody, EmptyMessageParameters> {


    private final ScheduledExecutorService executor;

    private volatile ConcurrentHashMap<String, RedPbUpdateResponseBody> latestPbSchema;

    private final transient Lock lock;

    private final int updateIntervalSeconds;

    public RedPbUpdateHandler(
            final GatewayRetriever<? extends RestfulGateway> leaderRetriever,
            final Time timeout,
            final Map<String, String> responseHeaders,
            final MessageHeaders<StringRequestBody, RedPbUpdateResponseBody, EmptyMessageParameters>
                    messageHeaders,
            final Configuration configuration) {
        super(leaderRetriever, timeout, responseHeaders, messageHeaders);
        this.executor = Executors.newScheduledThreadPool(
                1,
                new ExecutorThreadFactory("tracker-format-update"));

        this.latestPbSchema = new ConcurrentHashMap<>();
        this.updateIntervalSeconds = configuration.getInteger(JobManagerOptions.RED_PB_UPDATE_INTERVAL_SECONDS);
        this.lock = new ReentrantLock();
    }


    @Override
    protected CompletableFuture<RedPbUpdateResponseBody> handleRequest(
            @Nonnull HandlerRequest<StringRequestBody> request,
            @Nonnull RestfulGateway gateway) throws RestHandlerException {
        String alias = request.getRequestBody().getValue();
        return CompletableFuture.supplyAsync(
                () -> {
                    if (latestPbSchema.containsKey(alias)) {
                        return latestPbSchema.get(alias);
                    }
                    lock.lock();
                    try {
                        if (latestPbSchema.containsKey(alias)) {
                            return latestPbSchema.get(alias);
                        }
                        RedPbUpdateResponseBody schema = getSchemaByAlias(alias);
                        this.executor.scheduleWithFixedDelay(
                                () -> {
                                    getSchemaByAlias(alias);
                                },
                                this.updateIntervalSeconds,
                                this.updateIntervalSeconds,
                                TimeUnit.SECONDS
                        );
                        log.info("add scheduler update pb {} task with {} seconds interval",
                                alias, updateIntervalSeconds);
                        return schema;
                    } catch (Throwable e) {
                        log.error("error when try to get schema:" + alias, e);
                    } finally {
                        lock.unlock();
                    }
                    return null;
                },
                executor);
    }

    private RedPbUpdateResponseBody getSchemaByAlias(String alias) {
        long startTs = System.currentTimeMillis();
        Map<String, String> schema = RetryUtil.retryFunc(
                3,
                2000,
                () -> PbSchemaUtil.querySchemaInfo(alias, PbSchemaUtil.TYPE.ALIAS));
        RedPbUpdateResponseBody result = new RedPbUpdateResponseBody(
                alias,
                schema.get(PbSchemaUtil.CLASS_NAME),
                schema.get(PbSchemaUtil.JAR_PATH),
                schema.get(PbSchemaUtil.CURRENT_VERSION)
        );
        this.latestPbSchema.put(alias, result);
        log.info(
                "get pb {} from schema server with version {} cost {} ms",
                alias,
                schema.get(PbSchemaUtil.CURRENT_VERSION),
                System.currentTimeMillis() - startTs);
        return result;
    }
}
