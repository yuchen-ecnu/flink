package org.apache.flink.runtime.util.red;

import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.util.concurrent.Callable;

public class RetryUtil {

    private static final Logger LOG = LoggerFactory.getLogger(RetryUtil.class);

    private RetryUtil() {
    }

    public static <T> T retryFunc(
            int tryLimit,
            int retryInterval,
            @Nonnull Callable<T> lambda) {
        Preconditions.checkArgument(tryLimit > 0, "tryLimit must be greater than zero");
        Preconditions.checkArgument(retryInterval > 0, "retryInterval must be greater than zero");
        int attempt = 0;
        Exception lastException = null;

        while (attempt < tryLimit) {
            try {
                return lambda.call();
            } catch (Exception e) {
                LOG.error("invoke failed, retry " + attempt + "/" + tryLimit + " times.", e);
                lastException = e;
                ++attempt;
                try {
                    Thread.sleep(retryInterval);
                } catch (InterruptedException interruptedException) {
                    throw new RuntimeException(
                            "interrupted in retry interval",
                            interruptedException);
                }
            }
        }

        throw new RuntimeException("Retry limit hit with exception", lastException);
    }

}
