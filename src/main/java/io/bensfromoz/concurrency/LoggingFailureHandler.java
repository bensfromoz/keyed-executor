package io.bensfromoz.concurrency;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingFailureHandler implements FailureHandler {
    private static final Logger LOG = LoggerFactory.getLogger(LoggingFailureHandler.class);

    public LoggingFailureHandler() {
    }

    @Override
    public void onFailure(Throwable t) {
        LOG.error("Task execution failed due to {}", t.getMessage());
    }
}
