package io.bensfromoz.concurrency;

public class KeyedExecutors {
    public static KeyedExecutorService newKeyedExecutorService(int threads) {
        return new KeyedExecutorService(threads, new LmaxStyleSleepingWait(), new LoggingFailureHandler());
    }

    public static KeyedExecutorService newKeyedExecutorService(int threads, BlockingWaitStrategy blockingWaitStrategy) {
        return new KeyedExecutorService(threads, blockingWaitStrategy, new LoggingFailureHandler());
    }

    public static KeyedExecutorService newKeyedExecutorService(int threads,
                                                               BlockingWaitStrategy blockingWaitStrategy,
                                                               LoggingFailureHandler loggingFailureHandler) {
        return new KeyedExecutorService(threads, blockingWaitStrategy, loggingFailureHandler);
    }
}
