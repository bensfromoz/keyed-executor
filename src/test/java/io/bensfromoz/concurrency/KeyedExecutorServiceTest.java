package io.bensfromoz.concurrency;

import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

public class KeyedExecutorServiceTest {

    private AtomicInteger executionCounter;
    private KeyedExecutorService<String, Integer> keyedExecutorService;
    @Before
    public void init() {
        executionCounter = new AtomicInteger(0);
        keyedExecutorService = new KeyedExecutorService<>(5, new LmaxStyleSleepingWait(), new LoggingFailureHandler());
    }

    @Test
    public void shouldReturnValidFuture() throws ExecutionException, InterruptedException {
        final String key = "id1";
        Future<Integer> future = keyedExecutorService.submit(key, newIncrementingKeyedCallable(key));
        assertEquals(new Integer(1), future.get());
    }

    @Test
    public void shouldRunUniqueKeysOnceEach() throws ExecutionException, InterruptedException {
        Future<Integer> future1 = keyedExecutorService.submit("id1", newIncrementingKeyedCallable("id1"));
        Future<Integer> future2 = keyedExecutorService.submit("id2", newIncrementingKeyedCallable("id2"));
        Future<Integer> future3 = keyedExecutorService.submit("id3", newIncrementingKeyedCallable("id3"));
        future1.get();
        future2.get();
        int result = future3.get();
        assertEquals(3, result);
    }

    @Test
    public void shouldDeduplicateBasedOnKey() throws ExecutionException, InterruptedException {
        final String key = "id1";
        Future<Integer> future1 = keyedExecutorService.submit(key, newIncrementingKeyedCallable(key));
        Future<Integer> future2 = keyedExecutorService.submit(key, newIncrementingKeyedCallable(key));
        Future<Integer> future3 = keyedExecutorService.submit(key, newIncrementingKeyedCallable(key));
        future1.get();
        future2.get();
        int result = future3.get();
        assertEquals(1, result);
    }

    @Test
    public void shouldDeduplicateMultipleKeys() throws ExecutionException, InterruptedException {
        List<Future<Integer>> futures = Lists.newArrayList(
                keyedExecutorService.submit("id1", newIncrementingKeyedCallable("id1")),
                keyedExecutorService.submit("id1", newIncrementingKeyedCallable("id1")),
                keyedExecutorService.submit("id2", newIncrementingKeyedCallable("id2")),
                keyedExecutorService.submit("id1", newIncrementingKeyedCallable("id1")),
                keyedExecutorService.submit("id3", newIncrementingKeyedCallable("id3"))
        );
        for (Future<Integer> f: futures) {
            f.get();
        }
        assertEquals(3, executionCounter.get());
    }

    private KeyedCallable<String, Integer> newIncrementingKeyedCallable(final String key) {
        return new KeyedCallable<String, Integer>() {
            @Override
            public String getKey() {
                return key;
            }

            @Override
            public Integer call() throws Exception {
                return executionCounter.incrementAndGet();
            }
        };
    }
}
