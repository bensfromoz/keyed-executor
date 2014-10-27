package io.bensfromoz.concurrency;

import com.google.common.util.concurrent.*;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * <p>This executor is suitable for long running tasks where the overhead of ensuring uniqueness is small
 * compared to the task duration</p>
 * <p>This executor is not suitable for high throughput, short duration tasks.</p>
 * <p>The UniqueTaskExecutorService guarantees that the there will never be more than a single unique task queued for execution
 * (so long as K - the key  is unique). It also makes it very unlikely (though not guaranteed) that tasks with the
 * same key (K) will not execute concurrently.</p>
 */
public class KeyedExecutorService<V, R> {

    private static final long WAIT_TIME = 50L;
    private final int maxThreads;

    private final ListeningExecutorService executorService;
    private final FailureHandler failureHandler;
    private final BlockingWaitStrategy<V, R> blockingWaitStrategy;

    private final Map<V, KeyedFutureTask<V, R>> queuedTasks = new ConcurrentHashMap<>();
    private final Map<V, KeyedFutureTask<V, R>> executingTasks = new ConcurrentHashMap<>();

    private AtomicBoolean enabled = new AtomicBoolean(true);

    public KeyedExecutorService(int maxThreads,
                                BlockingWaitStrategy blockingWaitStrategy,
                                FailureHandler failureHandler) {
        this.maxThreads = maxThreads;
        this.blockingWaitStrategy = blockingWaitStrategy;
        this.failureHandler = failureHandler;
        this.executorService = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(maxThreads));
        Executors.newScheduledThreadPool(1)
                .scheduleWithFixedDelay(this::executingListener, 5, 80, TimeUnit.MILLISECONDS);
    }

    public void shutdown() {
        enabled.set(false);
        executorService.shutdown();
    }

    public List<Runnable> shutdownNow() {
        enabled.set(false);
        return executorService.shutdownNow();
    }

    public boolean isShutdown() {
        return (enabled.get() || executorService.isShutdown());
    }

    public boolean isTerminated() {
        return executorService.isTerminated();
    }

    public boolean awaitTermination(long l, TimeUnit timeUnit) throws InterruptedException {
        return executorService.awaitTermination(l, timeUnit);
    }

    public Future<R> submit(V key, KeyedCallable<V, R> keyedCallable) {
        return addOrGetTask(key, keyedCallable);
    }

    public KeyedFutureTask<V, R> enqueue(V key, KeyedCallable<V, R> keyedCallable) {
        return addOrGetTask(key, keyedCallable);
    }

    public Optional<KeyedFutureTask<V, R>> dequeue(V key) {
        return Optional.ofNullable(queuedTasks.getOrDefault(key, null));
    }

    private KeyedFutureTask<V, R> addOrGetTask(final V key, KeyedCallable<V, R> tCallable) {
        KeyedFutureTask<V, R> task;
        if (queuedTasks.get(key) == null) {
            task = new KeyedFutureTask<>(new KeyedCallable<V, R>() {
                @Override
                public R call() throws Exception {
                    try {
                        return tCallable.call();
                    } finally {
                        executingTasks.remove(key);
                    }
                }

                @Override
                public V getKey() {
                    return key;
                }
            });
            queuedTasks.put(key, task);
        } else {
            task = queuedTasks.get(key);
        }
        return task;
    }

    private boolean blockingHasCapacity() {
        if (enabled.get()) {
            blockingWaitStrategy.onWait(this.maxThreads, executingTasks);
            return true;
        }
        return false;
    }

    private void executingListener() {
        while (blockingHasCapacity()) {
            Iterator<Map.Entry<V, KeyedFutureTask<V, R>>> iterator = queuedTasks.entrySet().iterator();
            while (iterator.hasNext() && executingTasks.size() < maxThreads + 1) {
                Map.Entry<V, KeyedFutureTask<V, R>> job = iterator.next();
                if (job != null && executingTasks.get(job.getKey()) == null) {
                    executeJob(job);
                }
            }
        }
    }

    private void executeJob(Map.Entry<V, KeyedFutureTask<V, R>> job) {
        final V key = job.getKey();
        executingTasks.put(key, job.getValue());
        ListenableFuture listenableFuture = executorService.submit(job.getValue());
        Futures.addCallback(listenableFuture, new FutureCallback() {
            @Override
            public void onSuccess(Object o) {
                executingTasks.remove(key);
            }

            @Override
            public void onFailure(Throwable throwable) {
                try {
                    failureHandler.onFailure(throwable);
                } finally {
                    executingTasks.remove(key);
                }
            }
        });
    }


    public long getQueuedTasksSize() {
        return queuedTasks.size();
    }

    public boolean isQueuedTasksEmpty() {
        return queuedTasks.isEmpty();
    }

    public long getExecutingTasksSize() {
        return executingTasks.size();
    }

    public boolean isExecutingTasksEmtpy() {
        return executingTasks.isEmpty();
    }

    public AtomicBoolean getEnabled() {
        return enabled;
    }

    public void disable() {
        enabled.set(false);
    }

}
