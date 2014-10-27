package io.bensfromoz.concurrency;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
public class UniqueTaskExecutorService<K, C> {

    private static final long WAIT_TIME = 50L;
    private final int maxThreads;

    private final ExecutorService executorService;

    private final Map<K, FutureTask<C>> queuedTasks = new ConcurrentHashMap<>();
    private final Map<K, FutureTask<C>> executingTasks = new ConcurrentHashMap<>();

    private AtomicBoolean enabled = new AtomicBoolean(true);

    public UniqueTaskExecutorService(int maxThreads) {
        this.maxThreads = maxThreads;
        this.executorService = Executors.newFixedThreadPool(maxThreads);
        Executors.newScheduledThreadPool(1).submit(new Runnable() {
            @Override
            public void run() {
                executingListener();
            }
        });
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

    public Future<C> submit(K key, Callable<C> tCallable) {
        return addOrGetTask(key, tCallable);
    }

    private Future<C> addOrGetTask(final K key, Callable<C> tCallable) {
        FutureTask<C> task;
        if (queuedTasks.get(key) == null) {
            task = new FutureTask<>(new Callable<C>() {
                @Override
                public C call() throws Exception {
                    try {
                        C result = tCallable.call();
                        return result;
                    } finally {
                        executingTasks.remove(key);
                    }
                }
            });
            queuedTasks.put(key, task);
        } else {
            task = queuedTasks.get(key);
        }
        return task;
    }

    private boolean blockingHasCapacity() {
        while (enabled.get()) {
            if (executingTasks.isEmpty() || executingTasks.size() < maxThreads + 1) {
                return true;
            }
            sleep();
        }
        return false;
    }

    private void sleep() {
        try {
            Thread.sleep(WAIT_TIME);
        } catch (InterruptedException ignored) {
            //noop
        }
    }

    private void executingListener() {
        while (blockingHasCapacity()) {
            Iterator<Map.Entry<K, FutureTask<C>>> iterator = queuedTasks.entrySet().iterator();
            while (iterator.hasNext() && executingTasks.size() < maxThreads + 1) {
                Map.Entry<K, FutureTask<C>> job = iterator.next();
                if (job != null && executingTasks.get(job.getKey()) == null) {
                    executingTasks.put(job.getKey(), job.getValue());
                    executorService.submit(job.getValue());
                }
            }
        }
    }

    public Map<K, FutureTask<C>> getQueuedTasks() {
        return queuedTasks;
    }

    public Map<K, FutureTask<C>> getExecutingTasks() {
        return executingTasks;
    }

    public AtomicBoolean getEnabled() {
        return enabled;
    }
}
