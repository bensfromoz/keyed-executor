package io.bensfromoz.concurrency;

import java.util.Map;
import java.util.concurrent.locks.LockSupport;

public class LmaxStyleSleepingWait<V, R> implements BlockingWaitStrategy<V, R> {

    public void onWait(final int threads,
                       final Map<V, KeyedFutureTask<V, R>> executingTasks) {
        while (true){
            if (executingTasks.isEmpty() || executingTasks.size() < threads + 1) {
                return;
            }
            LockSupport.parkNanos(1L);
        }
    }
}
