package io.bensfromoz.concurrency;

import java.util.Map;

public interface BlockingWaitStrategy<V, R> {

    void onWait(final int threads,
                final Map<V, KeyedFutureTask<V, R>> executingTasks);
}
