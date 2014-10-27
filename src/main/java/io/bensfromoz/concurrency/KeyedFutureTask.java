package io.bensfromoz.concurrency;

import java.util.concurrent.FutureTask;

public class KeyedFutureTask<K, F> extends FutureTask<F> {

    private volatile K key;

    public KeyedFutureTask(KeyedCallable<K,F> keyedCallable) {
        super(keyedCallable);
        this.key = keyedCallable.getKey();
    }

    public KeyedFutureTask(Runnable runnable, F f) {
        super(runnable, f);
        this.key = null;
    }
}
