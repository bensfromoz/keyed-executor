package io.bensfromoz.concurrency;

import java.util.concurrent.Callable;

public interface KeyedCallable<V, R> extends Callable<R> {

    V getKey();

}
