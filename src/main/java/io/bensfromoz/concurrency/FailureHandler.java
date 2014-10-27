package io.bensfromoz.concurrency;

public interface FailureHandler {

    void onFailure(Throwable t);

}
