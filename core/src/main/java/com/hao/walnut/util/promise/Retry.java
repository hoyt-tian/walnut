package com.hao.walnut.util.promise;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.Callable;

@Slf4j
public class Retry extends Promise {
    protected static int sleepInterval = 5;

    public Retry(Callable callable) {
        this(callable, 5);
    }

    public Retry(Callable callable, int maxRetry) {
        super((onResolve, onReject) -> {
            int retry = 0;
            while(retry < maxRetry) {
                try {
                    onResolve.call(callable.call());
                    return;
                } catch (Exception e) {
                    retry++;
                    try {
                        Thread.sleep(sleepInterval * retry);
                    } catch (InterruptedException interruptedException) {
                        log.error("{}", interruptedException.getMessage());
                    }
                }
            }
            onReject.call(new Exception("Max Retry Time"));
        });
    }
}
