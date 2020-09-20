package com.hao.walnut.util.promise;

import lombok.extern.slf4j.Slf4j;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class Promise<T> {
    static final byte STATUS_Pending = 0;
    static final byte STATUS_Resolve = 1;
    static final byte STATUS_Reject = 2;

    T data;

    byte status = STATUS_Pending;

    List<Callback> resolveCallbacks = new LinkedList<>();
    List<Callback> rejectCallbacks = new LinkedList<>();

    private Promise() {

    }

    public Promise(PromiseFunc c) {
        c.execute(this::_resolve, this::_reject);
    }

    public Promise(PromiseFuncWithResolveOnly c) {
        c.execute(this::_resolve);
    }

    public T getData() { return data; }

    void _resolve(Object ...args) {
        this.status = STATUS_Resolve;
        if (args.length == 1) {
            this.data = (T)args[0];
        } else {
            this.data = (T) args;
        }
        this.fire(resolveCallbacks, args);
    }

    void _reject(Object ...args) {
        this.status = STATUS_Reject;
        if (args.length == 1) {
            this.data = (T)args[0];
        } else {
            this.data = (T) args;
        }
        this.fire(rejectCallbacks, args);
    }

    void fire(List<Callback> callbacks, Object ...args) {
        for(Callback callback : callbacks) {
            callback.call(args);
        }
    }

    protected void addCallbacks(Callback callback, List<Callback> callbacks) {
        if (callback != null) {
            callbacks.add(callback);
        }
    }

    public Promise then(Callback onResolve, Callback onReject) {
        switch (status) {
            default:
            case STATUS_Pending:
                addCallbacks(onResolve, resolveCallbacks);
                addCallbacks(onReject, rejectCallbacks);
                return new Promise(($resolve, $reject) -> {
                    addCallbacks($resolve, resolveCallbacks);
                    addCallbacks($reject, rejectCallbacks);
                });
            case STATUS_Resolve:
                onResolve.call(this.data);
                return this;
            case STATUS_Reject:
                onReject.call(this.data);
                return this;
        }
    }

    public Promise then(Callback resolve) {
        return this.then(resolve, null);
    }

    public boolean isPending() {
        return status == STATUS_Pending;
    }

    public boolean isResolved() {
        return status == STATUS_Resolve;
    }

    public boolean isReject() {
        return status == STATUS_Reject;
    }

    public static <T>  Promise<T> resolve(T data) {
        Promise p = new Promise();
        p._resolve(data);
        return p;
    }

    public static <T> Promise<T> reject(T data) {
        Promise p = new Promise();
        p._reject(data);
        return p;
    }

    public static Promise all(Promise ...promises) {
        return new Promise((onResolve, onReject) -> {
            AtomicInteger resolveCounter = new AtomicInteger(0);
            for(Promise p : promises) {
                p.then((result) -> {
                    int val = resolveCounter.incrementAndGet();
                    if (val == promises.length) {
                        Object[] results = new Object[promises.length];
                        for(int i = 0; i < promises.length; i++) {
                            results[i] = promises[i].data;
                        }
                        onResolve.call(results);
                    }
                }, onReject);
            }
        });

    }
}
