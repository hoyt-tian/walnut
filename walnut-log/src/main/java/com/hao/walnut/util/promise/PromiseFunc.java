package com.hao.walnut.util.promise;

@FunctionalInterface
public interface PromiseFunc {
    void execute(Callback onResolve, Callback onReject);
}