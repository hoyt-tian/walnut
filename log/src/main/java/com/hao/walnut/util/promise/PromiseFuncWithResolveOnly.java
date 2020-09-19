package com.hao.walnut.util.promise;

@FunctionalInterface
public interface PromiseFuncWithResolveOnly {
    void execute(Callback onResolve);
}