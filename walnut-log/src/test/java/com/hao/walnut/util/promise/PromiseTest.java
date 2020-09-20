package com.hao.walnut.util.promise;

import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.hao.walnut.util.promise.Promise.resolve;

@Slf4j
@Ignore
public class PromiseTest {

    @Test
    public void testResolve() throws InterruptedException {
        Promise p = resolve("something");
        Assert.assertEquals(p.status, Promise.STATUS_Pending);

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        p = new Promise((onResolve) -> {
            try {
                log.info("create promise here {}", System.currentTimeMillis());
                Thread.sleep(2000);
                log.info("wake up and finish promise {}", System.currentTimeMillis());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            executorService.submit(() -> {
                try {
                    log.info("new thread sleep first and then call resolve");
                    Thread.sleep(1000);
                    onResolve.call("abc");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            });
        });

        p.then((val) -> {
            log.info("val = {}, execute then {}", val, System.currentTimeMillis());
        }).then((val) -> {
            log.info("val = {}, execute then {}", val, System.currentTimeMillis());
        });
        executorService.shutdown();
        while (!executorService.isTerminated()) {
            Thread.sleep(100);
        }
        log.info("finish test suit");
    }

    @Test
    public void testPromiseAll() throws InterruptedException {
        ExecutorService executorService = Executors.newCachedThreadPool();

        Promise a = new Promise((onResolve) -> {
            executorService.submit(() -> {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                log.info("a resolve at {}", System.currentTimeMillis());
                onResolve.call(123, "456");
            });
        });
        Promise b = new Promise((onResolve) -> {
            executorService.submit(() -> {
                try {
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                log.info("b resolve at {}", System.currentTimeMillis());
                onResolve.call(789, "JQK");
            });
        });

        Promise all = Promise.all(a, b);

        all.then((result) -> {
            log.info("result {}, {}", result, System.currentTimeMillis());
        });

        executorService.shutdown();
        while (executorService.isTerminated() == false) {
            Thread.sleep(10);
        }
    }
}
