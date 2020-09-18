package com.hao.walnut.server;

import com.hao.walnut.WALException;
import com.hao.walnut.server.WALServer;
import com.hao.walnut.server.WALServerImpl;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@Slf4j
public class WALServerImplTest {

    @Test
    public void testReadAndWrite() throws InterruptedException {
        long start = System.currentTimeMillis();
        WALServer walServer = new WALServerImpl();

        ExecutorService executorService = Executors.newFixedThreadPool(20);

        List<Future> futureList = new LinkedList<>();

        for(int i = 0; i < 100000; i++) {
            final int idx = i;
            Future future = executorService.submit(() -> {
                try {
                    Future f = walServer.append("testFd", String.valueOf("test"+idx).getBytes(StandardCharsets.UTF_8), (offset) -> {
                        log.info("testFd {}", offset);
                    });
                    f.get();
                } catch (WALException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
            });
            futureList.add(future);
        }

        executorService.shutdown();
        while(executorService.isTerminated() == false) {
            Thread.sleep(10);
        }
        log.info("total cost {}", System.currentTimeMillis() - start);
    }
}
